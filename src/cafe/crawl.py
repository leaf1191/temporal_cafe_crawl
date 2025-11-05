from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from copy import deepcopy
import json
import time
import random
import requests
import os
import traceback
import boto3


# JSONL 파일의 마지막 줄을 읽음
def get_last_cursor_from_jsonl(filename):
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        return None
    
    try:
        with open(filename, 'rb') as f: # 바이너리(rb) 모드로 읽기
            f.seek(0, os.SEEK_END)
            buffer_size = 1024 # 적절한 버퍼 크기
            
            buffer = bytearray()
            
            current_pos = f.tell()
            while current_pos > 0:
                read_size = min(buffer_size, current_pos)
                new_pos = current_pos - read_size
                current_pos = new_pos
                f.seek(new_pos)
                
                # 데이터를 읽어서 버퍼 앞에 추가 (거꾸로 쌓음)
                buffer = f.read(read_size) + buffer
                
                try:
                    # 처음에 있는 빈 줄 제거
                    temp_buffer = buffer.rstrip()
                    if not temp_buffer:
                        continue
                    
                    # 버퍼에서 마지막 줄바꿈 문자(\n)를 찾음
                    last_newline = temp_buffer.rindex(b'\n')
                    last_line_bytes = temp_buffer[last_newline+1:]
                    
                    # 빈 줄이 아닐 경우
                    if last_line_bytes:
                        last_line_str = last_line_bytes.decode('utf-8')
                        data = json.loads(last_line_str)
                        return data.get('cursor')
                except ValueError:
                    # 버퍼 안에 줄바꿈이 아직 없음
                    pass
            
            # 파일 전체를 다 읽었는데 줄바꿈이 없는 경우
            if buffer.rstrip():
                try:
                    last_line_str = buffer.rstrip().decode('utf-8')
                    data = json.loads(last_line_str)
                    return data.get('cursor')
                except Exception as e:
                    print(f"마지막 줄 파싱 오류: {e}")
                    traceback.print_exc()

    except Exception as e:
        print(f"마지막 커서 읽기 오류: {e}")
        traceback.print_exc()
        return None
    
    return None

def scrape_reviews_by_api(business_id, max_reviews=10000, cursor=None):
    # 초기 설정
    # 재시도 설정
    MAX_NETWORK_RETRIES = 5

    # API 설정
    API_URL = "https://pcmap-api.place.naver.com/graphql"
    HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://pcmap.place.naver.com",
        "Referer": f"https://pcmap.place.naver.com/restaurant/{business_id}/review/visitor",
    }
    payload_template = [
        {
            "operationName": "getVisitorReviews",
            "variables": {
                "input": {
                    "businessId": f"{business_id}",
                    "after": None,

                    "businessType": "restaurant",
                    "item": "0",
                    "bookingBusinessId": None,
                    "size": 50,
                    "isPhotoUsed": False,
                    "includeContent": True,
                    "getUserStats": True,
                    "includeReceiptPhotos": True,
                    "cidList": None,
                    "getReactions": True,
                    "getTrailer": True,
                }
            },
            "query": """query getVisitorReviews($input: VisitorReviewsInput) {
                        visitorReviews(input: $input) {
                            items {
                            id
                            cursor
                            reviewId
                            rating
                            author {
                                id
                                nickname
                                from
                                imageUrl
                                borderImageUrl
                                objectId
                                url
                                review {
                                totalCount
                                imageCount
                                avgRating
                                __typename
                                }
                                theme {
                                totalCount
                                __typename
                                }
                                isFollowing
                                followerCount
                                followRequested
                                __typename
                            }
                            body
                            thumbnail
                            media {
                                type
                                thumbnail
                                thumbnailRatio
                                class
                                videoId
                                videoUrl
                                trailerUrl
                                __typename
                            }
                            tags
                            status
                            visitCount
                            viewCount
                            visited
                            created
                            reply {
                                editUrl
                                body
                                editedBy
                                created
                                date
                                replyTitle
                                isReported
                                isSuspended
                                status
                                __typename
                            }
                            originType
                            item {
                                name
                                code
                                options
                                __typename
                            }
                            language
                            highlightRanges {
                                start
                                end
                                __typename
                            }
                            apolloCacheId
                            translatedText
                            businessName
                            showBookingItemName
                            bookingItemName
                            votedKeywords {
                                code
                                iconUrl
                                iconCode
                                name
                                __typename
                            }
                            userIdno
                            loginIdno
                            receiptInfoUrl
                            reactionStat {
                                id
                                typeCount {
                                name
                                count
                                __typename
                                }
                                totalCount
                                __typename
                            }
                            hasViewerReacted {
                                id
                                reacted
                                __typename
                            }
                            nickname
                            showPaymentInfo
                            visitCategories {
                                code
                                name
                                keywords {
                                code
                                name
                                __typename
                                }
                                __typename
                            }
                            representativeVisitDateTime
                            showRepresentativeVisitDateTime
                            __typename
                            }
                            starDistribution {
                            score
                            count
                            __typename
                            }
                            hideProductSelectBox
                            total
                            showRecommendationSort
                            itemReviewStats {
                            score
                            count
                            itemId
                            starDistribution {
                                score
                                count
                                __typename
                            }
                            __typename
                            }
                            __typename
                        }
                        }"""
        }
    ]

    # payload를 독립적으로 운용하기 위해 deepcopy
    payload_to_send = deepcopy(payload_template)

    all_reviews = []
    is_completed = False
    current_cursor = cursor

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
        )
        try:
            page.goto(f"https://pcmap.place.naver.com/restaurant/{business_id}/review/visitor", wait_until="networkidle")
        except Exception as e:
            print(f"[{business_id}] 쿠키 획득용 페이지 접속 실패: {e}")
            browser.close()
            return [], False

        api_request_context = page.request

        # 페이징 루프(한번에 50개 씩) 
        # 위험 요소로 50개 단위 크롤링이므로 최대 리뷰가 50의 배수가 아니라면 초과 가능성
        # 어짜피 다 크롤링 할 거라 일단 진행
        while len(all_reviews) < max_reviews:
            payload_to_send[0]["variables"]["input"]["after"] = current_cursor
            response = None
            attempt_count = 0
            is_429 = False

            # API 요청 재시도 전략
            while attempt_count < MAX_NETWORK_RETRIES:
                try:
                    response = api_request_context.post(API_URL, data=payload_to_send, headers=HEADERS, timeout=10000)

                    if response.status == 200:
                        break
                    elif response.status == 429:
                        default_wait = 540
                        retry_after = response.headers.get("Retry-After")
                        retry_after_seconds = default_wait # 기본값으로 설정
                        
                        if retry_after:
                            try:
                                # 숫자인지 먼저 시도
                                retry_after_seconds = int(retry_after)
                            except ValueError:
                                # 숫자가 아니면 그냥 9분 대기
                                retry_after_seconds = default_wait
                        wait_time = min(retry_after_seconds, default_wait) # 9분과 헤더 값 중 작은 시간

                        print(f"429 발생 {wait_time}초 대기...") # 20분 컨트롤 하기 위함
                        time.sleep(wait_time)
                        if(is_429):
                            print("429 2회째 발생, 작업 일시 중지...")
                            browser.close()
                            return all_reviews, is_completed
                        is_429 = True
                        continue
                    elif response.status >= 500:
                        attempt_count += 1
                        print(f"서버 오류 ({response.status})")
                        time.sleep(5 * attempt_count)
                        continue
                    else:
                        raise Exception(f"클라이언트 또는 예상치 못한 오류 ({response.status}): {response.text()}")

                except PlaywrightTimeoutError as e:
                    attempt_count += 1
                    print(f"네트워크 에러 발생 ({e})")
                    time.sleep(3 * attempt_count)
                    continue
                except Exception as e:
                    print(f"치명적 에러 발생: {e}. {business_id} 수집 일시 종료.")
                    traceback.print_exc()
                    browser.close()
                    return all_reviews, is_completed

            # break 안된 경우
            if attempt_count >= MAX_NETWORK_RETRIES:
                print(f"최대 재시도 횟수 ({MAX_NETWORK_RETRIES}) 초과. {business_id} 수집 일시 종료.")
                browser.close()
                return all_reviews, is_completed

            # 무조건 성공 전제
            try:
                data = response.json()
                reviews_data = data[0].get("data", {}).get("visitorReviews", {})
                items = reviews_data.get("items", [])
                
                if not items:
                    print("더 이상 가져올 리뷰가 없습니다.")
                    is_completed = True
                    break

                for item in items:
                    author_info = item.get("author", {})
                    extracted_data = {
                        "author_id": author_info.get("id"),
                        "body": item.get("body"),
                        "visit_count": item.get("visitCount"),
                        "visit_time": item.get("representativeVisitDateTime"),
                        "cursor": item.get("cursor"),
                    }
                    all_reviews.append(extracted_data)
                    
                current_cursor = items[-1]['cursor']
                
                print(f"리뷰 {len(items)}개 수집 완료. (총 {len(all_reviews)}개)")
                time.sleep(random.uniform(2, 4)) # 2초~4초 사이 랜덤 대기

                if random.random() < 0.8: # 80% 확률로 추가 대기
                    time.sleep(random.uniform(0.5, 3))

                if random.random() < 0.1: # 10% 확률로 추가 대기
                    time.sleep(random.uniform(4, 6))

            except Exception as e:
                print(f"요청 중 심각한 오류 발생: {e}")
                traceback.print_exc()
                break
        browser.close()     
    return all_reviews, is_completed

# 반환 값을 string
def process_and_save_reviews(target_id, max_reviews):
    LOCK_TIMEOUT_SECONDS = 1200 # 락 제한 시간

    EFS_BASE_PATH = "/mnt/efs_data" # EFS 마운트 경로
    REVIEW_DIR = f"{EFS_BASE_PATH}/data/cafe_reviews"
    MARKER_DIR = f"{EFS_BASE_PATH}/data/cafe_reviews_completed" # 마커 파일 저장 위치
    LOCK_DIR = f"{EFS_BASE_PATH}/data/cafe_reviews_locks" # 락 파일 저장 위치
    
    output_file = f"{REVIEW_DIR}/{target_id}_reviews.jsonl"
    marker_file = f"{MARKER_DIR}/{target_id}.COMPLETED"
    lock_file = f"{LOCK_DIR}/{target_id}.LOCKED"

    if os.path.exists(marker_file):
        print(f"[{target_id}] 스킵: 이미 '.COMPLETED' 마커 파일이 존재합니다.")
        return "SKIPPED_COMPLETED"
    
    # 락 확인 로직
    try:
        if os.path.exists(lock_file):
            # 락 파일이 존재하면, 얼마나 오래됐는지 확인
            file_mod_time = os.path.getmtime(lock_file)
            age_seconds = time.time() - file_mod_time
            
            if age_seconds < LOCK_TIMEOUT_SECONDS:
                # 락이 아직 '신선함' -> 다른 워커가 작업 중
                print(f"[{target_id}] 스킵: 다른 워커가 작업 중 (.LOCKED 파일 존재, {int(age_seconds)}초 경과).")
                return "SKIPPED_LOCKED"
            else:
                # 락이 '오래됨' -> 이전 워커가 죽었다고 간주
                print(f"[{target_id}] 경고: 락 타임아웃({LOCK_TIMEOUT_SECONDS}초) 초과. 락을 제거하고 작업을 시작합니다.")
                os.remove(lock_file) 
        
        # os.O_CREAT | os.O_EXCL로 원자적(Atomic) 락파일 생성 시도
        directory = os.path.dirname(lock_file)
        if directory and not os.path.exists(directory):
             os.makedirs(directory, exist_ok=True)
        
        fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"Locked by PID {os.getpid()} at {time.time()}".encode())
        os.close(fd)
        
    except FileExistsError:
        # 락을 생성하려는데 그사이에 다른 워커가 락을 먼저 생성함
        print(f"[{target_id}] 스킵: 다른 워커가 방금 락을 획득함.")
        return "SKIPPED_LOCKED"
    except Exception as e:
        print(f"[{target_id}] 락 처리 중 오류: {e}")
        traceback.print_exc()
        return "FAILED_LOCK_ERROR"
    
    # 완료되지 않은 카페라면
    last_cursor = get_last_cursor_from_jsonl(output_file) # 마지막 커서
    if last_cursor:
        print(f"[{target_id}] 작업 재개: 마지막 커서 '{last_cursor[:10]}...' 부터 시작합니다.")
    else:
        print(f"[{target_id}] 작업 시작: 처음부터 수집합니다.")

    reviews_data, is_completed = scrape_reviews_by_api(target_id, max_reviews, last_cursor)
    # 파일 생성
    try:
        if len(reviews_data) > 0:
            print(f"[{target_id}] {len(reviews_data)}개 리뷰 수집됨. 파일에 이어쓰기...")
            directory = os.path.dirname(output_file)
            # 디렉토리 없으면 생성
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            # 'a' 모드로 이어쓰기
            with open(output_file, "a", encoding="utf-8") as f:
                for review in reviews_data:
                    f.write(json.dumps(review, ensure_ascii=False) + "\n")
            print(f"[{target_id}] 파일 저장 완료: {output_file}")
        else:
            print(f"[{target_id}] 이번 실행에서 수집된 새 리뷰 없음.")

        if(is_completed):
            print(f"[{target_id}] API가 '완료' 신호를 보냈습니다. 마커 파일을 생성합니다.")
            create_completion_marker(marker_file)
            return f"SUCCESS_COMPLETED: {target_id}"
        else:
            return f"INCOMPLETE: {target_id}"
    except Exception as e:
            print(f"[{target_id}] 파일 저장 중 오류 발생: {e}")
            traceback.print_exc()
            return f"FAILED_SAVE_ERROR: {target_id}"
    finally:
        # 락 해제
        if os.path.exists(lock_file):
            try:
                os.remove(lock_file)
            except Exception as e:
                print(f"[{target_id}] 락 해제 중 오류: {e}")
                traceback.print_exc()

def create_completion_marker(marker_file):
    directory = os.path.dirname(marker_file)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    # 빈 파일 생성
    with open(marker_file, "w") as f:
        pass
    print(f"[{os.path.basename(marker_file)}] 마커 파일 생성 완료.")

def main():
    SQS_QUEUE_URL = "https://sqs.ap-northeast-2.amazonaws.com/181474919825/cafe_queue"
    SQS_REGION = "ap-northeast-2"
    
    sqs = boto3.client('sqs', region_name=SQS_REGION)
    
    print("--- SQS 크롤링 워커 시작 ---")

    # 큐가 빌 때까지 무한 반복
    while True:
        try:
            print("\nSQS 큐에서 새 작업 수신 대기 중... (최대 20초)")
            
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20 # 큐가 비어있으면 20초간 대기
            )
            
            messages = response.get('Messages', [])
            
            if len(messages) > 0:
                message = messages[0]
                cafe_id = message['Body']
                receipt_handle = message['ReceiptHandle']
                
                print(f"--- 작업 시작: [Cafe ID: {cafe_id}] ---")
                result_status = process_and_save_reviews(cafe_id, 10000)
                print(f"작업 결과: [Cafe ID: {cafe_id}] - {result_status}")

                if "SUCCESS_COMPLETED" in result_status or "SKIPPED_COMPLETED" in result_status:
                    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
                    print(f"[{cafe_id}] 작업 완료, 큐에서 메시지 삭제 완료.")
                else:
                    print(f"[{cafe_id}] 작업 실패. 큐에 남겨둡니다 (자동 재시도).")
                
                # 다음 카페 작업을 받기 전, 25~35초 랜덤 대기
                time.sleep(random.uniform(25, 35))
            else: 
                print("큐가 비어있음. '진짜' 작업이 끝났는지 확인 중...")
                # 큐의 현재 상태 속성을 가져옴
                attrs = sqs.get_queue_attributes(
                    QueueUrl=SQS_QUEUE_URL,
                    AttributeNames=[
                        'ApproximateNumberOfMessages',          # 큐에 보이는 메시지 수
                        'ApproximateNumberOfMessagesNotVisible' # '처리 중'(투명)인 메시지 수
                    ]
                )
                    
                visible_count = int(attrs['Attributes']['ApproximateNumberOfMessages'])
                inflight_count = int(attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])

                # '진짜' 종료 조건:
                if visible_count == 0 and inflight_count == 0:
                    print("모든 작업(대기+처리 중)이 0입니다. 워커를 종료합니다.")
                    break
                else:
                    # 큐는 비었지만, 다른 워커가 아직 일하고 있는 경우
                    print(f"다른 워커가 아직 {inflight_count}개 작업 처리 중... 30초 후 다시 확인합니다.")
                    time.sleep(30)

        except KeyboardInterrupt:
            print("\n수동으로 종료 신호 받음. 워커를 종료합니다.")
            break
        except Exception as e:
            print("메인 루프에서 치명적 오류 발생!")
            traceback.print_exc()
            print("10초 후 재시도...")
            time.sleep(10)

    print("--- SQS 크롤링 워커 종료 ---")

if __name__ == "__main__":
    main()
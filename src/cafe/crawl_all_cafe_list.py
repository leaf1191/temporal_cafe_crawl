from playwright.sync_api import sync_playwright
import time
import re
import json
import random
import os

def parse_script_content(script_content, cafes):
    # 패턴 내부 데이터 추출
    match = re.search(r"window\.__APOLLO_STATE__\s*=\s*({.*?});", script_content, re.DOTALL)
    if match:
        json_string = match.group(1)
        try:
            initial_apollo_state = json.loads(json_string)
            for key, value in initial_apollo_state.items():
                # Key가 "RestaurantListSummary:"로 시작하는 항목만 찾음
                if key.startswith("RestaurantListSummary:"):
                    cafe_info = {
                        "id" : value.get("id"),
                        "name" : value.get("name"),
                        "category" : value.get("category")
                    }
                    cafes.append(cafe_info)
            print('파싱 및 리스트 추가 완료')

        except json.JSONDecodeError as e:
            print(f"초기 데이터 JSON 파싱 오류: {e}")
        except KeyError as e:
            print(f"초기 데이터 구조 탐색 오류 (KeyError): {e}")
    else:
        print("스크립트에서 APOLLO_STATE 패턴을 찾지 못했습니다.")

def parse_graphql_data(response_body, cafes):
    try:
        items = response_body[0].get("data", {}).get("restaurants", {}).get("items", [])
        for item in items:
            cafe_info = {
                "id" : item.get("id"),
                "name" : item.get("name"),
                "category" : item.get("category")
            }
            cafes.append(cafe_info)
        print(f'리스트 추가 완료: {len(items)}개')
    except Exception as e:
          print(f"GraphQL 데이터 파싱 중 예상치 못한 오류 발생: {e}")

    

def is_valid_cafe_list_response(response):
    if response.ok:
        try:
            data = response.json()
            items = data[0].get("data", {}).get("restaurants", {}).get("items", [])
            if items is not None and isinstance(items, list) and len(items) > 0:
                return True
        except Exception:
            return False # JSON 파싱 실패 등
    return False
    

def extract_cafe_list(url):
    script_content = None
    cafes = []
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True) 
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=10000)

            # var naver를 포함한 스크립트 검색
            js_code = """
            () => {
                const scripts = document.querySelectorAll('script'); // 모든 script 태그 선택
                const searchPattern = 'var naver=typeof naver'; // 찾으려는 정확한 패턴
                for (const script of scripts) {
                    if (script.textContent && script.textContent.includes(searchPattern)) {
                        return script.textContent; // 찾으면 내용 반환
                    }
                }
                return null; // 못 찾으면 null 반환
            }
            """
            script_content = page.evaluate(js_code) # 브라우저에서 위 코드 실행
            
            # 스크립트가 존재
            if script_content:
                print("스크립트 내용 찾음!")
                parse_script_content(script_content, cafes)

                if(len(cafes) > 0):
                    # 1페이지가 추가되었다면 페이지 이동
                    page_num = 1
                    while(True):
                        page_num+=1
                        try:
                            page_button = page.get_by_role("button", name=str(page_num), exact=True) # 페이지 버튼 찾기

                            if page_button.is_visible():
                                # GraphQL 응답 캡처를 먼저 설정하고 버튼 클릭
                                with page.expect_response(lambda res: "/graphql" in res.url and is_valid_cafe_list_response(res), timeout=10000) as response_info:
                                    print(f"{page_num}페이지 버튼 클릭...")
                                    page_button.click()
                                
                                response = response_info.value
                                print("GraphQL 응답 수신!")

                                response_body = response.json()
                                parse_graphql_data(response_body, cafes)
                                # 다음 페이지 이동까지 잠깐 대기
                                time.sleep(random.uniform(1.5, 2.0))
                            else:
                                print(f"{page_num}페이지 버튼을 찾을 수 없습니다.")
                                break
                        except Exception as e:
                            print(f"{page_num}페이지 처리 중 오류: {e}")
                            break
            else:
                # 스크립트가 존재하지 않음
                print("'var naver'을 가진 스크립트를 찾을 수 없습니다.")
            browser.close()
            
        except Exception as e:
            print(f"예외 발생: {e}")
            if 'browser' in locals() and browser.is_connected():
                browser.close()

    return cafes

def save_extracted_cafe_list(cafes, filename="./data/cafe_list.jsonl"):
    if not cafes:
        print("저장할 카페 데이터가 없습니다.")
        return
    try:
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            print(f"폴더 '{directory}'를 생성합니다.")
            os.makedirs(directory, exist_ok=True)

        with open(filename, "a", encoding="utf-8") as f:
            for cafe_info in cafes:
                f.write(json.dumps(cafe_info, ensure_ascii=False) + "\n")

        print(f"카페 데이터 {len(cafes)}건이 '{filename}'에 성공적으로 추가되었습니다.")

    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    # 우선은 성수 카페만
    target_url = "https://pcmap.place.naver.com/restaurant/list?query=%EC%84%B1%EC%88%98%20%EC%B9%B4%ED%8E%98" 
    
    cafes = extract_cafe_list(target_url)
    
    if cafes:
        save_extracted_cafe_list(cafes)
    else:
        print("추출 실패하였습니다.")
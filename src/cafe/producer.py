import json
import os
import boto3
from crawl_cafe_basic_info import load_cafe_ids_from_jsonl


def send_ids_to_sqs(queue_url, id_list):
    sqs = boto3.client('sqs', region_name='ap-northeast-2')
    batch_size = 10  # SQS 배치 전송 최대 10개
    
    print(f"총 {len(id_list)}개의 ID를 SQS 큐로 전송 시작...")

    for i in range(0, len(id_list), batch_size):
        # 10개씩 묶기
        batch_ids = id_list[i:i + batch_size]
        
        # SQS 배치 형식에 맞게 변환
        entries = []
        for index, cafe_id in enumerate(batch_ids):
            entries.append({
                'Id': str(index),       # 배치 내 고유 ID
                'MessageBody': cafe_id  # 실제 보낼 데이터
            })

        # 배치 전송
        try:
            response = sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            if 'Successful' in response:
                print(f"  {len(response['Successful'])}개 메시지 전송 성공 (ID {i+1} ~ {i+len(batch_ids)})")
            if 'Failed' in response and response['Failed']:
                print(f"  [경고] {len(response['Failed'])}개 메시지 전송 실패: {response['Failed']}")
                
        except Exception as e:
            print(f"SQS 전송 중 오류 발생: {e}")

    print("모든 ID 전송 완료.")

if __name__ == "__main__":
    QUEUE_URL = "https://sqs.ap-northeast-2.amazonaws.com/181474919825/cafe_queue"
    CAFE_LIST_FILE = "./data/cafe_list.jsonl"

    cafe_ids = load_cafe_ids_from_jsonl(CAFE_LIST_FILE)
    
    if len(cafe_ids) > 0:
        send_ids_to_sqs(QUEUE_URL, cafe_ids)
    else:
        print("전송할 ID가 없습니다.")
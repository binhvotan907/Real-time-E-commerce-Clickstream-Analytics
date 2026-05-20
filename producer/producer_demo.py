from kafka import KafkaProducer
import json
import time
import os

'''
Nhiệm vụ mô phỏng luồng dữ liệu clickstream thời gian thực bằng cách đọc dữ liệu từ file YooChoose, 
chuyển từng dòng thành một sự kiện JSON và gửi vào Kafka topic clickstream với tốc độ có kiểm soát. 
Điều này giúp tái hiện hành vi người dùng trong môi trường thương mại điện tử và cung cấp dữ liệu đầu vào cho tầng xử lý streaming bằng Apache Spark.
'''

TOPIC_NAME = "clickstream" # Đây là Kafka topic mà Producer sẽ gửi dữ liệu vào

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH = os.path.join(BASE_DIR, "data", "yoochoose-clicks.dat")

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


MAX_RECORDS = 2_000_000  

def main():
    print("=== DEMO MODE: stream 2.000.000 dòng đầu tiên ===")
    print("Reading:", FILE_PATH)

    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):

            if i >= MAX_RECORDS:
                break

            parts = line.strip().split(",")
            if len(parts) < 4:
                continue

            session_id, ts, item_id, category = parts[:4]

            try:
                # Tạo event clickstream (JSON)
                event = {
                    "session_id": int(session_id),
                    "timestamp": ts,
                    "item_id": int(item_id),
                    "category": int(category)
                }
            except ValueError:
                continue
            
            # Gửi message vào Kafka
            producer.send(TOPIC_NAME, event)

            if i % 1000 == 0: 
                print(f"[DEMO] Sent record #{i+1}: {event}")

            time.sleep(0.001)  # ~1000 events/giây

    producer.flush()
    print("=== DONE DEMO MODE – đã gửi đủ 2.000.000 dòng ===")

if __name__ == "__main__":
    main()

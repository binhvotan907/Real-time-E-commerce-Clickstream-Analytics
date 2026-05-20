from kafka import KafkaProducer
import json
import time
import os

'''
Kafka Producer trong chế độ real-time loop có nhiệm vụ mô phỏng luồng dữ liệu clickstream liên tục bằng cách đọc dữ liệu từ file YooChoose, 
chuyển đổi từng bản ghi thành JSON và gửi vào Kafka topic. Cơ chế sleep được sử dụng để điều chỉnh tốc độ gửi, 
giúp tái hiện hành vi người dùng trong hệ thống thực tế và phục vụ kiểm thử Spark Structured Streaming.
'''
TOPIC_NAME = "clickstream"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH = os.path.join(BASE_DIR, "data", "yoochoose-clicks.dat")

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

SLEEP_TIME = 0.01   # ~100 records / giây 

def stream_once():
    """
    Đọc từng dòng click -> chuyển đổi thành dạng Json -> gửi vào kafka topic "clickstream"
    """
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue

            session_id, ts, item_id, category = parts[:4]

            try:
                event = {
                    "session_id": int(session_id),
                    "timestamp": ts,
                    "item_id": int(item_id),
                    "category": int(category)
                }
            except ValueError:
                continue
            
            # Gửi dữ liệu vào Kafka
            producer.send(TOPIC_NAME, event)
            print("[REALTIME] Sent:", event)

            time.sleep(SLEEP_TIME)

def main():
    print("=== REAL-TIME LOOP MODE ===")
    print("Streaming liên tục – nhấn Ctrl + C để dừng")
    print("Reading:", FILE_PATH)

    try:
        while True:
            print(">>> Bắt đầu 1 vòng stream mới...")
            stream_once()
            print(">>> Hết file, quay lại từ đầu sau 2 giây...\n")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n=== STOP REAL-TIME STREAM (Ctrl + C) ===")

    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()

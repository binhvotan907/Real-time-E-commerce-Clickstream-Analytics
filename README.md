# Real-time E-commerce Clickstream Analytics

Project này xây dựng một pipeline phân tích clickstream thương mại điện tử theo thời gian thực. Hệ thống mô phỏng luồng người dùng click vào sản phẩm, gửi sự kiện vào Kafka, xử lý bằng Spark Structured Streaming, sau đó xuất dữ liệu và các chỉ số phân tích để phục vụ quan sát hành vi người dùng trên dashboard.

## 1. Mục tiêu project

Trong hệ thống thương mại điện tử, mỗi lượt click của người dùng có thể được xem như một sự kiện. Khi số lượng người dùng lớn, dữ liệu clickstream cần được xử lý liên tục thay vì đợi gom thành batch. Project này mô phỏng bài toán đó bằng cách:

- Đọc dữ liệu click từ bộ dữ liệu YooChoose.
- Giả lập luồng dữ liệu real-time bằng Python Kafka Producer.
- Đẩy từng click event vào Kafka topic `clickstream`.
- Dùng Spark Structured Streaming đọc dữ liệu từ Kafka.
- Tính toán các metric gần real-time về sản phẩm, danh mục, session và số lượt click.
- Ghi dữ liệu raw ra Parquet để có thể phân tích hoặc trực quan hóa bằng Power BI.

## 2. Kiến trúc hệ thống

```text
YooChoose click dataset
        |
        v
Python Producer
        |
        v
Kafka topic: clickstream
        |
        v
Spark Structured Streaming
        |
        +--> Real-time metrics on console
        +--> Raw data in Parquet format
        +--> Power BI dashboard
```

Các thành phần chính:

- **Dataset**: file `yoochoose-clicks.dat` chứa lịch sử click của người dùng.
- **Kafka Producer**: đọc file dữ liệu, chuyển từng dòng thành JSON event và gửi vào Kafka.
- **Kafka**: đóng vai trò message broker, nhận và lưu tạm luồng clickstream.
- **Spark Structured Streaming**: đóng vai trò stream consumer, đọc message từ Kafka và xử lý dữ liệu liên tục.
- **Output Parquet**: lưu dữ liệu đã parse để phục vụ phân tích downstream.
- **Power BI**: dashboard trực quan hóa kết quả phân tích.

## 3. Cấu trúc thư mục

```text
realtime-clickstream/
|-- docker/
|   `-- docker-compose.yml
|-- producer/
|   |-- producer_demo.py
|   `-- producer_test.py
|-- spark/
|   `-- spark_streaming.py
|-- dashboard/
|   `-- demo2m.pbix
|-- data/
|   `-- yoochoose-clicks.dat
|-- checkpoint/
|-- output/
|-- check_data.ipynb
|-- .gitignore
`-- README.md
```

Ghi chú: thư mục `data/`, `checkpoint/`, `output/`, file report và môi trường ảo `venv/` không được đưa lên Git.

## 4. Công nghệ sử dụng

- **Python**: viết producer gửi dữ liệu vào Kafka.
- **kafka-python**: thư viện Python để kết nối Kafka.
- **Apache Kafka**: message broker cho luồng clickstream.
- **Apache Spark Structured Streaming**: xử lý stream và tính toán metric.
- **PySpark 3.5.1**: API Spark dùng trong Python.
- **Docker Compose**: chạy Kafka và Zookeeper.
- **Parquet**: định dạng lưu dữ liệu output.
- **Power BI**: xây dựng dashboard phân tích.

## 5. Dữ liệu đầu vào

Project sử dụng bộ dữ liệu YooChoose clickstream. File dữ liệu local cần đặt tại:

```text
data/yoochoose-clicks.dat
```

Mỗi dòng có 4 trường:

```text
session_id,timestamp,item_id,category
```

Ví dụ:

```text
1,2014-04-07T10:51:09.277Z,214536502,0
1,2014-04-07T10:54:09.868Z,214536500,0
2,2014-04-07T13:56:37.614Z,214662742,0
```

Ý nghĩa các trường:

- `session_id`: mã phiên truy cập của người dùng.
- `timestamp`: thời điểm phát sinh click trong dữ liệu gốc.
- `item_id`: mã sản phẩm được click.
- `category`: mã danh mục sản phẩm.

File dữ liệu này có dung lượng lớn nên không được push lên GitHub. Khi clone project về máy khác, cần tự tạo thư mục `data/` và đặt file `yoochoose-clicks.dat` vào đó.

## 6. Mô tả các module chính

### 6.1. Kafka và Zookeeper

File:

```text
docker/docker-compose.yml
```

File này khai báo 2 service:

- `zookeeper`: service điều phối cho Kafka.
- `kafka`: broker Kafka chạy tại `localhost:9092`.

Kafka được cấu hình để producer và Spark trên máy local có thể kết nối thông qua:

```text
localhost:9092
```

### 6.2. Producer demo

File:

```text
producer/producer_demo.py
```

Chức năng:

- Đọc file `data/yoochoose-clicks.dat`.
- Parse từng dòng thành event JSON.
- Gửi event vào Kafka topic `clickstream`.
- Giới hạn tối đa `2_000_000` dòng đầu tiên.
- Sleep `0.001` giây giữa các event, tương đương khoảng 1000 event/giây.

Event gửi vào Kafka có dạng:

```json
{
  "session_id": 1,
  "timestamp": "2014-04-07T10:51:09.277Z",
  "item_id": 214536502,
  "category": 0
}
```

### 6.3. Producer real-time loop

File:

```text
producer/producer_test.py
```

Chức năng:

- Đọc toàn bộ file dữ liệu.
- Gửi từng event vào Kafka topic `clickstream`.
- Sleep `0.01` giây giữa các event, tương đương khoảng 100 event/giây.
- Khi đọc hết file, chờ 2 giây rồi quay lại đọc từ đầu.
- Phù hợp để demo luồng dữ liệu chạy liên tục.

### 6.4. Spark Streaming

File:

```text
spark/spark_streaming.py
```

Spark thực hiện các bước:

1. Tạo `SparkSession` ở chế độ local.
2. Kết nối Kafka topic `clickstream`.
3. Đọc message Kafka dưới dạng stream.
4. Parse JSON thành các cột có schema rõ ràng.
5. Gắn `event_time` bằng `current_timestamp()` để demo window processing theo thời gian chạy thực tế.
6. Ghi raw data ra Parquet.
7. Tính các metric streaming và in ra console.

Schema dữ liệu sau khi parse:

```text
session_id: integer
timestamp: string
item_id: integer
category: integer
event_time: timestamp
```

## 7. Các metric được tính

Spark Structured Streaming đang tính 5 nhóm metric:

### Metric 1: Top Product

Đếm số click theo `item_id` để xác định sản phẩm được quan tâm nhiều nhất.

```text
groupBy(item_id).count()
```

### Metric 2: Active Sessions trong 5 phút

Đếm số session khác nhau trong cửa sổ thời gian 5 phút.

```text
window(event_time, "5 minutes")
approx_count_distinct(session_id)
```

### Metric 3: Clicks per 10 seconds

Đếm tổng số click phát sinh trong mỗi cửa sổ 10 giây.

```text
window(event_time, "10 seconds")
count(*)
```

### Metric 4: Top Category

Đếm số click theo `category` để xác định danh mục được click nhiều.

```text
groupBy(category).count()
```

### Metric 5: Bounce Session

Phát hiện các session chỉ có đúng 1 click trong cửa sổ 30 phút.

```text
window(event_time, "30 minutes"), session_id
count(*) == 1
```

## 8. Cài đặt môi trường

Yêu cầu:

- Python 3.10 hoặc mới hơn.
- Docker Desktop.
- Java JDK, khuyến nghị Java 8, 11 hoặc 17.
- Git.

Tạo môi trường Python:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install pyspark==3.5.1 kafka-python
```

## 9. Cách chạy project

### Bước 1: Chuẩn bị dữ liệu

Tạo thư mục `data/` nếu chưa có, sau đó đặt file dataset vào:

```text
data/yoochoose-clicks.dat
```

### Bước 2: Chạy Kafka

Từ thư mục gốc project:

```powershell
cd docker
docker compose up -d
```

Kiểm tra container:

```powershell
docker ps
```

### Bước 3: Chạy Spark Streaming

Mở terminal mới tại thư mục gốc project:

```powershell
.\venv\Scripts\Activate.ps1
python .\spark\spark_streaming.py
```

Spark sẽ bắt đầu chờ dữ liệu từ Kafka topic `clickstream`.

### Bước 4: Chạy Producer

Mở terminal khác tại thư mục gốc project.

Chạy producer demo:

```powershell
.\venv\Scripts\Activate.ps1
python .\producer\producer_demo.py
```

Hoặc chạy producer loop liên tục:

```powershell
.\venv\Scripts\Activate.ps1
python .\producer\producer_test.py
```

## 10. Output

Raw data sau khi Spark parse được ghi ra:

```text
output/parquet
```

Checkpoint của các streaming query được lưu trong:

```text
checkpoint/
```

Các metric được in trực tiếp ra console của Spark.

Dashboard Power BI:

```text
dashboard/demo2m.pbix
```

## 11. Lưu ý về đường dẫn

Trong `spark/spark_streaming.py`, biến `BASE_PATH` đang được hard-code:

```python
BASE_PATH = "C:/Users/hokhi/Desktop/realtime-clickstream"
```

Nếu chạy project ở thư mục khác, cần sửa `BASE_PATH` thành đúng đường dẫn local. Ví dụ:

```python
BASE_PATH = "C:/Users/hokhi/Desktop/nam4_hk1/NHAP MON DU LIEU LON/realtime-clickstream"
```

Nếu không sửa, Spark có thể ghi output và checkpoint sai vị trí.

## 12. Dừng hệ thống

Dừng producer hoặc Spark bằng:

```text
Ctrl + C
```

Dừng Kafka:

```powershell
cd docker
docker compose down
```

## 13. Push project lên GitHub

Repository GitHub:

```text
https://github.com/binhvotan907/Real-time-E-commerce-Clickstream-Analytics
```

Khởi tạo Git và commit các file cần push:

```powershell
git init
git add README.md .gitignore producer spark docker dashboard check_data.ipynb
git commit -m "Initial realtime clickstream analytics project"
```

Kết nối remote và push:

```powershell
git branch -M main
git remote add origin https://github.com/binhvotan907/Real-time-E-commerce-Clickstream-Analytics.git
git push -u origin main
```

Các file/thư mục không push:

- `data/`
- `report.docx`
- `report.pdf`
- `venv/`
- `checkpoint/`
- `output/`
- file tạm như `~$Report.docx`
- file `.tmp`

## 14. Hướng phát triển thêm

Project có thể mở rộng theo các hướng:

- Ghi các metric ra file hoặc database thay vì chỉ in ra console.
- Dùng dashboard đọc trực tiếp từ output Parquet.
- Thêm metric conversion rate nếu có dữ liệu mua hàng.
- Tách cấu hình như Kafka server, topic name và output path ra file `.env`.
- Docker hóa cả producer và Spark job để chạy pipeline đầy đủ bằng Docker Compose.

# Violence\_Detection\_Videos\_Stream

>Dự án trình bày nghiên cứu và phát triển một hệ thống phát hiện hành vi bạo lực dựa trên công nghệ xử lý luồng dữ liệu thời gian thực, sử dụng Kafka, Apache Spark, và mô hình học sâu MH-BiLSTM và các cải tiếng mà chúng tôi thực hiện .

---


## Giới thiệu

`Violence_Detection_Videos_Stream` là một pipeline **end‑to‑end** cho việc:

1. Thu thập khung hình (frames) từ camera hoặc video lưu sẵn.
2. Đẩy khung hình vào Kafka (`raw‑frames`).
3. Spark Structured Streaming giải mã, trích đặc trưng (HOG) và nhóm thành chuỗi.
4. Mô hình MH‑BiLSTM phân loại thời gian thực, phát hiện hành vi bạo lực.
5. Kết quả (topic `fight‑events`) được lưu vào PostgreSQL và MongoDB, đồng thời hiện cảnh báo .


---

## Yêu cầu trước khi cài đặt

* Java 11+
* Python 3.8+
* Apache Kafka 2.\*\*
* Apache Spark 3.5.5
* PostgreSQL 12+
* MongoDB 4.4+
* FFmpeg
* Docker & Docker Compose (nếu dùng containers)

---

## Cài đặt & Cấu hình

1. **Clone repo**

   ```bash
   git clone https://github.com/your-org/Violence_Detection_Videos_Stream.git
   cd Violence_Detection_Videos_Stream
   ```

2. **Tạo môi trường Python**

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```


---

## Chạy dự án

### 1. Khởi động Kafka & Zookeeper

```bash
# Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka broker
bin/kafka-server-start.sh config/server.properties
```

### 2. Tạo topics

```bash
# Topic raw‑frames (6 partitions)
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 6 \
  --topic raw-frames

# Topic fight‑events (3 partitions)
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic fight-events
```

### 3. Chạy Spark Structured Streaming

```bash
spark-submit \
  --master local[4] \
  --packages \
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5, \
    org.postgresql:postgresql:42.6.0 \
  --conf "spark.driverEnv.JDBC_URL=$POSTGRES_JDBC_URL" \
  --conf "spark.driverEnv.JDBC_USER=$POSTGRES_USER" \
  --conf "spark.driverEnv.JDBC_PASS=$POSTGRES_PASS" \
  spark_streaming/stream_job.py
```

### 4. Chạy Kafka Producer

```bash
python kafka/kafka_producer.py
```

### 5. Chạy Kafka Consumer downstream

```bash
python kafka/consumer_downstream.py
```

---

## Cấu trúc thư mục

```
Violence_Detection_Videos_Stream/
│
├── kafka/
│   ├── kafka_producer.py        # Producer: push JPEG bytes → Kafka
│   └── consumer_downstream.py   # Consumer: trim clip → MongoDB
│
├── spark_streaming/
│   └── stream_job.py            # Spark Structured Streaming job
│
├── mh_bilstm_savedmodel/        # Thư mục SavedModel (mounted)
│
├── requirements.txt             # Python dependencies
└── docker-compose.yml           # Tập tin Docker Compose (Kafka, Spark, PG,…)
```

---

## Docker Compose (tùy chọn)

Nếu bạn muốn chạy toàn bộ stack bằng Docker:

```bash
docker-compose up --build
```

Mặc định sẽ khởi động các service: Zookeeper, Kafka, Spark, PostgreSQL, MongoDB, và app Python.

---

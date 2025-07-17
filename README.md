# Violence\_Detection\_Videos\_Stream

> Hệ thống phát hiện hành vi bạo lực trong video stream thời gian thực
> Sử dụng Apache Kafka, Spark Structured Streaming và mô hình MH‑BiLSTM.

---

## Mục lục

* [Giới thiệu](#giới-thiệu)
* [Tính năng chính](#tính-năng-chính)
* [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
* [Yêu cầu trước khi cài đặt](#yêu-cầu-trước-khi-cài-đặt)
* [Cài đặt & Cấu hình](#cài-đặt--cấu-hình)
* [Chạy dự án](#chạy-dự-án)

  * [1. Khởi động Kafka & Zookeeper](#1-khởi-động-kafka--zookeeper)
  * [2. Tạo topics](#2-tạo-topics)
  * [3. Chạy Spark Streaming](#3-chạy-spark-streaming)
  * [4. Chạy Kafka Producer](#4-chạy-kafka-producer)
  * [5. Chạy Kafka Consumer downstream](#5-chạy-kafka-consumer-downstream)
* [Cấu trúc thư mục](#cấu-trúc-thư-mục)
* [Docker Compose (tùy chọn)](#docker-compose-tùy-chọn)
* [Đóng góp](#đóng-góp)
* [License](#license)

---

## Giới thiệu

`Violence_Detection_Videos_Stream` là một pipeline **end‑to‑end** cho việc:

1. Thu thập khung hình (frames) từ camera hoặc video lưu sẵn.
2. Đẩy khung hình vào Kafka (`raw‑frames`).
3. Spark Structured Streaming giải mã, trích đặc trưng (HOG) và nhóm thành chuỗi.
4. Mô hình MH‑BiLSTM phân loại thời gian thực, phát hiện hành vi bạo lực.
5. Kết quả (topic `fight‑events`) được lưu vào PostgreSQL và MongoDB, đồng thời tạo video highlight.

---

## Tính năng chính

* **Real‑time streaming**: latency thấp, xử lý liên tục.
* **Scalable**: Kafka partitions, Spark executors, dễ dàng scale ngang.
* **Modular & Configurable**: tách biệt Producer, Spark job, Consumer.
* **Alert & Highlight**: tự động cắt clip trước/sau sự kiện, lưu trữ và cảnh báo.

---

## Kiến trúc hệ thống

```text
Camera / Video File
      │
      ▼
  Kafka Producer
      │ Topic: raw‑frames
      ▼
Spark Structured Streaming
 (decode → grayscale → resize → HOG → MH‑BiLSTM)
      │ Topic: fight‑events
      ▼
Kafka Consumer ↓─────────► PostgreSQL (long‑term)  
         │                MongoDB (JSON records + clip paths)
         ▼
     Streamlit UI (alert)
```

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

3. **Thiết lập biến môi trường**
   Tạo file `.env` với nội dung mẫu:

   ```env
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   RAW_FRAMES_TOPIC=raw-frames
   FIGHT_EVENTS_TOPIC=fight-events

   POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/violence_db
   POSTGRES_USER=bigdata
   POSTGRES_PASS=1234

   MONGO_URI=mongodb://localhost:27017
   MONGO_DB=violence_events
   ```

   rồi chạy:

   ```bash
   export $(grep -v '^#' .env | xargs)
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

## Đóng góp

1. Fork & clone repo này
2. Tạo branch feature: `git checkout -b feature/your-feature`
3. Commit code & push:

   ```bash
   git add .
   git commit -m "Add your feature"
   git push origin feature/your-feature
   ```
4. Mở Pull Request vào `main`
5. Đảm bảo tất cả tests (nếu có) đều pass, coding style và linting ok.

---

## License

Distributed under the MIT License. Xem file [`LICENSE`](./LICENSE) để biết chi tiết.

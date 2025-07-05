# Violence_Detection_Videos_Stream


Violence_Detection_Videos_Stream/
│
├── kafka/
│   ├── kafka_producer.py        # Service đẩy frame lên Kafka (raw-frames)
│   └── consumer_downstream.py   # Service lấy kết quả fight-events, lưu DB + cắt clip
│
├── model/
│   ├── mh_bilstm_savedmodel/    # SavedModel (TF Serving)
│   |   ├── variables/               # checkpoint của model
│   |   ├── saved_model.pb           # biểu đồ đồ TF graph
│   ├── hog_mh_bdlstm_best.h5    # weights Keras (cho training lại)
│   └── HOG_biLSTM.ipynb         # notebook huấn luyện
│
├── pyflink_streaming/
│   ├── Config.py                # config chung (Kafka, TF-Serving, JDBC,…)
│   ├── frame_hog.py             # MapFunction: decode frame → HOG
│   ├── sequence_accum.py        # KeyedProcessFunction: gom SEQ_LEN → sequence
│   ├── inference.py             # AsyncFunction: gọi TF-Serving → label/confidence
│   ├── sinks.py                 # Định nghĩa Kafka & JDBC sinks
│   └── pipeline.py              # Xây dựng Flink job (source→transform→sink)  
│   
|__ .Dockerfile                  # chỉ định model volume, ports,…  
│
├── .gitignore  
├── docker-compose.yml           # Orchestrate Kafka, ZK, TF-Serving, Flink…  
├── streaming_job.py             # (có thể là phiên bản chạy Flink non-modular)  
├── streamlit_app.py             # Dashboard demo với Streamlit  
├── requirements.txt             # Python deps chung  
└── README.md


├── kafka/
│   ├── kafka_producer.py        # Push JPEG bytes to Kafka (raw-frames)
│   └── consumer_downstream.py   # Consume fight-events → trim clip & Mongo
│
├── spark_streaming/
│   └── stream_job.py            # Single-file Spark Structured Streaming job
│
├── mh_bilstm_savedmodel/        # SavedModel directory (mounted)
│
├── requirements.txt             # Python deps for Spark driver/executors
└── docker-compose.yml           # Local orchestration (Kafka, Spark, PG, …)
# ───────────── Base stage ─────────────
FROM python:3.11-slim AS base

# Install system dependencies, including FFmpeg for video processing
RUN apt-get update \
 && apt-get install -y ffmpeg curl \
 && rm -rf /var/lib/apt/lists/*

# Copy core requirements and install
WORKDIR /app
COPY requirements-core.txt /app/
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements-core.txt

# ────────── Stage for PyFlink job ──────────
FROM base AS pyflink-job
COPY requirements-flink.txt /app/
RUN pip install --no-cache-dir -r requirements-flink.txt
COPY pyflink_streaming/ /app/pyflink_streaming/
WORKDIR /app/pyflink_streaming
ENTRYPOINT ["python", "pipeline.py"]

# ────────── Stage for Consumer downstream ──────────
FROM base AS consumer-downstream
COPY requirements-consumer.txt /app/
RUN pip install --no-cache-dir -r requirements-consumer.txt
COPY kafka/consumer_downstream.py /app/
WORKDIR /app
ENTRYPOINT ["python", "consumer_downstream.py"]

# ────────── Stage for Streamlit Dashboard ──────────
FROM base AS streamlit-app
COPY requirements-streamlit.txt /app/
RUN pip install --no-cache-dir -r requirements-streamlit.txt
COPY streamlit_app.py /app/
WORKDIR /app
EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.enableCORS=false"]

# ────────── Stage for Video Producer Test ──────────
FROM base AS video-producer
COPY requirements-producer.txt /app/
RUN pip install --no-cache-dir -r requirements-producer.txt
COPY kafka/kafka_producer.py /app/
WORKDIR /app
ENTRYPOINT ["python", "kafka_producer.py"]

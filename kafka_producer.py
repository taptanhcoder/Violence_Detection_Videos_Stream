import argparse
import time
import uuid
import logging
import signal
import sys
import os
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# --- Logging and shutdown handling ------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("producer")
shutdown = False

def _sig_handler(sig, _frame):
    global shutdown
    log.info("Signal %s received → initiating shutdown", sig)
    shutdown = True

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

# --- Argument parsing -------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser("Kafka video-frame producer (Spark edition)")
    p.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"),
                   help="Kafka broker address")
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "raw-frames"),
                   help="Kafka topic to publish frames")
    p.add_argument("--source", default=os.getenv("VIDEO_SOURCE", "0"),
                   help="Video source (device index or file path)")
    p.add_argument("--fps", type=float, default=float(os.getenv("FRAME_FREQUENCY", 10)),
                   help="Frames per second to send")
    p.add_argument("--jpeg-quality", type=int, default=int(os.getenv("JPEG_QUALITY", 80)),
                   help="JPEG encoding quality (0-100)")
    p.add_argument("--acks", default=os.getenv("ACKS_MODE", "all"),
                   help="Kafka acks mode")
    p.add_argument("--create-topic", action="store_true",
                   help="Create the topic if it does not exist")
    p.add_argument("--partitions", type=int, default=int(os.getenv("TOPIC_PARTITIONS", 6)),
                   help="Number of partitions for new topic")
    p.add_argument("--replication-factor", type=int, default=int(os.getenv("TOPIC_REPLICATION", 1)),
                   help="Replication factor for new topic")
    p.add_argument("--camera-id", default=os.getenv("CAMERA_ID"),
                   help="Unique camera identifier key for Kafka messages")
    return p.parse_args()

# --- Topic management -------------------------------------------------------
def ensure_topic(broker, topic, partitions, replication):
    admin = KafkaAdminClient(bootstrap_servers=[broker])
    try:
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])
        log.info("Created topic %s (partitions=%d, replication=%d)", topic, partitions, replication)
    except TopicAlreadyExistsError:
        log.debug("Topic %s already exists, skipping creation", topic)
    except KafkaError as e:
        log.error("Topic creation failed: %s", e)
        sys.exit(1)
    finally:
        admin.close()

# --- Producer factory -------------------------------------------------------
def new_producer(broker, acks):
    return KafkaProducer(
        bootstrap_servers=[broker],
        acks=acks,
        enable_idempotence=True,
        compression_type='zstd',
        linger_ms=30,
        batch_size=256*1024,
        request_timeout_ms=20000,
        max_in_flight_requests_per_connection=1,
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: v
    )

# --- Callbacks ---------------------------------------------------------------
def on_send_success(record_metadata):
    log.debug("Message sent to %s-%d@%d", record_metadata.topic, record_metadata.partition, record_metadata.offset)

def on_send_error(excp):
    log.error("Failed to send message: %s", excp)

# --- Frame generator with monotonic pacing ---------------------------------
def frames(src, fps, quality):
    cap_idx = int(src) if str(src).isdigit() else src
    cap = cv2.VideoCapture(cap_idx)
    if not cap.isOpened():
        log.error("Cannot open video source %s", src)
        sys.exit(1)
    interval = 1.0 / fps
    next_time = time.monotonic() + interval

    while not shutdown:
        ret, frame = cap.read()
        if not ret:
            log.warning("Frame read failed, reopening source")
            cap.release()
            time.sleep(1)
            cap = cv2.VideoCapture(cap_idx)
            continue

        ts = int(time.time() * 1000)
        frame_id = str(uuid.uuid4())
        ok, buf = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
        if not ok:
            continue

        yield frame_id, ts, buf.tobytes()

        # Pace next iteration
        next_time += interval
        sleep = next_time - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        else:
            next_time = time.monotonic() + interval

    cap.release()

# --- Main -------------------------------------------------------------------
def main():
    args = parse_args()
    camera_key = args.camera_id or str(uuid.uuid4())
    log.info("CAMERA_ID=%s", camera_key)

    if args.create_topic:
        ensure_topic(args.broker, args.topic, args.partitions, args.replication_factor)

    producer = new_producer(args.broker, args.acks)
    sent_count = 0

    try:
        for fid, ts, payload in frames(args.source, args.fps, args.jpeg_quality):
            try:
                producer.send(
                    args.topic,
                    key=camera_key,
                    value=payload,
                    headers=[('ts', str(ts).encode()), ('frame_id', fid.encode())]
                ) \
                .add_callback(on_send_success) \
                .add_errback(on_send_error)

                sent_count += 1
                if sent_count % 100 == 0:
                    log.info("Queued %d frames", sent_count)

            except KafkaError as e:
                log.error("Producer send error: %s", e)

            if shutdown:
                break

    finally:
        log.info("Flushing and closing producer…")
        producer.flush(timeout=10)
        producer.close(timeout=10)
        log.info("Total frames sent: %d", sent_count)

if __name__ == '__main__':
  
    import cv2
    main()
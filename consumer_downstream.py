import os
import json
import logging
import signal
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata
from kafka.errors import KafkaError
from pymongo import MongoClient, errors as mongo_errors

# --- Configuration ---------------------------------------------------------
BROKER         = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC          = os.getenv('FIGHT_TOPIC',  'fight-events')
GROUP_ID       = os.getenv('CONSUMER_GROUP', 'fight-consumer-group')
VIDEO_SRC      = os.getenv('VIDEO_SOURCE', 'rtsp://camera-stream')
BEFORE_S       = int(os.getenv('HIGHLIGHT_BEFORE_S', '2'))
AFTER_S        = int(os.getenv('HIGHLIGHT_AFTER_S', '2'))
WORKERS        = int(os.getenv('WORKER_THREADS', '4'))
POLL_TIMEOUT   = int(os.getenv('POLL_TIMEOUT_MS', '1000'))
DLQ_TOPIC      = os.getenv('DLQ_TOPIC', 'fight-events-dlq')
FFMPEG_TIMEOUT = int(os.getenv('FFMPEG_TIMEOUT_S', '10'))

MONGO_URI      = os.getenv('MONGO_URI',  'mongodb://localhost:27017/')
MONGO_DB       = os.getenv('MONGO_DB',   'video_events')
MONGO_COL      = os.getenv('MONGO_COLL', 'fight_highlights')

# --- Logging & Shutdown -----------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger('consumer')
shutdown = False

def _sig_handler(signum, frame):
    global shutdown
    log.info('Signal %s received → shutdown', signum)
    shutdown = True

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

# --- MongoDB Setup ----------------------------------------------------------
mongo = MongoClient(MONGO_URI)
coll = mongo[MONGO_DB][MONGO_COL]

# --- Kafka Factory Functions ------------------------------------------------
def create_consumer(broker, topic, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        max_poll_records=500,
        fetch_max_bytes=5*1024*1024,
        fetch_min_bytes=1,
        fetch_max_wait_ms=500,
        value_deserializer=lambda b: json.loads(b.decode('utf-8'))
    )

def create_dlq_producer(broker):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# --- Event Processing -------------------------------------------------------
def process_event(evt):
    try:
        cam   = evt.get('camera_id', '')
        ts    = int(evt.get('event_time', 0))
        label = evt.get('label', '')
        conf  = float(evt.get('confidence', 0.0))

        ev_time = datetime.fromtimestamp(ts / 1000)

        start_s = max(0, ts/1000 - BEFORE_S)
        duration = BEFORE_S + AFTER_S
        out_dir = 'highlights'
        os.makedirs(out_dir, exist_ok=True)
        fname = f"{cam}_{ts}.mp4"
        out_path = os.path.join(out_dir, fname)

        cmd = [
            'ffmpeg', '-loglevel', 'error', '-y',
            '-ss', f"{start_s:.3f}", '-i', VIDEO_SRC,
            '-t', str(duration), '-c', 'copy', out_path
        ]
        proc = subprocess.run(cmd, stdout=subprocess.DEVNULL,
                              stderr=subprocess.PIPE,
                              timeout=FFMPEG_TIMEOUT)
        clip_path = out_path if proc.returncode == 0 else None
        if clip_path:
            log.info('Saved highlight %s', clip_path)
        else:
            log.error('FFmpeg failed for %s: %s', fname,
                      proc.stderr.decode('utf-8', errors='ignore'))

        doc = {
            'camera_id':  cam,
            'event_time': ev_time,
            'timestamp':  ts,
            'label':      label,
            'confidence': conf,
            'clip_path':  clip_path
        }
        coll.insert_one(doc)
        return True

    except (subprocess.TimeoutExpired, KafkaError,
            mongo_errors.PyMongoError, Exception) as e:
        log.exception('Processing event failed: %s', e)
        return False

# --- Main Loop --------------------------------------------------------------
def main():
    global shutdown
    consumer = create_consumer(BROKER, TOPIC, GROUP_ID)
    dlq_producer = create_dlq_producer(BROKER)
    executor = ThreadPoolExecutor(max_workers=WORKERS)
    pending  = {}

    log.info('Consumer started on topic %s [group=%s]', TOPIC, GROUP_ID)

    try:
        while not shutdown:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT)
            for tp, msgs in records.items():
                for msg in msgs:
                    fut = executor.submit(process_event, msg.value)
                    pending[fut] = (tp, msg.offset, msg.value)

            done, _ = wait(pending.keys(), timeout=0)
            for fut in done:
                tp, off, raw_evt = pending.pop(fut)
                success = fut.result()
                if not success:
                    # gửi event lỗi vào DLQ để điều tra
                    dlq_producer.send(DLQ_TOPIC, raw_evt)
                # commit offset dù thành công hay thất bại
                consumer.commit({tp: OffsetAndMetadata(off+1, None)})

    finally:
        log.info('Shutting down: waiting for tasks...')
        shutdown = True
        executor.shutdown(wait=True)
        consumer.close()
        dlq_producer.close()
        mongo.close()
        log.info('Consumer stopped')

if __name__ == '__main__':
    main()

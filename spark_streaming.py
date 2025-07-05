import os
# ————— Tắt hoàn toàn GPU cho driver để tránh xung đột plugin CUDA —————
os.environ['CUDA_VISIBLE_DEVICES'] = ''

import re
import cv2
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, pandas_udf, PandasUDFType
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    FloatType, ArrayType, LongType
)

# --- Configurations ---------------------------------------------------------
KAFKA_BROKER     = os.getenv('KAFKA_BROKER',    'localhost:9092')
RAW_TOPIC        = os.getenv('RAW_TOPIC',       'raw-frames')
FIGHT_TOPIC      = os.getenv('FIGHT_TOPIC',     'fight-events')
MODEL_DIR        = os.getenv('MODEL_DIR',       'mh_bilstm_savedmodel')
JDBC_URL         = 'jdbc:postgresql://localhost:5433/violence_db_dt'
JDBC_USER        = 'dtnghia'
JDBC_PASS        = '1234'
PARALLELISM      = int(os.getenv('PARALLELISM',  '4'))
SEQ_LEN          = int(os.getenv('SEQ_LEN',      '16'))
CONF_THRESH      = float(os.getenv('CONF_THRESH','0.6'))
CKPT_DIR         = os.getenv('CHECKPOINT_DIR',  './chk/fight-stream')

# --- Helpers ---------------------------------------------------------------
def parse_interval(s: str, default: str) -> str:
    if not s:
        return default
    s = s.strip().lower()
    if re.fullmatch(r'\d+', s):
        return f"{s} seconds"
    m = re.fullmatch(r'(\d+)([smhd])', s)
    if m:
        num, unit = m.groups()
        unit_map = {'s':'seconds','m':'minutes','h':'hours','d':'days'}
        return f"{num} {unit_map[unit]}"
    if re.fullmatch(r'\d+\s+[a-z]+', s):
        return s
    raise ValueError(f"Cannot parse interval: {s!r}")

WATERMARK_DELAY  = parse_interval(os.getenv('WATERMARK_DELAY',''),  '5 seconds')
TRIGGER_INTERVAL = parse_interval(os.getenv('TRIGGER_INTERVAL',''),'2 seconds')

# HOG params
HOG_PARAMS = dict(
    win_size     = (64, 64),
    block_size   = (16, 16),
    block_stride = (8, 8),
    cell_size    = (8, 8),
    nbins        = 9,
)

# --- Schema for prediction results -----------------------------------------
schema_pred = StructType([
    StructField('camera_id', StringType()),
    StructField('event_time', TimestampType()),
    StructField('label', StringType()),
    StructField('confidence', FloatType()),
])

# --- UDF: compute HOG once per worker --------------------------------------
@pandas_udf(ArrayType(FloatType()), PandasUDFType.SCALAR)
def hog_transform_series(img_bytes: pd.Series) -> pd.Series:
    if not hasattr(hog_transform_series, '_hog'):
        hog_transform_series._hog = cv2.HOGDescriptor(
            HOG_PARAMS['win_size'],
            HOG_PARAMS['block_size'],
            HOG_PARAMS['block_stride'],
            HOG_PARAMS['cell_size'],
            HOG_PARAMS['nbins']
        )
    hog = hog_transform_series._hog
    def compute(b: bytes):
        arr = np.frombuffer(b, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
        img = cv2.resize(img, HOG_PARAMS['win_size'])
        return hog.compute(img).flatten().tolist()
    return img_bytes.map(compute)

# --- GROUPED_MAP UDF: sliding window seq ----------------------------------
@pandas_udf(
    StructType([
        StructField('camera_id', StringType()),
        StructField('timestamp', LongType()),
        StructField('seq', ArrayType(ArrayType(FloatType())))
    ]),
    PandasUDFType.GROUPED_MAP
)
def accumulate_seq(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values('timestamp')
    cam = pdf['camera_id'].iat[0]
    buf, out = [], []
    for ts, feat in zip(pdf['timestamp'], pdf['hog']):
        ts_ms = int(ts.value // 1_000_000)
        buf.append(feat)
        if len(buf) >= SEQ_LEN:
            out.append((cam, ts_ms, buf[-SEQ_LEN:]))
    return pd.DataFrame(out, columns=['camera_id','timestamp','seq'])

# --- mapInPandas UDF: lazy-load TF model, disable GPU ---------------------
# --- mapInPandas UDF: lazy-load TF model & lọc sequence hợp lệ -------------
def predict_batches(pdf_iter):

    import os, tensorflow as _tf

    # *** tắt GPU trong Python worker ***
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    try:
        _tf.config.set_visible_devices([], "GPU")
    except Exception:
        pass

    # *** lazy-load model 1 lần / worker ***
    if not hasattr(predict_batches, "_infer"):
        mdl = _tf.saved_model.load(MODEL_DIR)
        predict_batches._infer = mdl.signatures["serving_default"]
    infer = predict_batches._infer

    LABELS = ["NonFight", "Fight"]

    for pdf in pdf_iter:
        # ------------------------------------------------------------------
        # 1) Lọc sequence hợp lệ
        good_cam, good_ts, good_seqs = [], [], []
        for cam, ts, seq in zip(pdf["camera_id"], pdf["timestamp"], pdf["seq"]):
            # seq phải đủ SEQ_LEN & tất cả frame vector cùng kích thước
            if (isinstance(seq, list) and len(seq) == SEQ_LEN
                    and all(isinstance(f, list) for f in seq)):
                feat_len = len(seq[0])
                if all(len(f) == feat_len for f in seq):
                    good_cam.append(cam)
                    good_ts.append(ts)
                    good_seqs.append(seq)

        if not good_seqs:          # Không còn dữ liệu hợp lệ -> skip batch
            continue

        # ------------------------------------------------------------------
        # 2) Inference
        seq_arr = np.stack(good_seqs).astype(np.float32)      # (B, SEQ_LEN, feat)
        scores  = infer(_tf.constant(seq_arr))["scores"].numpy()
        idx     = scores.argmax(axis=1)
        confs   = scores[np.arange(len(idx)), idx]
        labels  = [LABELS[i] for i in idx]

        # ------------------------------------------------------------------
        # 3) Yield kết quả
        yield pd.DataFrame({
            "camera_id":  good_cam,
            "event_time": pd.to_datetime(good_ts, unit="ms"),
            "label":      labels,
            "confidence": confs.astype(np.float32)
        })

# --- Main streaming pipeline -----------------------------------------------
def main():
    spark = (
        SparkSession.builder.appName('FightDetectionSpark')
            .config('spark.sql.shuffle.partitions', PARALLELISM)
            .config('spark.driverEnv.CUDA_VISIBLE_DEVICES', '')
            .config('spark.executorEnv.CUDA_VISIBLE_DEVICES', '')
            .config('spark.sql.execution.arrow.maxRecordsPerBatch', '1024')
            .config('spark.python.worker.reuse', 'true')
            .config(
                'spark.sql.streaming.stateStore.providerClass',
                'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider'
            )
            .getOrCreate()
    )

    raw = (
        spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', KAFKA_BROKER)
            .option('subscribe', RAW_TOPIC)
            .option('startingOffsets', 'latest')
            .load()
    )

    parsed = (
        raw.selectExpr(
            "CAST(key AS STRING) AS camera_id",
            "value",
            "timestamp"
        )
        .withWatermark('timestamp', WATERMARK_DELAY)
    )

    hog_df = (
        parsed
        .withColumn('hog', hog_transform_series(col('value')))
        .select('camera_id','timestamp','hog')
    )

    seq_df = hog_df.groupBy('camera_id').apply(accumulate_seq)
    preds  = seq_df.mapInPandas(predict_batches, schema_pred)
    fights = preds.filter(
        (col('label')=='Fight') & (col('confidence')>=CONF_THRESH)
    )

    def sink_batch(df, epoch_id):
        # — Gửi sang Kafka —
        kafka_df = df.selectExpr(
            "CAST(camera_id AS STRING) AS key",
            "to_json(struct(camera_id, event_time, label, confidence)) AS value"
        )
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", FIGHT_TOPIC) \
            .save()

        # — Ghi sang PostgreSQL —
        pg_df = df.withColumnRenamed("confidence", "probability") \
                  .select("camera_id", "event_time", "probability")
        pg_df.write \
             .mode("append") \
             .jdbc(
                 url=JDBC_URL,
                 table="fight_events",
                 properties={
                    "user":     JDBC_USER,
                    "password": JDBC_PASS,
                    "driver":   "org.postgresql.Driver"
                 }
             )


    (fights.writeStream
           .foreachBatch(sink_batch)
           .option('checkpointLocation', CKPT_DIR)
           .trigger(processingTime=TRIGGER_INTERVAL)
           .outputMode('append')
           .start()
    )
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    main()

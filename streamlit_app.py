import os, json, time, threading, queue
from collections import deque, Counter

import streamlit as st
import cv2, numpy as np
from kafka import KafkaConsumer


RAW_TOPIC    = os.getenv("RAW_TOPIC",   "raw-frames")
ALERT_TOPIC  = os.getenv("FIGHT_TOPIC", "fight-events")
BROKER       = os.getenv("KAFKA_BROKER","localhost:9092")

st.set_page_config(page_title="Violence-Detection Demo", layout="wide")

mode        = st.sidebar.radio("Source", ["Kafka stream", "Local file"])
fps_target  = st.sidebar.slider("Display FPS", 1, 30, 10)
conf_thresh = st.sidebar.slider("Alert threshold", 0.0, 1.0,
                                float(os.getenv("CONF_THRESH", .6)))
start_btn   = st.sidebar.button("▶ Run")


frame_q = queue.Queue(maxsize=500)
alert_q = queue.Queue(maxsize=200)

def consume_frames():
    consumer = KafkaConsumer(
        RAW_TOPIC, bootstrap_servers=[BROKER],
        value_deserializer=lambda b: b,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        consumer_timeout_ms=10_000,
    )
    for msg in consumer:
        try: frame_q.put_nowait(msg.value)
        except queue.Full: pass

def consume_alerts():
    consumer = KafkaConsumer(
        ALERT_TOPIC, bootstrap_servers=[BROKER],
        value_deserializer=lambda b: json.loads(b.decode()),
        enable_auto_commit=False,
        auto_offset_reset="latest",
        consumer_timeout_ms=10_000,
    )
    for msg in consumer:
        try: alert_q.put_nowait(msg.value)
        except queue.Full: pass


if start_btn:
    # a) nguồn video
    if mode == "Kafka stream":
        threading.Thread(target=consume_frames,  daemon=True).start()
        threading.Thread(target=consume_alerts, daemon=True).start()
    else:
        upload = st.sidebar.file_uploader("Upload video", ["mp4", "avi"])
        if not upload: st.stop()
        tmp = "/tmp/_demo.mp4"
        open(tmp, "wb").write(upload.getbuffer())
        vcap = cv2.VideoCapture(tmp)

    # b) bố cục
    col_vid, col_dash = st.columns([4, 1.7], gap="small")

    with col_vid:
        st.markdown(
            """
            <style>
            .video-frame img{width:100% !important;height:auto !important;border-radius:4px;}
            .status{padding:6px 10px;border-radius:6px;font-size:18px;font-weight:600;
                    background:#eee;margin-top:6px;}
            </style>
            """,
            unsafe_allow_html=True,
        )
        video_box  = st.empty()
        status_box = st.empty()
        fps_box    = st.empty()

    with col_dash:
        st.subheader("Dashboard")
        kpi_frame  = st.metric("Frames", 0)
        kpi_alert  = st.metric("Alerts", 0)
        table_box  = st.empty()

    # c) state
    alerts_log = deque(maxlen=50)
    counters   = Counter()
    last_time  = time.time()

    # d) loop
    while True:
        # ---------- 1. Lấy khung ----------
        frame = None
        if mode == "Kafka stream":
            try:
                buf = frame_q.get(timeout=1)
                img = cv2.imdecode(np.frombuffer(buf, np.uint8), cv2.IMREAD_COLOR)
                frame = img
            except queue.Empty: pass
        else:
            ok, fr = vcap.read()
            if ok: frame = fr

        if frame is not None:
            counters["frame"] += 1
            video_box.image(frame, channels="BGR", use_container_width=True)

        # ---------- 2. Alert ----------
        try:
            msg = alert_q.get_nowait()
            if msg["confidence"] >= conf_thresh:
                counters["alert"] += 1
                entry = {
                    "Time": msg["event_time"],
                    "Cam":  msg["camera_id"],
                    "Conf": f'{msg["confidence"]:.2f}',
                }
                alerts_log.appendleft(entry)

                # popup + status bar
                st.toast(f'🚨  Fight at {entry["Cam"]}  ({entry["Conf"]})')
                status_box.markdown(
                    f'<div class="status" style="background:#d33;color:#fff">'
                    f'🚨  FIGHT  |  {entry["Cam"]}  •  {entry["Conf"]}</div>',
                    unsafe_allow_html=True,
                )
        except queue.Empty:
            # hiển thị trạng thái bình thường
            status_box.markdown(
                '<div class="status">Status: <b>Normal</b></div>',
                unsafe_allow_html=True,
            )

        # ---------- 3. Dashboard ----------
        kpi_frame.metric("Frames", counters["frame"])
        kpi_alert.metric("Alerts", counters["alert"])
        if alerts_log:
            table_box.table(list(alerts_log))

        # ---------- 4. Giới hạn FPS ----------
        dt = 1.0 / fps_target - (time.time() - last_time)
        if dt > 0: time.sleep(dt)
        last_time = time.time()

from diagrams import Diagram, Cluster, Edge
from diagrams.programming.language import Python
from diagrams.onprem.client import Client
from diagrams.onprem.compute import Server
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Spark
from diagrams.onprem.database import Postgresql, Mongodb
from diagrams.onprem.monitoring import Grafana

graph_attr = {
    "fontsize": "18",
    "splines": "spline",
    "rankdir": "LR", 
    "pad": "0.5",     
}

node_attr = {
    "fontcolor": "black",
    "fontsize": "14",
    "penwidth": "2",

}

with Diagram(
    name="Real-Time Violence Detection Pipeline",
    filename="pipeline",
    show=False,
    outformat="png",
    graph_attr=graph_attr,
    node_attr=node_attr,
):

    # 1. Ingestion
    with Cluster("Ingestion", graph_attr={"labeljust": "l"}):
        cam      = Client("Camera\n(OpenCV)")
        producer = Python("Producer\n(Python + CV)")

    # 2. Stream Processing
    with Cluster("Stream Processing", graph_attr={"labeljust": "l"}):
        airflow = Airflow("Airflow\n(ETL)")
        kafka   = Kafka("Kafka\nraw-frames / fight-events")
        spark   = Spark("Spark\nStreaming\n(micro-batch + HOG)")
        mh      = Server("MH‑BiLSTM\nInference")

    # 3. Storage
    with Cluster("Storage & Highlights", graph_attr={"labeljust": "l"}):
        pg    = Postgresql("PostgreSQL\n(metadata)")
        mongo = Mongodb("MongoDB\n(events + clips)")

    # 4. Visualization
    ui = Grafana("Streamlit UI\n(Dashboard & Alerts)")

    # --- Edges & Flows ---
    cam       >> Edge(label="JPEG frames",       tailport="e", headport="w") >> producer
    producer  >> Edge(label="trigger ETL")                                  >> airflow
    airflow   >> Edge(label="→ raw-frames")                                 >> kafka
    kafka     >> Edge(label="readStream")                                   >> spark
    spark     >> Edge(label="extract HOG")                                  >> mh

    # Model outputs
    mh        >> Edge(label="→ fight-events")                               >> kafka
    mh        >> Edge(label="store metadata")                               >> pg
    mh        >> Edge(label="store events + clips")                         >> mongo

    # Dashboard
    [pg, mongo] >> Edge(label="visualize / alerts")                         >> ui

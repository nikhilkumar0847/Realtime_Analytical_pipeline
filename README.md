# Real-Time E-Commerce Clickstream Analytics Pipeline

About the E-commerce realtime analytical pipeline

A real-time streaming data pipeline that ingests simulated e-commerce clickstream events, processes them with Spark Structured Streaming, and lands them through a medallion (bronze/silver/gold) architecture ---- built with Kafka, PySpark, Delta Lake, Airflow, and Azure.

## Current Project Status

-> [x] Local dev environment (Python 3.14 venv)
-> [x] PySpark verified working locally
-> [x] Docker Compose for Kafka + Zookeeper
-> [x] Kafka producer — tested and confirmed working
-> [x] Spark Structured Streaming job — tested, confirmed writing real parquet output (bronze + windowed silver aggregation)
-> [x] Airflow running via Docker (postgres + webserver + scheduler), custom image built successfully with Java 17 + PySpark 3.5.0
-> [x] DAG gold_layer_pipeline visible in Airflow UI with correct 2 tasks (`run_gold_aggregation` >> `run_data_quality_check`)
-> [x] (run_gold_aggregation) task — fixed and passing
-> [x] (run_data_quality_check) task — fixed and passing
-> [x] End-to-end DAG run verified fully green in Airflow UI
-> [ ] Great Expectations data quality checks — not started
-> [ ] Azure cloud deployment — not started (Terraform scripts written, not yet run)
-> [ ] Power BI dashboard — not started
-> [ ] Streaming job containerization

## Tech Stack that uses in this project

| Category | Tools |
|----------|-------|

| Language | Python |
| Streaming broker | Apache Kafka, Zookeeper |
| Stream processing | Apache Spark (Structured Streaming) |
| Storage / table format | Delta Lake |
| Orchestration | Apache Airflow |
| Data quality | Great Expectations |
| Cloud storage | Azure Data Lake Storage Gen2 |
| Cloud messaging | Azure Event Hubs |
| Cloud compute | Azure Databricks |
| Cloud warehouse | Azure Synapse Analytics |
| BI / Visualization | Power BI |
| Containerization | Docker, Docker Compose |
| Infrastructure as Code | Terraform |
| CI/CD | GitHub Actions |
| Version control | Git, GitHub |

## Setup So Far

### Prerequisites
-> Python 3.11+ (developed using 3.14)
-> Docker Desktop with WSL2 backend (Windows)
-> Java 11+ (required by PySpark)

### Local environment
```
python -m venv venv
.\venv\Scripts\Activate.ps1      # Windows
pip install -r requirements.txt
```

### Local Kafka (via Docker Compose)

docker-compose up -d
docker ps   # confirm kafka and zookeeper containers are running


## Project Structure (so far)
```
This is the step by step structure
.
├── venv/                            # virtual environment (not committed)
├── kafka_producer/
│   └── generate_event.py            # Kafka producer simulating clickstream events
├── streaming/
│   └── stream_processing.py         # Spark Structured Streaming job (bronze + silver layers)
├── docker-compose.yml                # local Kafka + Zookeeper setup
├── requirements.txt
├── test_spark.py                    # verification script confirming PySpark runs locally
└── README.md
```



## Debugging Journey on the Airflow DAG

While building the Airflow orchestration, the `run_gold_aggregation` task kept failing with a Parquet schema-inference error despite the data file clearly existing. Root cause: the streaming job ran on Windows outside Docker, so Spark's internal `_spark_metadata` log stored an absolute Windows path that the Linux-based Airflow container couldn't resolve — deleting the stale metadata folder fixed it. That unblocked the aggregation task, but exposed a second issue: `run_data_quality_check` kept failing with "gold table is empty," which traced back to Structured Streaming's watermark semantics --- a 5-minute window with a 10-minute watermark never emitted, since my test events were sent in short bursts rather than over real elapsed time. Letting the producer and streaming job run continuously for 15-20 minutes let the watermark advance naturally, closing the window and producing real output. Together, this was a good lesson in separating infrastructure/environment bugs from actual streaming-semantics bugs, even when both surface as the same "no data" symptom.


## Important Stuffs / Lessons Learned

-> Ran into a Python 3.14 vs. PySpark compatibility question early on — PySpark's official support currently tops out around Python 3.11, but unpinned installs worked fine in practice for local development on 3.14.

-> Hit a Windows-specific issue where Spark's internal worker processes were being intercepted by the Windows Store's Python stub instead of the venv's Python — fixed by setting PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON 
environment variables explicitly.

-> Kafka producer initially failed with KafkaTimeoutError because the Kafka Docker container wasn't running yet — confirmed via docker ps, then started it with docker-compose up -d before retrying.

-> PySpark 4.1.1 requires Java 17 or 21 specifically — initially had Java 24 installed, which caused a JAVA_GATEWAY_EXITED error; had to install Java 17 alongside the existing version.

-> Hit the classic Windows "HADOOP_HOME and hadoop.home.dir are unset" error — fixed by downloading winutils.exe and hadoop.dll and setting HADOOP_HOME.

-> Encountered a known Spark bug (SPARK-53042) causing a random NullPointerException on Windows + Java 17 during BlockManager registration — fixed by explicitly setting spark.driver.bindAddress and spark.driver.host to 127.0.0.1.

-> Project folder inside OneDrive caused intermittent file-locking issues when deleting Spark checkpoint files during testing — worth keeping build folders outside synced directories in future projects.

-> Confirmed successful end-to-end run: verified via file counts in both bronze (25 files) and silver (5 files) layers after a clean checkpoint reset.

-> Stale (_spark_metadata) with absolute Windows host paths**: Spark's streaming commit log recorded (file:///C:/Users/...) paths since the writer ran on Windows outside Docker. Airflow's Linux container couldn't resolve that path, so it saw zero committed files despite the real parquet file existing. Fix: delete (_spark_metadata) before batch reads; permanent fix is containerizing the streaming writers.

-> Windowed aggregations don't emit until the watermark passes window-end**: With [outputMode("append")], a 5-minute window + 10-minute watermark never closed because test events were sent in short bursts, not spread over real elapsed time. Fix: ran producer + streaming job together continuously for 15-20 minutes so wall-clock time advanced the watermark naturally.

-> startingOffsets: earliest [reprocesses full Kafka topic history on every restart**: Every streaming job restart reread all messages ever sent to the topic, not just new ones — explains why aggregated windows spanned multiple days despite short individual test runs. Worth switching to latest] post-debugging.

After these steps now moved onto the data quality check.
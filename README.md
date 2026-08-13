# Real-Time E-Commerce Clickstream Analytics Pipeline

About the E-commerce realtime analytical pipeline

A real-time streaming data pipeline that ingests simulated e-commerce clickstream events, processes them with Spark Structured Streaming, and lands them through a medallion (bronze/silver/gold) architecture ---- built with Kafka, PySpark, Delta Lake, Airflow, and Azure.

## Current Progress

- [x] Local dev environment set up (Python virtual environment)
- [x] PySpark installed and verified working locally
- [x] Docker Compose configuration for local Kafka + Zookeeper   # There is the two project Kafka and the Zookeper
- [X] Kafka event producer (Python)
- [X] Spark Structured Streaming job
- [X] Airflow orchestration DAG
- [ ] Data quality checks (Great Expectations)
- [ ] Azure cloud deployment
- [ ] Power BI dashboard

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

## Important Stuffs / Lessons Learned

-> Ran into a Python 3.14 vs. PySpark compatibility question early on — PySpark's official support currently tops out around Python 3.11, but unpinned installs worked fine in practice for local development on 3.14.
-> Hit a Windows-specific issue where Spark's internal worker processes were being intercepted by the Windows Store's Python stub instead of the venv's Python — fixed by setting PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON environment variables explicitly.
-> Kafka producer initially failed with KafkaTimeoutError because the Kafka Docker container wasn't running yet — confirmed via docker ps, then started it with docker-compose up -d before retrying.
-> PySpark 4.1.1 requires Java 17 or 21 specifically — initially had Java 24 installed, which caused a JAVA_GATEWAY_EXITED error; had to install Java 17 alongside the existing version.
-> Hit the classic Windows "HADOOP_HOME and hadoop.home.dir are unset" error — fixed by downloading winutils.exe and hadoop.dll and setting HADOOP_HOME.
-> Encountered a known Spark bug (SPARK-53042) causing a random NullPointerException on Windows + Java 17 during BlockManager registration — fixed by explicitly setting spark.driver.bindAddress and spark.driver.host to 127.0.0.1.
-> Project folder inside OneDrive caused intermittent file-locking issues when deleting Spark checkpoint files during testing — worth keeping build folders outside synced directories in future projects.
-> Confirmed successful end-to-end run: verified via file counts in both bronze (25 files) and silver (5 files) layers after a clean checkpoint reset.

-> After that going to orchestrate the data's with the help of Airflow and here will verify the Gold data.

-> Then after working on the orchestarted data

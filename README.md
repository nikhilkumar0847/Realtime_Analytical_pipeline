# Real-Time E-Commerce Clickstream Analytics Pipeline

About the E-commerce realtime analytical pipeline

A real-time streaming data pipeline that ingests simulated e-commerce clickstream events, processes them with Spark Structured Streaming, and lands them through a medallion (bronze/silver/gold) architecture ---- built with Kafka, PySpark, Delta Lake, Airflow, and Azure.

## Current Progress

- [x] Local dev environment set up (Python virtual environment)
- [x] PySpark installed and verified working locally
- [x] Docker Compose configuration for local Kafka + Zookeeper
- [ ] Kafka event producer (Python)
- [ ] Spark Structured Streaming job
- [ ] Airflow orchestration DAG
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
```bash
python -m venv venv
.\venv\Scripts\Activate.ps1      # Windows
pip install -r requirements.txt
```

### Local Kafka (via Docker Compose)
```bash
docker-compose up -d
docker ps   # confirm kafka and zookeeper containers are running
```

## Project Structure (so far)

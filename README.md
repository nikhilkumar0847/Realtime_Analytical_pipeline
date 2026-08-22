# Real-Time E-Commerce Clickstream Analytics Pipeline

A real-time streaming data pipeline that ingests simulated e-commerce clickstream events, processes them with Spark Structured Streaming, and lands them through a medallion (bronze/silver/gold) architecture — built with Kafka, PySpark, Airflow, Great Expectations, and Azure.

----------------------------------------------------------

## Current Project Status

-> [x] Local dev environment (Python 3.14 venv)
-> [x] PySpark verified working locally
-> [x] Docker Compose for Kafka + Zookeeper
-> [x] Kafka producer — tested and confirmed working
-> [x] Spark Structured Streaming job — tested, confirmed writing real parquet output (bronze + windowed silver aggregation)
-> [x] Airflow running via Docker (postgres + webserver + scheduler), custom image built successfully with Java 17 + PySpark 3.5.0
-> [x] DAG `gold_layer_pipeline` visible in Airflow UI with correct 2 tasks (`run_gold_aggregation` >> `run_data_quality_check`)
-> [x] `run_gold_aggregation` task — fixed and passing
-> [x] `run_data_quality_check` task — fixed and passing
-> [x] End-to-end DAG run verified fully green in Airflow UI
-> [x] Great Expectations data quality checks — implemented locally, validates silver layer (null checks, row count, value ranges)
-> [ ] Azure cloud deployment — not started (Terraform scripts written, not yet run)
-> [ ] Power BI dashboard — not started

-> [**] Streaming job containerization — attempted, [blocked by unresolved Spark state-store bug in Docker] (**mentioned the problem i had in  the note respectively**) ; streaming job currently runs on Windows directly  ( had problem with the containerization in the docker with spark)


**Note on streaming job containerization with the spark**:

### ** Attempted to containerize on the docker with the spark help**

 `stream_processing.py` to eliminate a manual `_spark_metadata` cleanup step needed when running on Windows. Hit a persistent `CANNOT_LOAD_STATE_STORE.CANNOT_READ_DELTA_FILE_NOT_EXISTS` error in Spark's stateful streaming state store, which recurred even after resetting checkpoints and switching to a Docker-managed volume. Root cause not fully identified — likely a deeper Spark/Docker filesystem interaction. The streaming job runs reliably on Windows directly; containerizing it remains a documented future improvement.

-------------------------------------------------------------------------------------

## Tech Stack

| Category | Tools |
|---|---|
| Language | Python |
| Streaming broker | Apache Kafka, Zookeeper |
| Stream processing | Apache Spark Structured Streaming |
| Storage format | Plain Parquet (medallion architecture: bronze/silver/gold) |
| Orchestration | Apache Airflow |
| Data quality | Great Expectations (local), PySpark assertions (in-pipeline) |
| Cloud storage | Azure Data Lake Storage Gen2 |
| Cloud messaging | Azure Event Hubs |
| Cloud compute | Azure Databricks |
| Cloud warehouse | Azure Synapse Analytics |
| BI / Visualization | Power BI |
| Containerization | Docker, Docker Compose |
| Infrastructure as Code | Terraform |
| Version control | Git, GitHub |

------------------------------------------------------------------------------------------

## Setups


### Prerequisites

-> Python 3.13+ (developed using 3.14 locally; Python 3.13 venv used for Great Expectations due to wheel availability)
-> Docker Desktop with WSL2 backend (Windows)
-> Java 17 or 21 (required by PySpark — Java 24 will not work, learned from some lessons)


### Local environment

```bash
python -m venv venv
.\venv\Scripts\Activate.ps1      # For the windows work
pip install -r requirements.txt
```

### Great Expectations (separate venv — Python 3.13 required for the better and work efficiently)

```bash
py -3.13 -m venv venv_gx
venv_gx\Scripts\activate
pip install great_expectations pandas pyarrow
python great_expectations_check.py
```

### Local Kafka (via Docker Compose and for the containerization) 


```bash
docker-compose up -d # (taking back all over the data)
docker ps   # confirm kafka and zookeeper containers are running (checking the process status)
```

### Airflow (via Docker Compose)

```bash
docker compose -f docker-compose-airflow.yml up -d
# UI available at http://localhost:8080 — login: admin / admin
```

----------------------------------------------------------------------------------------

## Project Structure

```
.
├── venv/                              # Python 3.14 venv — Kafka/Spark/Airflow scripts (not committed)
├── venv_gx/                           # Python 3.13 venv — Great Expectations only (not committed)
├── kafka_producer/
│   └── generate_event.py              # Kafka producer simulating clickstream events
├── streaming/
│   └── stream_processing.py           # Spark Structured Streaming job (bronze + silver layers)
├── dags/
│   └── gold_layer_pipeline.py         # Airflow DAG: gold aggregation + data quality check
├── great_expectations_check.py        # GX validation script — validates silver layer parquet output
├── docker-compose.yml                 # Local Kafka + Zookeeper
├── docker-compose-airflow.yml         # Airflow (postgres + webserver + scheduler)
├── docker_file_setup.airflow          # Custom Airflow Dockerfile (Java 17 + PySpark 3.5.0)
├── requirements.txt
├── test_spark.py                      # Verification script confirming PySpark runs locally
└── README.md
```

--------------------------------------------------------------------------------------------

## Data Quality

Great Expectations (GX Core) is used to validate the silver layer Parquet output before the gold aggregation results are considered final.

**Validation script:** `great_expectations_check.py`

### Expectations defined on `data/silver/category_window_agg`:

-> Table row count >= 1 (confirms data is non-empty)
-> `category` column has no null values
-> `window` column has no null values
-> `event_count` column values >= 0 (no negative counts)

### How it works:
The script reads all silver layer Parquet partition files via pandas, loads them into a GX ephemeral context, and runs the defined expectations as a checkpoint. If any expectation fails, the script exits with code 1. The Airflow DAG also runs an in-pipeline PySpark quality check on the gold layer (`run_data_quality_check` task) validating row count and null categories directly via Spark.

### Design note:
GX Core does not yet officially support Python 3.14. A separate Python 3.13 venv (`venv_gx/`) is used for local development and testing of the GX script. The Airflow container (based on `apache/airflow:2.8.0`, which uses Python 3.11 internally) has no version conflict.

---

## Debugging Journey with the airflow dag of the gold_aggregation_data and the data_quality_check

### Airflow DAG failures
The `run_gold_aggregation` task kept failing with a Parquet schema-inference error despite the data file clearly existing. Root cause: the streaming job ran on Windows outside Docker, so Spark's internal `_spark_metadata` log stored absolute Windows paths (`file:///C:/Users/...`) that the Linux-based Airflow container couldn't resolve — deleting the stale metadata folder fixed it.

That unblocked the aggregation task, but exposed a second issue: `run_data_quality_check` kept failing with "gold table is empty," which traced back to Structured Streaming's watermark semantics — a 5-minute window with a 10-minute watermark never emitted since test events were sent in short bursts rather than over real elapsed time. Letting the producer and streaming job run continuously for 15–20 minutes let the watermark advance naturally, closing the window and producing real output.

Together, this was a good lesson in separating infrastructure/environment bugs from actual streaming-semantics bugs, even when both surface as the same "no data" symptom.

-----------------------------------------------------------------------

## Lessons Learned

1. **PySpark requires Java 17 or 21** — Java 24 caused a `JAVA_GATEWAY_EXITED` error. Had to install Java 17 alongside the existing version and pin it via environment variables.

2. **Windows Store Python stub intercepted Spark worker processes** — fixed by explicitly setting `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` environment variables to the venv's Python executable.

3. **Kafka producer `KafkaTimeoutError`** — Kafka container wasn't running yet. Confirmed via `docker ps`, started it with `docker-compose up -d` before retrying.

4. **Windows `HADOOP_HOME` unset error** — fixed by downloading `winutils.exe` and `hadoop.dll` and setting `HADOOP_HOME=C:\hadoop`.

5. **Spark bug SPARK-53042** — random `NullPointerException` on Windows + Java 17 during BlockManager registration. Fixed by explicitly setting `spark.driver.bindAddress` and `spark.driver.host` to `127.0.0.1`.

6. **OneDrive file-locking** — project folder inside OneDrive caused intermittent file-locking issues when deleting Spark checkpoint files during testing. Worth keeping build folders outside synced directories in future projects.

7. **Stale `_spark_metadata` with absolute Windows paths** — Spark's streaming commit log recorded `file:///C:/Users/...` paths since the writer ran on Windows outside Docker. The Airflow Linux container couldn't resolve that path, so it saw zero committed files despite the real Parquet file existing. Fix: delete `_spark_metadata` before batch reads.

8. **Windowed aggregations don't emit until the watermark passes window-end** — with `outputMode("append")`, a 5-minute window + 10-minute watermark never closed because test events were sent in short bursts, not spread over real elapsed time. Fix: ran producer + streaming job together continuously for 15–20 minutes.

9. **`startingOffsets: earliest` reprocesses full Kafka topic history on every restart** — explains why aggregated windows spanned multiple days despite short individual test runs. Worth switching to `latest` post-debugging.

10. **Airflow `depends_on` cannot live inside a shared YAML anchor** — had to add it individually per service in `docker-compose-airflow.yml`.

11. **Airflow init `psycopg2.OperationalError`** — fixed by starting postgres alone first, waiting ~20 seconds, then running `airflow-init`.

12. **`delta-spark` pip conflict with `pyspark==3.5.0`** — dropped Delta Lake in favour of plain Parquet for the Airflow container image to avoid dependency conflicts.

13. **GX + Python 3.14 incompatibility** — Great Expectations has no pre-built wheels for Python 3.14 dependencies (numpy, pandas, pyarrow). Solved by creating a separate Python 3.13 venv (`venv_gx`) exclusively for GX work.

14. **GX `DataContextError: datasource already exists`** — running the GX script twice with `mode="file"` tried to recreate already-saved objects. Fixed by switching to `mode="ephemeral"` so nothing is persisted to disk and the script is safe to run multiple times.

15. **GX `add_batch_definition_whole_directory` not available on `ParquetAsset`** — method name varies by GX version. For multiple Parquet partition files (as Spark writes), the correct approach is to read the whole folder via `pandas.read_parquet()` and pass the resulting DataFrame to a `add_pandas` datasource with `add_batch_definition_whole_dataframe`.

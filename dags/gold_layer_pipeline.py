from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_gold_aggregation():
    import os
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("GoldAggregation").master("local[*]").getOrCreate()

    silver = spark.read.parquet("/opt/airflow/data/silver/category_window_agg")

    gold = (
        silver.groupBy("category")
        .sum("event_count")
        .withColumnRenamed("sum(event_count)", "total_events")
    )

    gold.write.mode("overwrite").parquet("/opt/airflow/data/gold/category_summary")

    print(f"Gold aggregation complete. Rows written: {gold.count()}")
    spark.stop()


def run_simple_quality_check():
    import os
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("GoldQualityCheck").master("local[*]").getOrCreate()

    gold = spark.read.parquet("/opt/airflow/data/gold/category_summary")

    row_count = gold.count()
    null_categories = gold.filter(gold.category.isNull()).count()

    if row_count == 0:
        raise ValueError("Data quality check FAILED: gold table is empty.")
    if null_categories > 0:
        raise ValueError(f"Data quality check FAILED: {null_categories} rows have a null category.")

    print(f"Data quality check PASSED. {row_count} rows, 0 null categories.")
    spark.stop()


with DAG(
    dag_id="gold_layer_pipeline",
    default_args=default_args,
    description="Aggregates silver-layer clickstream data into a gold-layer summary, with a quality check",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["clickstream", "gold-layer"],
) as dag:

    aggregate_task = PythonOperator(
        task_id="run_gold_aggregation",
        python_callable=run_gold_aggregation,
    )

    quality_check_task = PythonOperator(
        task_id="run_data_quality_check",
        python_callable=run_simple_quality_check,
    )

    aggregate_task >> quality_check_task
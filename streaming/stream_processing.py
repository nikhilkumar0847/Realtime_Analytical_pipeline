import os

# Same fix we used before — ensures Spark's internal worker processes use
# this venv's Python instead of Windows' Store-redirect stub

# environment setup for the locals abd their work 

os.environ["PYSPARK_PYTHON"] = r"C:\Users\nikhi\OneDrive\Desktop\project\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\nikhi\OneDrive\Desktop\project\venv\Scripts\python.exe"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]   # add the both into the single environment varaible


# * Without of Hadoop environment
# os.environ["PYSPARK_PYTHON"] = "python3"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ... rest of your existing code stays the same
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType   # contain all the data_types for the storation of the data



# --- Build the Spark session ---
# We're using plain Parquet output instead of Delta Lake for now (simpler, easier, no larger and
# no extra package dependency) — same medallion architecture concept,
# just without Delta's transaction log / ACID features. Worth mentioning
# this trade-off explicitly if asked in an interview.

spark = (
    SparkSession.builder
    .appName("ClickstreamStreamProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.shuffle.partitions","2")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --- Define the schema of incoming JSON events ---
# Kafka messages arrive as raw bytes; we need to tell Spark exactly
# what fields to expect and their types, so it can parse the JSON correctly
# Storing the information of the data and 

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
])

# --- Read the raw stream from Kafka ---

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")  # server for the working for the windows and for the local
    #.option("kafka.bootstrap.servers", "kafka:29092")  # changing the port from the locals to kafka for the private containerization inside of the docker
    
    .option("subscribe", "clickstream-events")
    .option("startingOffsets", "earliest")   # read from the beginning of the topic
    .load()
)

# Kafka gives us raw key/value bytes — we cast value to a string,
# then parse that JSON string into structured columns using our schema

parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data"))
    .select("data.*")
)

# --- BRONZE LAYER: The raw parsed events as-is, no transformation ---
# This preserves the original data exactly as it arrived — the "source of truth"

bronze_query = (
    parsed.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "./data/bronze/events")
    .option("checkpointLocation", "./checkpoints/bronze")  # checking the bronze  layer for the windows and that local host can work easily out it of
    
    #  * container portion for the private working use(commneted out for the windows)
    # .option("path", "/opt/airflow/data/bronze/events")
    # .option("checkpointLocation", "/opt/airflow/checkpoints/bronze")
    .start()
)

# --- SILVER LAYER: windowed aggregation with watermarking ---
# Groups events into 5-minute windows, per category, and counts them

windowed_agg = (
    parsed
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("category"),
    )
    .agg(
        count("*").alias("event_count"),
        avg("price").alias("avg_price"),
    )
)

silver_query = (
    windowed_agg.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "./data/silver/category_window_agg")
    .option("checkpointLocation", "./checkpoints/silver")

    # * Commeneted out for the windows and the local use
    # .option("path", "/opt/airflow/data/silver/category_window_agg")
    # .option("checkpointLocation", "/opt/airflow/checkpoints/silver")
    .start()
)

print("Streaming job started. Writing to ./data/bronze and ./data/silver. Press Ctrl+C to stop.")



# print("Streaming job started. Writing to /opt/airflow/data/bronze and /opt/airflow/data/silver. Press Ctrl+C to stop.") # Both of the data working for the airflow (streaming-job)

# Keeps the script alive, processing continuously until you stop it

spark.streams.awaitAnyTermination() 
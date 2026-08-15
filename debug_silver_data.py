import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('debug').master('local[*]').getOrCreate()
df = spark.read.parquet('/opt/airflow/data/silver/category_window_agg')
print('SILVER ROW COUNT:', df.count())
df.show(20, truncate=False)
spark.stop()
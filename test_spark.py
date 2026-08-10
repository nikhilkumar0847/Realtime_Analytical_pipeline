import os

#  Here it will read the path with the help of os module/
os.environ["PYSPARK_PYTHON"] = r"C:\Users\nikhi\OneDrive\Desktop\project\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\nikhi\OneDrive\Desktop\project\venv\Scripts\python.exe"

# Now importation of the sparksession from the pyspark.sql
from pyspark.sql import SparkSession

# Creation of the data_frame and inside of the data_frame just take to show the example as id and the value

# Sparsession helps to create the data_frame and give all the data in the forms of tables.

# Builder helps to construct inside of the sparksession
spark = SparkSession.builder.appName("TestSpark").master("local[*]").getOrCreate()
df = spark.createDataFrame([(1, "Test"), (2, "Exams")], ["id", "value"])
df.show()
spark.stop()

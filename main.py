from pyspark.sql import SparkSession, functions as F
from pyspark.conf import SparkConf

conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

appName = "Write Cyclist Data to Kafka "
master = "local"

spark = SparkSession.builder \
  .master(master) \
  .appName(appName) \
  .config(conf=conf) \
  .getOrCreate()

# Kafka parameters
kafka_servers = "localhost:9092"
topic_name = "test"

# Read CSV file 
csv_path = "cyclist.csv"

try:
  df = spark.read.format("csv").option("header", True).load(csv_path)

  # Handle empty CSV case
  if df.isEmpty():
    print("CSV file is empty!")
    spark.stop()
    exit(1)

  # Flatten all columns into a single array
  df = df.select(F.to_json(F.struct(*df.columns)).alias("value"))

  # Write data to Kafka
  df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("topic", topic_name) \
    .save()
  print("Successfully wrote data to Kafka")

except Exception as e:
  print(f"Error writing data: {e}")

spark.stop()

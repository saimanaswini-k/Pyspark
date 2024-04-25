from pyspark.sql import SparkSession, functions as F
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

appName = "Write Cyclist Data to Kafka"
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
  df = df.select(F.to_json(F.struct(*df.columns)).alias("value"))
except:  # Catch any errors during reading with header
  pass

if not df:
 try:
   selected_columns=["member_casual","rideable_type","day_of_week","month","ride_length"]
   schema=StructType([
   StructField("member_casual",StringType(),True),
   StructField("rideable_type",StringType(),True),
   StructField("day_of_week",StringType(),True),
   StructField("month",StringType(),True),
   StructField("ride_length",StringType(),True),
  ])
   df = spark.read.format("csv").option("header", True).schema(schema).load(csv_path)
   df = df.select(*selected_columns)
   
 except:  # Catch any errors during reading with schema
    pass
 
if df:
  print("Successfully read the data")
  df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("topic", topic_name) \
    .save()
else:
  print("Error reading the data ")

spark.stop()

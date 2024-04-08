from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Streaming").getOrCreate()

# Kafka Source Configuration
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot-meter") \
    .option("startingOffsets", "earliest") \
    .load()

# Define a Unified Schema
schema = StructType([
    StructField("device_id", StringType()),  # Assuming string for meter IDs
    StructField("timestamp", StringType()),
    StructField("usage", IntegerType()),
    StructField("usage_kwh", IntegerType()),
    StructField("flow_rate", DoubleType())  
])

# Parse JSON and Filter
df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

filtered_df = df.filter(
    (col("data.flow_rate") == -1) | (col("data.usage_kwh") == -1)
)
# Prepare for Kafka (Assuming you only want to output the invalid records)
output_df = filtered_df.selectExpr("to_json(struct(*)) AS value") 
# Kafka Sink Configuration (same as before)
query = output_df.writeStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("topic", "iot-abnormal")  \
          .option("checkpointLocation", "/Users/tranan/Desktop/HCMUT/BigData/iot-bigdata/test") \
          .outputMode("update") \
          .start()
query.awaitTermination()

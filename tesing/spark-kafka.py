from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

# Kafka Source Configuration
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "electricity-usage") \
    .option("startingOffsets", "earliest") \
    .load()

# Define Schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("usage", IntegerType()),
    StructField("timestamp", StringType())
])

# Parse JSON, Calculate Average and Count
df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
       .select("data.usage") \
       .agg(avg("usage").alias("avg_usage"), count("*").alias("total_count")) 

# Kafka Sink Configuration
output_df = df.selectExpr("to_json(struct(*)) AS value")  # Prepare output format

query = output_df.writeStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("topic", "avg_usage_results")  \
          .option("checkpointLocation", "/Users/tranan/Desktop/HCMUT/BigData/iot-bigdata/test") \
          .outputMode("update") \
          .start()

query.awaitTermination()

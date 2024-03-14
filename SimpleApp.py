from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import *

logFile = "./requirement.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# logData = spark.read.text(logFile).cache()

# numAs = logData.filter(logData.value.contains('a')).count()
# numBs = logData.filter(logData.value.contains('b')).count()

# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

# spark.stop()

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

# Output to Console (for demonstration)
query = df.writeStream \
          .outputMode("complete") \
          .format("console") \
          .start()

query.awaitTermination()
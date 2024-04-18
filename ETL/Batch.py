from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, current_date, to_date,lit
import datetime
from datetime import date

appName = "iot-batch"
master = "spark://Trans-MacBook-Pro.local:7077"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .config("spark.mongodb.input.uri", "mongodb://admin:password@localhost:27017/iot.iot-meter?authSource=admin") \
    .config("spark.mongodb.output.uri", "mongodb://admin:password@localhost:27017/iot.iot-meter?authSource=admin") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

# Read data from MongoDB
df = spark.read.format('mongo').load()
# df.printSchema()
df.show()
df = df.withColumn("meter_type", when(col("usage_kwh").isNotNull(), "electricity") \
                   .otherwise("water"))

# Filter for the current date
today = datetime.date.today()
# today = date(2024, 4, 17)
df_filtered = df.filter(to_date("timestamp") == today)

# Calculate average usage per meter type for the current date
df_averages = df_filtered.groupBy("meter_type").agg(
    avg(col("usage").cast("double")).alias("avg_usage"),
    avg(col("usage_kwh").cast("double")).alias("avg_usage_kwh"),
    current_date().alias("calculation_date")
    # lit(today).cast("date").alias("calculation_date") 
)

# Save the result DataFrame back to MongoDB 
df_averages.write.format("mongo") \
    .mode("append") \
    .option("database", "iot") \
    .option("collection", "daily_averages") \
    .save()

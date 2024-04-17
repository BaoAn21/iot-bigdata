from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, current_date

appName = "PySpark MongoDB Examples"
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

# Calculate average usage per meter type
# Calculate average usage per meter type and add current date
df_averages = df.groupBy("meter_type").agg(
    avg(col("usage").cast("double")).alias("avg_usage"),
    avg(col("usage_kwh").cast("double")).alias("avg_usage_kwh"),
    current_date().alias("calculation_date")
)

# Save the result DataFrame back to MongoDB 
df_averages.write.format("mongo") \
    .mode("append") \
    .option("database", "iot") \
    .option("collection", "daily_averages") \
    .save()

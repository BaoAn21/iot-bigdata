from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# MongoDB Configuration
client = MongoClient("mongodb://admin:password@localhost:27017")  # Adjust if your MongoDB runs differently
db = client['iot']  # Replace 'mydatabase' with your database name
collection = db['iot-meter']

# Kafka Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'iot-meter'  # Replace with your actual topic name

# Create a Kafka Consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)

# Consume messages from the topic
for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    print(data)
    collection.insert_one(data)

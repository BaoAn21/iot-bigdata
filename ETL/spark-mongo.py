
# Take data from spark to mongo
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# MongoDB Configuration
client = MongoClient("mongodb://admin:password@localhost:27017")  # Adjust if your MongoDB runs differently
db = client['iot'] 
collection = db['iot-abnormal']

# Kafka Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'iot-abnormal' 

# Create a Kafka Consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)

# Consume messages from the topic
for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    print(data['data'])
    collection.insert_one(data['data'])

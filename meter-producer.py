import random
from datetime import datetime, timedelta
from faker import Faker 
import time
from kafka import KafkaProducer
import json

fake = Faker() 

# Kafka Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'iot-meter'  # Name of your Kafka topic
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Meter IDs
electricity_meters = ["em1", "em2", "em3", "em4"]
water_meters = ["wm1", "wm2", "wm3", "wm4"]

def generate_electricity_data(meter_id):
    usage_kwh = fake_number = fake.random_int(min=-1, max=10)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") # Adjusted for current sim time
    return {
        "device_id": meter_id,
        "timestamp": timestamp,
        "usage_kwh": usage_kwh
    }

def generate_water_data(meter_id):
    usage = random.randint(5, 20)
    flow_rate = fake_number = fake.random_int(min=-1, max=10)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") # Adjusted for current sim time
    return {
        "device_id": meter_id,
        "timestamp": timestamp,
        "usage": usage,
        "flow_rate": flow_rate,
    }

while(True):
  for em in electricity_meters:
    data = generate_electricity_data(em)
    message = json.dumps(data).encode('utf-8')
    producer.send(topic_name, message)
    print(message)
  for wm in water_meters:
    data = generate_water_data(wm)
    message = json.dumps(data).encode('utf-8')
    producer.send(topic_name, message)
    print(message)
  time.sleep(1)
import time
from faker import Faker
from kafka import KafkaProducer

fake = Faker() 

def generate_electricity_usage(id):
    """Generates a fake electricity usage reading."""
    usage_kwh = fake.random_int(min=50, max=500)  # Random usage between 50-500 kWh
    timestamp = fake.date_time_this_month().isoformat()  # Current month timestamp
    return {
        "id":id,
        "usage": usage_kwh,
        "timestamp": timestamp
    }

# Kafka Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'electricity-usage'  # Name of your Kafka topic
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


if __name__ == "__main__":
    id = 1
    while True:
        for _ in range(1):  # Generate 5 data points
            data = generate_electricity_usage(id)
            id += 1
            message = str(data).encode('utf-8')  # Convert data to bytes
            producer.send(topic_name, message)
            print(data)
        time.sleep(1)  # Pause for 1 second

from kafka import KafkaConsumer

# Kafka Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'electricity-usage'  # Replace with your actual topic name

# Create a Kafka Consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)

# Consume messages from the topic
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")

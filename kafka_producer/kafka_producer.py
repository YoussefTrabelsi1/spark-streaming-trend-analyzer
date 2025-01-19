import json
import time
from kafka import KafkaProducer

# Kafka configuration
KAFKA_TOPIC = 'test-topic'
KAFKA_BROKER = 'localhost:9092'  # Update if running Kafka on a different host/port

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_data():
    # Load sample data from a JSON file
    with open('../data/sample_data.json', 'r') as file:
        data = json.load(file)

    for record in data:
        # Send each record to the Kafka topic
        producer.send(KAFKA_TOPIC, record)
        print(f"Sent: {record}")
        time.sleep(1)  # Simulate a delay between messages

if __name__ == "__main__":
    produce_data()

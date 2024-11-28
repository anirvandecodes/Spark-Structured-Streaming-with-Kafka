from confluent_kafka import Producer
import json
import time
import os
import random

# Kafka configuration
config = {
    'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
    'client.id': 'shopping-transaction-producer',
}

# Initialize the Kafka producer
producer = Producer(config)

# Define the topic to send data to
topic = 'shopping_transaction'


def delivery_report(err, msg):
    """Callback for delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def generate_transaction():
    """Generate a random shopping transaction."""
    items = ["laptop", "smartphone"]
    return {
        "timestamp": time.time(),
        "user_id": random.randint(1, 10),
        "item": random.choice(items),
        "quantity": random.randint(1, 3)
    }


print("Starting producer. Press Ctrl+C to stop.")
try:
    while True:
        # Generate a transaction
        transaction = generate_transaction()

        # Serialize the transaction as JSON
        serialized_value = json.dumps(transaction).encode('utf-8')

        # Send the message to Kafka
        producer.produce(
            topic,
            value=serialized_value,
            on_delivery=delivery_report
        )

        # Flush the message queue
        producer.poll(0)
        time.sleep(1)  # Send a transaction every second
except KeyboardInterrupt:
    print("\nProducer stopped.")
finally:
    # Ensure all messages are delivered
    producer.flush()

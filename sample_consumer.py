from confluent_kafka import Consumer, KafkaException
import os

# Define example configurations for the Kafka consumer
config = {
    'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
    'client.id': 'transaction-consumer',
    'group.id': 'example_consumer_group',
    'auto.offset.reset': 'earliest',  # Start at the earliest message if no offset is present
    'enable.auto.commit': False,      # Disable auto commit for more control
    'fetch.min.bytes': 1024,          # Fetch minimum 1 KB at a time
}

# Initialize the Kafka consumer
consumer = Consumer(config)

# Set the topic to subscribe to
topic = 'my_first_topic'

# Subscribe to the topic
consumer.subscribe([topic])

try:
    print(f"Subscribed to topic: {topic}")
    # Start consuming messages
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with a 1 second timeout
        if msg is None:
            continue  # No message received in the last poll cycle
        if msg.error():
            # Handle any errors, including end-of-partition (EOF)
            raise KafkaException(msg.error())

        # Process the message (print the key and value)
        print(f"Received message: key={msg.key()}, value={msg.value()}")

        # Commit the offset after processing each message
        consumer.commit(msg)

except KafkaException as e:
    # Error handling
    print(f"Kafka error occurred: {e}")

except KeyboardInterrupt:
    # Gracefully handle shutdown on interrupt
    print("Consumer interrupted by user")

finally:
    # Ensure consumer is properly closed
    consumer.close()
    print("Consumer closed")

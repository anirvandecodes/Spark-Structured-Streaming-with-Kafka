# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Structured Streaming Trigger Types

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType


# Kafka configuration
kafka_bootstrap_servers = os.getenv("KAFKA_BROKER")
kafka_topic = 'shopping_transaction '

# Kafka Consumer settings for Confluent Cloud
kafka_config = {
    'kafka.bootstrap.servers': kafka_bootstrap_servers,
    'subscribe': kafka_topic,
    'startingOffsets': 'earliest',  # To start from the earliest message
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    "failOnDataLoss" : "false",
    "kafka.ssl.endpoint.identification.algorithm" :  "https",
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_API_KEY")}" password="{os.getenv("KAFKA_API_SECRET")}";',
    "startingOffsets" :"earliest"
}


schema = "timestamp DOUBLE, user_id INT, item STRING, quantity INT"


# Read data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_config) \
    .load()

# Parse JSON data
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

display(parsed_stream)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Trigger Types in Spark Structured Streaming
# MAGIC
# MAGIC In Apache Spark Structured Streaming, you can control how often the streaming query's output operations are triggered using **trigger types**. Below are the available trigger types:
# MAGIC
# MAGIC ### 1. `Default (Unspecified)`
# MAGIC By default, Spark uses a *micro-batch* execution model where it continuously processes the input data as it arrives. No explicit trigger is set. It processes data as soon as it is ready.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

query = parsed_stream.writeStream \
    .format("console") \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. `Trigger.Once`
# MAGIC The query processes all the available data and then stops. This is useful for scenarios where you want to process data only once, such as in one-time jobs.

# COMMAND ----------

query = parsed_stream.writeStream \
    .format("console") \
    .trigger(once=True) \
    .start()

# COMMAND ----------

query = parsed_stream.writeStream \
    .format("console") \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. `Trigger.ProcessingTime (Fixed interval micro-batching)`
# MAGIC Specifies that the query should run at fixed intervals (e.g., every 5 seconds). This controls the latency of the query.

# COMMAND ----------

from pyspark.sql.streaming import Trigger

query = parsed_stream.writeStream \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()


    10:00 100

    10:05 0

    10:10


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4. `Trigger.Continuous (Experimental - Continuous Processing)`
# MAGIC Specifies a continuous processing mode with the minimum delay between data processing. This mode is designed for low-latency, real-time use cases. Note: This mode is experimental and might not support all sinks or transformations.

# COMMAND ----------

query = parsed_stream.writeStream \
    .format("console") \
    .trigger(continuous='1 second') \
    .start()

# COMMAND ----------

# KAFKA PRODUCER CODE


# from confluent_kafka import Producer
# import json
# import time
# import os
# import random

# # Kafka configuration
# config = {
#     'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
#     'security.protocol': 'SASL_SSL',
#     'sasl.mechanisms': 'PLAIN',
#     'sasl.username': os.getenv("KAFKA_API_KEY"),
#     'sasl.password': os.getenv("KAFKA_API_SECRET"),
#     'client.id': 'shopping-transaction-producer',
# }

# # Initialize the Kafka producer
# producer = Producer(config)

# # Define the topic to send data to
# topic = 'shopping_transaction'


# def delivery_report(err, msg):
#     """Callback for delivery reports."""
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# def generate_transaction():
#     """Generate a random shopping transaction."""
#     items = ["laptop", "smartphone", "tablet", "headphones", "smartwatch"]
#     return {
#         "timestamp": time.time(),
#         "user_id": random.randint(1, 10),
#         "item": random.choice(items),
#         "quantity": random.randint(1, 3)
#     }


# print("Starting producer. Press Ctrl+C to stop.")
# try:
#     while True:
#         # Generate a transaction
#         transaction = generate_transaction()

#         # Serialize the transaction as JSON
#         serialized_value = json.dumps(transaction).encode('utf-8')

#         # Send the message to Kafka
#         producer.produce(
#             topic,
#             value=serialized_value,
#             on_delivery=delivery_report
#         )

#         # Flush the message queue
#         producer.poll(0)
#         time.sleep(1)  # Send a transaction every second
# except KeyboardInterrupt:
#     print("\nProducer stopped.")
# finally:
#     # Ensure all messages are delivered
#     producer.flush()


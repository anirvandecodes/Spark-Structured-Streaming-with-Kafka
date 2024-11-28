# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Structured Streaming Output Mode

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, from_unixtime
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
     

# Transformation: Count the number of times each item is added
item_counts = parsed_stream.groupBy("item").count()


display(item_counts)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Complete Mode
# MAGIC
# MAGIC - **Description:** Outputs the entire result table at every trigger, including all rows (whether updated or not).
# MAGIC - **When to Use:** Use it when you want to see the entire state of the aggregation at every trigger.
# MAGIC

# COMMAND ----------

# Complete Mode
complete_query = item_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="3 seconds") \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Update Mode
# MAGIC
# MAGIC - **Description:** Outputs only updated rows in the result table since the last trigger. Rows that haven’t changed since the last trigger are not emitted.
# MAGIC - **When to Use:** Use it when intermediate updates are needed for aggregation queries without finalizing all data.
# MAGIC

# COMMAND ----------

smartphone 20

smartphone 20

# COMMAND ----------

# Update Mode
update_query = item_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="3 seconds") \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Append Mode
# MAGIC
# MAGIC - **Description:** Outputs only new rows added to the result table since the last trigger.
# MAGIC - **When to Use:** Use it when results are finalized, meaning the data for a specific group/window is complete (e.g., after a watermark for late data).
# MAGIC - **Key Point: Append mode emits a row only once when it is finalized**
# MAGIC

# COMMAND ----------

# Append Mode without aggregates
append_query = parsed_stream.writeStream \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .format("console") \
    .start()

# COMMAND ----------

# Append Mode with aggregates
append_query = item_counts.writeStream \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .format("console") \
    .start()

# COMMAND ----------

10:00 

# COMMAND ----------

employee name
1       a
2       b
3       c




# COMMAND ----------

from pyspark.sql.functions import window,from_unixtime

# COMMAND ----------

# Convert the 'timestamp' column from Double to TimestampType
parsed_stream = parsed_stream.withColumn("timestamp", from_unixtime("timestamp").cast("timestamp"))


# COMMAND ----------

item_counts = parsed_stream.withWatermark("timestamp", "10 seconds") \
    .groupBy(window("timestamp", "10 seconds"),"item") \
    .count()
item_counts = item_counts.select("window.start","window.end","item","count")

# Define the checkpoint location
checkpoint_path = "/mnt/checkpoints/shopping_transactions"



# Append Mode with aggregates (with watermark)
append_query = item_counts.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="5 seconds") \
    .format("console") \
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC | **Feature**           | **Append Mode**                                      | **Update Mode**                                       | **Complete Mode**                                   |
# MAGIC |------------------------|-----------------------------------------------------|-----------------------------------------------------|---------------------------------------------------|
# MAGIC | **Output**             | Only new rows that are finalized.                   | Only rows that are updated since the last trigger.   | Entire result table, including all rows.          |
# MAGIC | **When to Use**        | When final results are ready (e.g., after watermark).| When incremental updates are acceptable.            | When the entire aggregation state is needed.      |
# MAGIC | **Supported Queries**  | Simple queries, aggregations with watermarks.       | Aggregations and stateful operations.               | Aggregations and stateful operations.             |
# MAGIC | **Example Use Case**   | Output finalized sales numbers for a time window.   | Show intermediate results of word counts.           | Display the complete state of user activity.      |
# MAGIC

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


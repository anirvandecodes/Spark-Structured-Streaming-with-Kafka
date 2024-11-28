# Databricks notebook source
# MAGIC %md
# MAGIC ### Streaming Aggregates in Spark: Tumbling vs Sliding Windows with Kafka 

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, from_unixtime, countDistinct
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
    "startingOffsets" :"latest"
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

# Convert the 'timestamp' column from Double to TimestampType
parsed_stream = parsed_stream.withColumn("timestamp", from_unixtime("timestamp").cast("timestamp"))    

#display(parsed_stream)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Use Case: Monitoring Real-Time Sensor Data in a Smart Factory
# MAGIC
# MAGIC ## Scenario: 
# MAGIC A smart factory uses IoT sensors on its machinery to monitor performance metrics such as **temperature**, **vibration**, and **energy consumption**. The factory needs to aggregate sensor data in fixed intervals to identify anomalies and optimize efficiency.
# MAGIC
# MAGIC Compute the following metrics
# MAGIC    - **Average temperature:** `avg(temperature)`
# MAGIC    - **Average vibration:** `avg(vibration)`
# MAGIC    - **Average energy consumption:** `avg(energy_consumption)`
# MAGIC

# COMMAND ----------

# MAGIC %md ### Tumbling Window Aggregation
# MAGIC
# MAGIC A tumbling window is a fixed-size, non-overlapping window. It is useful for aggregating data within a time period, such as counting the number of items sold in each hour.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://github.com/user-attachments/assets/e6475a70-284a-48da-a8f7-fbab3c926fb8)

# COMMAND ----------

agg_df_tumbling = (
    parsed_stream.groupBy(
        window("timestamp", "20 seconds"),  # Tumbling window of 1 hour
        "item"
    )
    .agg(
       count("item").alias("item_count")
    )
)

# Just to show the window start and end time
agg_df_tumbling = agg_df_tumbling.select("window.start","window.end","item","item_count")

# Write the result to a Delta table or console for visualization
query_tumbling = (
    agg_df_tumbling.writeStream
    .outputMode("complete") 
    .format("console")
    .option("checkpointLocation", "/mnt/checkpoints/tumbling_window_test")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sliding window 
# MAGIC It aggregates data over a moving time window. For example, you can calculate the sum of the quantity of items sold in the past 15 minutes, but the window slides as time progresses.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ![](https://github.com/user-attachments/assets/bb2b8a53-34c2-4cb4-bc75-f91c600e043d)
# MAGIC

# COMMAND ----------

agg_df_sliding = (
    parsed_stream.groupBy(
        window("timestamp", "20 seconds","10 seconds"), 
        "item"
    )
    .agg(
       count("item").alias("item_count")
    )
)


agg_df_sliding = agg_df_sliding.select("window.start","window.end","item","item_count")

# Write the result to a Delta table or console for visualization
query_sliding = (
    agg_df_sliding.writeStream
    .outputMode("complete") 
    .format("console")
    .option("checkpointLocation", "/mnt/checkpoints/sliding_window_test")
    .start()
)

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


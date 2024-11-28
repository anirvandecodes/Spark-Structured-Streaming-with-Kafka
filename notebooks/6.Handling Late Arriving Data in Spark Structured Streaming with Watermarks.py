# Databricks notebook source
# MAGIC %md
# MAGIC ### Handling Late Arriving Data in Spark Structured Streaming with Watermarks

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

parsed_stream = parsed_stream.withColumn("timestamp", from_unixtime("timestamp").cast("timestamp"))    



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Handling Late Data (Event-Time Processing)
# MAGIC When working with event-time data, it’s common to deal with late-arriving data. In Spark, you can handle late data by using watermarks.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Causes of Late-Arriving Data in Kafka
# MAGIC
# MAGIC 1. **Network and Producer Delays**: High network latency, resource contention, or retries in the producer can delay message delivery.  
# MAGIC 2. **Broker Overload**: Overloaded Kafka brokers or slow replication can introduce processing delays.  
# MAGIC 3. **Upstream Delays**: Latency in upstream systems or IoT devices can make events arrive late in Kafka.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event Time vs Processing Time in Structured Streaming
# MAGIC
# MAGIC #### Time Ranges:
# MAGIC - **Start:** `2024-11-22T08:52:00.000+00:00`
# MAGIC - **End:** `2024-11-22T09:52:20.000+00:00`
# MAGIC
# MAGIC #### Example Kafka Message:
# MAGIC ```json
# MAGIC {
# MAGIC     "timestamp": "2024-11-22T08:52:10.000+00:00",
# MAGIC     "user_id": "123",
# MAGIC     "item": "headphones",
# MAGIC     "quantity": 1
# MAGIC }
# MAGIC ```
# MAGIC - **Processing Time:** The message was actually read from Kafka Structured Streaming at: `2024-11-22T10:00:10.000+00:00`
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Understanding the State Store in Spark Structured Streaming
# MAGIC
# MAGIC ### 1. What is a State Store?
# MAGIC
# MAGIC The **state store** is a **key-value store** used by Spark to persist and manage the state for each micro-batch in a streaming query. This state is updated with each batch and saved for future use.
# MAGIC
# MAGIC #### Example Use Case:
# MAGIC - In a **windowed aggregation**, the state store keeps track of partial results for each window until the window closes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. How It Works
# MAGIC
# MAGIC Each streaming query operates in **micro-batches**, following these steps:
# MAGIC
# MAGIC 1. **Input Data**: Data is read and processed.
# MAGIC 2. **Query Existing State**: The state store is queried for existing state.
# MAGIC 3. **State Update**: The state is updated based on the new data.
# MAGIC 4. **Output Results**: Results are written to the output sink.
# MAGIC 5. **Persist State**: The updated state is saved for use in subsequent micro-batches.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Automatic State Cleanup:
# MAGIC - Spark automatically removes **old state** based on the **watermark**, which defines when data is considered late and no longer affects the state.
# MAGIC

# COMMAND ----------


provider_class = spark.conf.get("spark.sql.streaming.stateStore.providerClass")
print(f"State Store Provider: {provider_class}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Background: What is RocksDB in Spark Structured Streaming?
# MAGIC
# MAGIC RocksDB is an embedded key-value store designed for **high-performance reads and writes**. 
# MAGIC
# MAGIC In the context of **Spark Structured Streaming**, it serves as a powerful alternative to the default file-based state store. By leveraging RocksDB, Spark can significantly boost the performance of stateful computations like **aggregations** and **joins**, particularly under heavy workloads. 
# MAGIC
# MAGIC This is achieved by minimizing disk I/O overhead, making RocksDB an excellent choice for handling large states or high-throughput streaming queries.
# MAGIC
# MAGIC

# COMMAND ----------

spark.conf.set("spark.sql.streaming.stateStore.providerClass","org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

from pyspark.sql.functions import window, count

# Add watermark and perform windowed aggregation
agg_df_tumbling = (
    parsed_stream
    .withWatermark("timestamp", "30 seconds")  # Add watermark for late data handling
    .groupBy(
        window("timestamp", "20 seconds"),  # Group data into 20-second windows
        "item"
    )
    .agg(
        count("item").alias("item_count")  # Aggregate the count of items
    )
)

# Select specific columns for output
agg_df_tumbling = agg_df_tumbling.select(
    "window.start",
    "window.end",
    "item",
    "item_count"
)

# Write to memory with checkpointing for fault tolerance
query = agg_df_tumbling.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("tumbling_window_query") \
    .option("checkpointLocation", "/mnt/checkpoints/late_arriving_data_demo") \
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


# COMMAND ----------

# MAGIC %md
# MAGIC

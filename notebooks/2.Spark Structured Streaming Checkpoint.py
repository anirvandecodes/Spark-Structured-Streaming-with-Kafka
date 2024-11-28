# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Structured Streaming Checkpoint

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType


# Kafka configuration
kafka_bootstrap_servers = os.getenv("KAFKA_BROKER")
kafka_topic = 'shopping_transaction'

# Kafka Consumer settings for Confluent Cloud
kafka_config = {
    'kafka.bootstrap.servers': kafka_bootstrap_servers,
    'subscribe': kafka_topic,
    'startingOffsets': 'latest',
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
    .format("kafka")  \
    .options(**kafka_config) \
    .load()

# Parse JSON data
# Count the number of rows in the stream
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpointing in Spark Structured Streaming
# MAGIC
# MAGIC Checkpointing in Spark Structured Streaming is essential, especially when working with Kafka, for the following reasons:
# MAGIC
# MAGIC ##### 1. Fault Tolerance
# MAGIC - **Recovery from Failures**: If the streaming application crashes or stops unexpectedly, checkpointing allows Spark to recover and resume processing from where it left off. Without it, data might be lost or reprocessed incorrectly.
# MAGIC
# MAGIC ##### 2. Stateful Operations
# MAGIC - **Maintaining Query State**: Stateful operations like aggregations, joins, and windowing require a state to be maintained over time. The state information is periodically saved in the checkpoint, ensuring it can be restored upon failure.
# MAGIC

# COMMAND ----------

# Define the checkpoint location
checkpoint_path = "/mnt/checkpoints/checkpoint_lecture"


query = .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(once=True) \
    .start()

# COMMAND ----------

dbutils.fs.ls("/mnt/checkpoints/checkpoint_lecture")

# COMMAND ----------

dbutils.fs.head("/mnt/checkpoints/checkpoint_lecture/metadata")


# COMMAND ----------

dbutils.fs.ls("/mnt/checkpoints/checkpoint_lecture/offsets")

# COMMAND ----------

dbutils.fs.head("/mnt/checkpoints/checkpoint_lecture/offsets/0")


# COMMAND ----------

v1
{"batchWatermarkMs":0,"batchTimestampMs":1732298581733,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"shopping_transaction":{"2":707,"1":742,"0":739}}


# COMMAND ----------

# MAGIC %md v1 --- The version of the checkpoint file format.

# COMMAND ----------

# MAGIC %md Metadata (JSON object 1)
# MAGIC

# COMMAND ----------

{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1732298581733,
  "conf": {
    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "spark.sql.streaming.join.stateFormatVersion": "2",
    "spark.sql.streaming.stateStore.compression.codec": "lz4",
    "spark.sql.streaming.stateStore.rocksdb.formatVersion": "5",
    "spark.sql.streaming.statefulOperator.useStrictDistribution": "true",
    "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion": "2",
    "spark.sql.streaming.multipleWatermarkPolicy": "min",
    "spark.sql.streaming.aggregation.stateFormatVersion": "2",
    "spark.sql.shuffle.partitions": "200"
  }
}


# COMMAND ----------

# MAGIC %md Offsets and State Information (JSON object 2)

# COMMAND ----------

{"shopping_transaction":{"2":707,"1":742,"0":739}}

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### State Information or Offsets for the Streaming Query
# MAGIC
# MAGIC #### Key (`shopping_transaction`):
# MAGIC - Likely represents the operation or table being processed in the stream.
# MAGIC
# MAGIC #### Value (`{"2":707,"1":742,"0":739}`):
# MAGIC - A mapping of **Kafka partition IDs** (`2`, `1`, `0`) to their offsets:
# MAGIC   - **Partition 2** has been processed up to offset **707**.
# MAGIC   - **Partition 1** has been processed up to offset **742**.
# MAGIC   - **Partition 0** has been processed up to offset **739**.
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


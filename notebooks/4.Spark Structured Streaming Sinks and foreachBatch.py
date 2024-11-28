# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Structured Streaming Sinks and foreachBatch

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
     

# Transformation: Count the number of times each item is added
item_counts = parsed_stream.groupBy("item").count()

display(item_counts)


# COMMAND ----------

# MAGIC %md
# MAGIC #### List of Sinks in Spark Structured Streaming
# MAGIC
# MAGIC 1. **Console Sink**  
# MAGIC    - Writes streaming query output to the console.
# MAGIC    - Useful for debugging and testing.
# MAGIC
# MAGIC 2. **File Sink**  
# MAGIC    - Writes output to files in formats like JSON, CSV, or Parquet.
# MAGIC    - Suitable for batch-style processing and data archiving.
# MAGIC
# MAGIC 3. **Kafka Sink**  
# MAGIC    - Writes processed data back to Kafka topics.
# MAGIC    - Ideal for building real-time data pipelines.
# MAGIC
# MAGIC 4. **Memory Sink**  
# MAGIC    - Writes the streaming output to an in-memory table.
# MAGIC    - Useful for interactive analysis during development.
# MAGIC
# MAGIC 5. **Delta Sink**  
# MAGIC    - Writes data to a Delta Lake table, enabling ACID transactions and efficient updates.
# MAGIC    - Commonly used in production-grade streaming applications.
# MAGIC
# MAGIC 6. **Foreach Sink**  
# MAGIC    - Allows applying custom logic to handle each row in the stream.
# MAGIC    - Flexible for advanced use cases.
# MAGIC
# MAGIC 7. **JDBC Sink**  
# MAGIC    - Writes output to relational databases using JDBC.
# MAGIC    - Useful for integrating Spark with existing database systems.
# MAGIC
# MAGIC 8. **Custom Sink (ForeachBatch)**  
# MAGIC    - Processes each micro-batch with custom logic.
# MAGIC    - Suitable for integrating with external systems or performing complex transformations.
# MAGIC

# COMMAND ----------

# MAGIC %md ### Memory Sink

# COMMAND ----------

checkpoint_location = "/mnt/checkpoint/memory_sink"

item_counts_memory_query = item_counts.writeStream \
    .format("memory") \
    .queryName("streaming_table") \
    .option("checkpointLocation", checkpoint_location) \
    .outputMode("complete") \
    .start()


# COMMAND ----------

# Query the memory table
display(spark.sql("SELECT * FROM streaming_table"))

# COMMAND ----------

# MAGIC %md ### Kafka Sink

# COMMAND ----------

kafka_df = item_counts.selectExpr(
    "CAST(item AS STRING) AS key",  # Use the item as the Kafka key
    "to_json(struct(item, count)) AS value"  # Serialize item and count as JSON for the value
)


# COMMAND ----------

display(kafka_df)

# COMMAND ----------

# Write the DataFrame to Kafka
checkpoint_location = "/mnt/checkpoint/kafka_sink"
query = kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", "shopping_items_aggregates") \
    .option("checkpointLocation", checkpoint_location) \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_API_KEY")}" password="{os.getenv("KAFKA_API_SECRET")}";')\
    .outputMode("update") \
    .start()

# COMMAND ----------

# MAGIC %md ### Delta Sink

# COMMAND ----------

checkpoint_location = "/mnt/checkpoints/table_sinks"

parsed_query = parsed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable("parsed_table")


# COMMAND ----------

# MAGIC %md
# MAGIC Delta sink update mode does not work

# COMMAND ----------

checkpoint_location = "/mnt/checkpoints/table_sink"

item_counts_query = item_counts.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable("shopping_aggregates_update")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `foreachBatch` Sink in Spark Structured Streaming
# MAGIC
# MAGIC The `foreachBatch` sink allows custom logic to be applied to each micro-batch of data. It's used for:
# MAGIC
# MAGIC 1. **Custom Data Sinks**: Write data to non-supported sinks (e.g., databases, REST APIs).
# MAGIC 2. **Transformations per Batch**: Apply batch-level transformations like deduplication or validation.
# MAGIC 3. **Advanced Processing**: Combine with batch APIs for complex operations like upserts in Delta tables.
# MAGIC
# MAGIC
# MAGIC ### Use Cases of `foreachBatch`
# MAGIC
# MAGIC - Writing to **custom sinks** like databases or external systems.
# MAGIC - Handling **complex updates** or performing **merge operations** with Delta tables.
# MAGIC - Implementing **batch-specific logic** in streaming pipelines.
# MAGIC
# MAGIC

# COMMAND ----------

checkpoint_location = "/mnt/checkpoints/item_counts"

parsed_query = item_counts.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable("item_counts")

# COMMAND ----------

# Write stream with foreachBatch
checkpoint_location = "/mnt/checkpoints/foreach_sink"

query = item_counts.writeStream \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .foreachBatch(merge_to_delta_table) \
    .start()


# COMMAND ----------


from delta.tables import DeltaTable

delta_table_name = "item_counts"

# Define the foreachBatch function
def merge_to_delta_table(batch_df, batch_id):
    delta_table = DeltaTable.forName(spark, delta_table_name)
    # Perform merge
    delta_table.alias("target").merge(
        batch_df.alias("source"),
        "target.item = source.item"  # Merge condition: match on 'item'
    ).whenMatchedUpdate(set={
        "count": "target.count + source.count"  # Update count by adding new values
    }).whenNotMatchedInsert(values={
        "item": "source.item", 
        "count": "source.count"  # Insert new items
    }).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history item_counts

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


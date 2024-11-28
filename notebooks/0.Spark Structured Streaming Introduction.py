# Databricks notebook source
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType


# Kafka configuration
kafka_bootstrap_servers = os.getenv("KAFKA_BROKER")
kafka_topic = 'my_first_topic'

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


# Read the stream from Kafka

# kafka_stream = spark.read \
#     .format("delta") \
#     .options(**options) \
#     .load("s3://some_location")



kafka_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_config) \
    .load()


# Deserialize it
stream_df = kafka_stream.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
     

display(stream_df)


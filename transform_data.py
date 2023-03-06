from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

from kafka import KafkaConsumer, KafkaProducer
import json

#Kafka topic
ACTIVE_TOPIC = 'data_extracts'

def transform_data():
    consumer = KafkaConsumer('data_extracts',
                    bootstrap_servers='localhost:9092',
                    auto_offset_reset='earliest',
                    enable_auto_commit=True)

    spark = SparkSession.builder.appName("DataPlump").getOrCreate()
    schema = StructType([
        StructField("Retailer", StringType(), True),
        StructField("Retailer ID", IntegerType(), True),
        StructField("Invoice Date", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("State", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Price per Unit", StringType(), True),
        StructField("Units Sold", StringType(), True),
        StructField("Total Sales", StringType(), True),
        StructField("Operating Profit", StringType(), True),
        StructField("Operating Margin", StringType(), True),
        StructField("Sales Method", StringType(), True)
])
    df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "data_extracts")\
    .option("startingOffset", "earliest")\
    .load()\
    .select(from_json(col("value").cast("string"), schema).alias("data"))\
    .select("data.*")

transform_data()
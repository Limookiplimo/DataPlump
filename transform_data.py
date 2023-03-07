from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import from_json, col, struct
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, TimestampType

def read_kafka():
    # create SparkSession
    spark = (SparkSession
            .builder
            .master('local')
            .appName('wiki-changes-event-consumer')
            # Add kafka package
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
            .getOrCreate())
    sc = spark.sparkContext

    df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092") # kafka server
        .option("subscribe", "data_extracts") # topic
        .option("startingOffsets", "earliest") # start from beginning 
        .load())
    
    schema = StructType([StructField("key",StringType(),True),
                StructField("value",StringType(),True),
                StructField("topic",StringType(),True),
                StructField("partition",IntegerType(),True),
                StructField("offset",LongType(),True),
                StructField("timestamp",TimestampType(),True),
                StructField("timestampType",IntegerType(),True)])
    
    # Convert binary to string key and value, create schema & DF
    df1 = (df
        .withColumn("key", df["key"].cast(StringType()))\
        .withColumn("value", df["value"].cast(StringType()))
        .withColumn("value", from_json("value", schema)) #schema
        .select('*', struct(col("value")).alias("data"))) #dataframe

    raw_path ="/home/limoo/Documents/projects/DataPlump/data-extracts"
    checkpoint_path = "/home/limoo/Documents/projects/DataPlump/data-extracts-checkpoint"

    queryStream = (
                    df1\
                    .writeStream\
                    .format("kafka")\
                    .queryName("data_transformation")\
                    .option("checkpointLocation", checkpoint_path)\
                    .option("path", raw_path)\
                    .outputMode("append")\
                    .partitionBy("change_timestamp_date", "server_name")\
                    .trigger(once=True)\
                    .start())
    clean_df = (
                spark\
                .readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", "localhost:9092")\
                .option("subscribe", "data_extracts")\
                .schema(df1.schema)\
                .load())
    queryStream()
    clean_df.printSchema()
    queryStream.stop()

read_kafka()
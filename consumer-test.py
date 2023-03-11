from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

def read_kafka_data():
    spark = (SparkSession
            .builder
            .master('local')
            .appName('dataplump')
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
            .getOrCreate())
    df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092") # kafka server
        .option("subscribe", "raw-data") # topic
        .option("startingOffsets", "earliest") # start from beginning 
        .load())
    df1 = (df\
        .withColumn("key", df["key"].cast(StringType()))\
        .withColumn("value", df["value"].cast(StringType())))
    query = df1\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()
    query.awaitTermination(8)
read_kafka_data()


    # for col_name, col_type in df.dtypes:
    #     if col_name == "InvoiceDate":
    #         if col_type != "date":
    #             raise ValueError(f"Expected 'date' data type for column '{col_name}' but got '{col_type}'")
    #     elif col_name == "UnitPrice" or col_name == "Sales" or col_name == "Profit" or col_name == "Margin":
    #         if col_type != "double":
    #             raise ValueError(f"Expected 'double' data type for column '{col_name}' but got '{col_type}'")
    #     elif col_name == "RetailerID" or col_name == "Quantity":
    #         if col_type != "int":
    #             raise ValueError(f"Expected 'int' data type for column '{col_name}' but got '{col_type}'")
    #     else:
    #         if col_type != "string":
    #             raise ValueError(f"Expected 'string' data type for column '{col_name}' but got '{col_type}'")
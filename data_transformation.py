from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import datetime
import smtplib
from email.mime.text import MIMEText

# Validate COLUMN SIZE, SCHEMA, DATA TYPES
def validate_transformed_data(data):
    # COLUMN SIZE
    if len(data.columns) != 13:
        raise ValueError(f"Expected 13 columns but got {len(data.columns)} columns. The missing columns are {data.columns}")
    # SCHEMA
    working_schema = StructType([
        StructField("Retailer", StringType(), True),
        StructField("RetailerID", IntegerType(), True),
        StructField("InvoiceDate", DateType(), True),
        StructField("Region", StringType(), True),
        StructField("State", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Profit", DoubleType(), True),
        StructField("Margin", DoubleType(), True),
        StructField("Channel", StringType(), True)
    ])
    if df.schema != working_schema:
        raise ValueError("Incorrect schema")
    # DATA TYPES (string, integer, date, double)
    string_types = ["Retailer","Region","State","City","Product","Channel"]
    integer_types = ["RetailID","Quantity"]
    date_types = ["InvoiceDate"]
    double_types = ["UnitPrice","Sales","Profit","Margin"]
    error = f"Incorrect data type. Expected a 'string' but got '{col_type}'"

    invalid_data = []
    for row in data.collect():
        for col_name, col_type in row.asDict().items():
            if col_name in date_types:
                if not isinstance(col_type, datetime.date):
                    invalid_data.append(row)
            if col_name in double_types:
                if not isinstance(col_type, float):
                    invalid_data.append(row)
            if col_name in string_types:
                if not isinstance(col_type, str):
                    invalid_data.append(row)
            if col_name in integer_types:
                if not isinstance(col_type, int):
                    invalid_data.append(row)
    if invalid_data:
        validation_alerts(invalid_data)
        raise ValueError("Validation unsuccessful")
    else:
        print("Validation succeeded")

def validation_alerts(invalid_data):
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login("dev@gmail.com", "123456789")
        subject = "Data Validation Error"
        message = f"The following rows did not pass validation: \n\n {invalid_data}"
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = "dev@gmail.com"
        msg['To'] = "limoodrice@gmail.com"
        server.sendmail("dev@gmail.com", "limoodrice@gmail.com", msg.as_string())
        print("Email sent successfully")
    except Exception as e:
        print("Error sending email: ", e)
    finally:
        server.quit()
        
def transform_data():
    try:
        spark = (SparkSession
                .builder
                .master('local')
                .appName('dataplump')
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
                .getOrCreate())
        df = (spark
            .read
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092") # kafka server
            .option("subscribe", "raw-data") # topic
            .option("startingOffsets", "earliest") # start from beginning 
            .load())
        # Convert data to DataFrame and split columns
        data = df.selectExpr("CAST(value AS STRING)").rdd\
            .map(lambda x: x[0].split(", ")) \
            .toDF(["Retailer", "Retailer ID", "Invoice Date", "Region", "State", "City", "Product", "Price per Unit", "Units Sold", "Total Sales", "Operating Profit","Operating Margin", "Sales Method"])
        # Remove dollar-percent signs
        data = (data\
            .withColumn("Price per Unit", regexp_replace("Price per Unit", "\\$", ""))
            .withColumn("Operating Margin", regexp_replace("Operating Margin", "%", "")))
        #Rename columns
        data = (data
                .withColumnRenamed("Retailer ID", "RetailerID")
                .withColumnRenamed("Invoice Date", "InvoiceDate")
                .withColumnRenamed("Price per Unit", "UnitPrice")
                .withColumnRenamed("Units Sold", "Quantity")
                .withColumnRenamed("Total Sales", "Sales")
                .withColumnRenamed("Operating Profit", "Profit")
                .withColumnRenamed("Operating Margin", "Margin")
                .withColumnRenamed("Sales Method", "Channel"))
        # Write transformed Kafka
        data \
            .selectExpr("concat_ws(', ',Retailer,RetailerID,InvoiceDate,Region,State,City,Product,UnitPrice,Quantity,Sales,Profit,Margin,Channel) AS value")\
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "clean-data") \
            .save()
        clean_data = spark\
                            .read\
                            .format("kafka")\
                            .option("kafka.bootstrap.servers", "localhost:9092")\
                            .option("subscribe", "clean-data")\
                            .load()
        validate_transformed_data(clean_data)
    except Exception as e:
        print("\n Error casting into DF: \n",e)
transform_data()
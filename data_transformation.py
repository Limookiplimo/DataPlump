from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import smtplib
from email.mime.text import MIMEText

# Validate COLUMN SIZE, SCHEMA, DATA TYPES
def validate_transformed_data(df):
    # COLUMN SIZE
    if len(df.columns) != 13:
        raise ValueError(f"Expected 13 columns but got {len(df.columns)} columns. The missing columns are {df.columns}")
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
        raise ValueError
    # DATA TYPES (string, integer, date, double)
    invalid_data = []
    for col_name, col_type in df.dtypes:
        if col_name == "InvoiceDate":
            if col_type != "date":
                invalid_data.append(col_name)
                raise ValueError(f"Expected 'date' data type for column '{col_name}' but got '{col_type}'")
        elif col_name == "UnitPrice" or col_name == "Sales" or col_name == "Profit" or col_name == "Margin":
            if col_type != "double":
                invalid_data.append(col_name)
                raise ValueError(f"Expected 'double' data type for column '{col_name}' but got '{col_type}'")
        elif col_name == "RetailerID" or col_name == "Quantity":
            if col_type != "int":
                invalid_data.append(col_name)
                raise ValueError(f"Expected 'int' data type for column '{col_name}' but got '{col_type}'")
        else:
            if col_type != "string":
                invalid_data.append(col_name)
                raise ValueError(f"Expected 'string' data type for column '{col_name}' but got '{col_type}'")
    if invalid_data:
        message = f"The following columns have invalid data types: {', '.join(invalid_data)}"
        validation_alerts(message)
        raise ValueError

def validation_alerts(message):
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login("limookiplimo@gmail.com", "token")
        subject = "Data Validation Error"
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = "limookiplimo@gmail.com"
        msg['To'] = "devdrice@gmail.com"
        server.sendmail("limookiplimo@gmail.com", "devdrice@gmail.com", msg.as_string())
        print("\n Notification mail sent successfully")
    except Exception as e:
        print("Error sending email: ", e)
    finally:
        server.quit()
        
def transform_data():
    try:
        spark = (SparkSession
                .builder
                .master('local')
                .appName('transformation')
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
                .getOrCreate())
        conn = (spark
            .read
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "raw-data")
            .option("startingOffsets", "earliest")
            .load())
        # Convert data to DataFrame and split columns
        data = conn.selectExpr("CAST(value AS STRING)").rdd\
            .map(lambda x: x[0].split(", ")) \
            .toDF(["Retailer","RetailerID","InvoiceDate","Region","State","City","Product","UnitPrice","Quantity","Sales","Profit","PercentMargin","Channel"])
        # Format columns values
        data = (data\
            .withColumn("InvoiceDate", regexp_replace(col("InvoiceDate"), "/", "-"))
            .withColumn("UnitPrice", regexp_replace("UnitPrice", '\\$', ''))
            .withColumn('Sales', regexp_replace('Sales', '\\$', ''))
            .withColumn("Profit", regexp_replace(col("Profit"), '\\$', ''))
            .withColumn("PercentMargin", regexp_replace(col("PercentMargin"), '\\%', '')))
        # Write transformed Kafka
        data \
            .selectExpr("concat_ws(', ',Retailer,RetailerID,InvoiceDate,Region,State,City,Product,UnitPrice,Quantity,Sales,Profit,PercentMargin,Channel) AS value")\
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "clean-data") \
            .save()
        validate_transformed_data(data)
    except Exception as e:
        print("\n Error casting into DF: \n",e)
transform_data()
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, to_date
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
        raise ValueError("Incorrect schema")
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
        validation_alerts(invalid_data)
        raise ValueError(f"\n Validation unsuccessful \n {len(invalid_data)}")
    print("Transformation Successful \n Validation Successful")

def validation_alerts(invalid_data):
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login("limookiplimo@gmail.com", "kyuxxojorpvtdaxy")
        subject = "Data Validation Error"
        message = f"The following rows did not pass validation: {len(invalid_data)} rows"
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
            .option("kafka.bootstrap.servers", "localhost:9092") # kafka server
            .option("subscribe", "raw-data") # topic
            .option("startingOffsets", "earliest") # start from beginning 
            .load())
        # Convert data to DataFrame and split columns
        data = conn.selectExpr("CAST(value AS STRING)").rdd\
            .map(lambda x: x[0].split(", ")) \
            .toDF(["Retailer", "Retailer ID", "Invoice Date", "Region", "State", "City", "Product", "Price per Unit", "Units Sold", "Total Sales", "Operating Profit","Operating Margin", "Sales Method"])
        # Remove dollar-percent signs
        data = (data\
            .withColumn("Retailer ID", col("Retailer ID").cast("int"))
            .withColumn("Invoice Date", to_date("Invoice Date", "MM/dd/yyyy"))
            .withColumn("Price per Unit", regexp_replace("Price per Unit", "\\$", "").cast("double"))
            .withColumn("Units Sold", col("Units Sold").cast("int"))
            .withColumn("Total Sales", regexp_replace("Total Sales", "\\$", "").cast("double"))
            .withColumn("Operating Profit", regexp_replace("Operating Profit", "\\$", "").cast("double"))
            .withColumn("Operating Margin", regexp_replace("Operating Margin", "%", "").cast("double")))
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
        
        validate_transformed_data(data)
    except Exception as e:
        print("\n Error casting into DF: \n",e)
transform_data()
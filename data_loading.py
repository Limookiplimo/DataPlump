from pyspark.sql import SparkSession
import psycopg2

def load_data():
    try:
        spark = SparkSession.builder.appName("loading").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2").getOrCreate()
        df = spark.read.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "clean-data") \
            .load()

        # Convert data to DataFrame and split columns
        data = df.selectExpr("CAST(value AS STRING)").rdd \
            .map(lambda x: x[0].split(", ")) \
            .toDF(["Retailer", "RetailerID", "InvoiceDate", "Region", "State", "City", "Product", "UnitPrice", "Quantity", "Sales", "Profit", "Margin", "Channel"])
        data.createOrReplaceTempView("clean_data")
        conn = psycopg2.connect(
            host="localhost",
            database="mydb",
            user="user",
            password="Mypassword"
        )
        cur = conn.cursor()

        # Load data into PostgreSQL table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Sales (
                retailer VARCHAR(255),
                retailer_id VARCHAR(255),
                invoice_date DATE,
                region VARCHAR(255),
                state VARCHAR(255),
                city VARCHAR(255),
                product VARCHAR(255),
                unit_price FLOAT,
                quantity INTEGER,
                sales FLOAT,
                profit FLOAT,
                margin FLOAT,
                channel VARCHAR(255)
            );
        """)
        cur.execute("""
            DELETE FROM Sales;
        """)
        cur.execute("""
            INSERT INTO Sales
            SELECT
                Retailer,
                RetailerID,
                TO_DATE(InvoiceDate, 'yyyy-MM-dd'),
                Region,
                State,
                City,
                Product,
                CAST(UnitPrice AS FLOAT),
                CAST(Quantity AS INTEGER),
                CAST(Sales AS FLOAT),
                CAST(Profit AS FLOAT),
                CAST(Margin AS FLOAT),
                Channel
            FROM clean_data
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print("\nError loading DB: ", e)
load_data()
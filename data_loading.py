import psycopg2
from kafka import KafkaConsumer

# set up PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="user",
    password="Mypassword"
)

# configure Kafka consumer
consumer = KafkaConsumer(
    'clean-data', # topic name
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
)

# insert data into PostgreSQL
for message in consumer:
    data = message.value.decode('utf-8')
    # assuming the data is in a CSV format
    fields = data.split(',')
    cur = conn.cursor()
    cur.execute("""
            CREATE TABLE IF NOT EXISTS Sales (
                Retailer VARCHAR(255),
                RetailerID VARCHAR(255),
                InvoiceDate DATE,
                Region VARCHAR(255),
                State VARCHAR(255),
                City VARCHAR(255),
                Product VARCHAR(255),
                UnitPrice VARCHAR(255),
                Quantity VARCHAR(255),
                Sales VARCHAR(255),
                Profit VARCHAR(255),
                PercentMargin VARCHAR(255),
                Channel VARCHAR(255)
            );
        """)
    cur.execute("INSERT INTO Sales( Retailer,RetailerID,InvoiceDate,Region,State,City,Product,UnitPrice,Quantity,Sales,Profit,PercentMargin,Channel) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (fields[0],fields[1],fields[2],fields[3],fields[4],fields[5],fields[6],fields[7],fields[8],fields[9],fields[10],fields[11],fields[12])
    )
    conn.commit()
    cur.close()

conn.close()
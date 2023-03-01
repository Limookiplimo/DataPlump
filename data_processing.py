import pyodbc
from kafka import KafkaProducer, KafkaConsumer


# Connection parameters
server = 'tcp:localhost,1433'
database = 'Adidas'
username = 'SA'
password = 'Admin@4321'
driver = 'ODBC Driver 18 for SQL Server'
Encrypt = 'No'

def ext_src_tbls():
    # DConnection string
    conn_string = f"""DRIVER={driver};
                        SERVER={server};
                        ENCRYPT={Encrypt};
                        DATABASE={database};
                        UID={username};kafka_bootstrap_servers
                        PWD={password};
    """
    # Establish connection
    conn= pyodbc.connect(conn_string)
    cursor = conn.cursor()
    #Execute sql query
    sql_query = f"SELECT * FROM Sales"
    cursor.execute(sql_query)

    # Set up Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    

    # Send each row to Kafka topic | Convert it to string
    for row in cursor.fetchall():
        message = str(row).encode('utf-8')
        producer.send('active_data', value = message)
    
    producer.flush()

    # Instantiate Kafka consumer to print one message from the topic
    consumer = KafkaConsumer('active_data', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', enable_auto_commit=True)
    message = next(consumer)
    print(message.value.decode())



    # Close connections
    producer.close()
    cursor.close()
    conn.close()
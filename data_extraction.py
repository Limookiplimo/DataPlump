import pyodbc
from kafka import KafkaProducer

# Define validation function
def validate_extracted_data(row):
    if len(row) !=13:
        return False
    return True
# Extract data from SQL server
def extract_data():
    conn_string = f"""DRIVER={'ODBC Driver 18 for SQL Server'};
                        SERVER={'tcp:localhost, 1433'};
                        ENCRYPT={'No'};
                        DATABASE={'DATABASE'};
                        UID={'SA'};
                        PWD={'PASSWORD'};
    """
    # Establish connection >> Exec query
    conn= pyodbc.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM Sales")
    rows = cursor.fetchall()
    # Validation checks
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    dead_letter_queue = []
    for row in rows:
        if validate_extracted_data(row):
            message = str(row).encode('utf-8')
            producer.send('raw-data', value = message)
            producer.flush()
        else:
            dead_letter_queue.append(row)
    # Close connections
    producer.close()
    cursor.close()
    conn.close()
    return dead_letter_queue

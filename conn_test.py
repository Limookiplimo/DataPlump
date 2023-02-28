import pyodbc
import pandas as pd

# Define the connection parameters
server = 'tcp:localhost,1433'
database = 'Adidas'
username = 'SA'
password = 'Admin@4321'
driver = 'ODBC Driver 18 for SQL Server'
Encrypt = 'No'

conn_string = f"DRIVER={driver};SERVER={server};ENCRYPT={Encrypt};DATABASE={database};UID={username};PWD={password}"

# Establish a connection
conn= pyodbc.connect(conn_string)
cursor = conn.cursor()
cursor.execute('SELECT * FROM Sales')
results = cursor.fetchall()

for row in results:
    print(row)

cursor.close()
conn.close()
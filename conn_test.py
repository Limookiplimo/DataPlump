import pyodbc
import pandas as pd

# Define the connection parameters
server = 'tcp:localhost,1433'
database = 'Adidas'
username = 'SA'
password = 'Admin@4321'
driver = 'ODBC Driver 18 for SQL Server' # Change this to match your installed driver
Encrypt = 'No'

# Define the connection string
conn_string = f"DRIVER={driver};SERVER={server};ENCRYPT={Encrypt};DATABASE={database};UID={username};PWD={password}"

# Establish a connection
conn= pyodbc.connect(conn_string)

# Create a cursor
cursor = conn.cursor()

# Execute a SQL query
cursor.execute('SELECT * FROM Sales')

# Fetch the results
results = cursor.fetchall()

# Print the results
for row in results:
    print(row)

# Close the cursor and connection
cursor.close()
conn.close()
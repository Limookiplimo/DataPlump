import pyodbc
import pandas as pd

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
                        UID={username};
                        PWD={password};
    """
    # Establish connection
    conn= pyodbc.connect(conn_string)

    sql_query = f"SELECT * FROM Sales"
    raw_data = pd.read_sql(sql_query, conn)
    conn.close()
    
    return raw_data



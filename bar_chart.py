import re
import psycopg2
import pandas as pd
from config import USER_NAME, DB_NAME, PASSWORD, HOST, PORT
from sqlalchemy import create_engine
import json
from psycopg2 import sql
from load import GLOBAL_CACHE
import paramiko
import socket
import threading
from dashboard_design import get_db_connection_or_path
import pyodbc
global_df = None  # Ensure global_df is initialized to None
global_column_names = None
def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def remove_symbols(value):
    if isinstance(value, str):
        return ''.join(e for e in value if e.isalnum())
    return value


# def get_column_names(db_name, username, password, table_name, selected_user, host='localhost', port='5432', connection_type='local'):
#     """
#     Retrieves numeric and text column names for a given table by querying database metadata.
#     This function avoids loading the entire table into memory, preventing MemoryErrors.

#     Args:
#         db_name (str): The name of the database.
#         username (str): Database username.
#         password (str): Database password.
#         table_name (str): The name of the table to inspect.
#         selected_user (str): The selected user for external connections.
#         host (str): Database host address.
#         port (str): Database port number.
#         connection_type (str): Type of connection ('local' or 'external').

#     Returns:
#         dict: A dictionary containing lists of 'numeric_columns' and 'text_columns'.
#               Includes an 'error' key if an exception occurs.
#     """
#     conn = None
#     cursor = None
#     try:
#         print("connection_type:", connection_type)
#         # Establish database connection based on connection_type
#         # if connection_type == 'local' or connection_type == 'null' or connection_type =='none': # 'null' for compatibility if frontend sends it
#         if not connection_type or connection_type.lower() in ('local', 'null', 'none'):
#             conn = psycopg2.connect(
#                 dbname=db_name,
#                 user=username,
#                 password=password,
#                 host=host,
#                 port=port
#             )
#         # else:  # External database connection
#         #     connection_details = fetch_external_db_connection(db_name, selected_user)
#         #     if not connection_details:
#         #         raise Exception(f"Unable to fetch external database connection details for {db_name}.")

#         #     # Ensure all required details are present and correctly mapped
#         #     db_details = {
#         #         "host": connection_details[3],
#         #         "database": connection_details[7],
#         #         "user": connection_details[4],
#         #         "password": connection_details[5],
#         #         "port": int(connection_details[6]) # Ensure port is an integer
#         #     }
#         #     print("External DB Details:", db_details)
#         else:
#             # ✅ EXTERNAL CONNECTION
#             connection_details = fetch_external_db_connection(db_name, selected_user)
#             print("fetched connection details:",connection_details)
#             if not connection_details:
#                 raise Exception(f"Unable to fetch external database connection details for user '{selected_user}'")

#             db_details = {
#                 "name": connection_details[1],
#                 "dbType": connection_details[2],
#                 "host": connection_details[3],
#                 "user": connection_details[4],
#                 "password": connection_details[5],
#                 "port": int(connection_details[6]),
#                 "database": connection_details[7],
#                 "use_ssh": connection_details[8],
#                 "ssh_host": connection_details[9],
#                 "ssh_port": int(connection_details[10]),
#                 "ssh_username": connection_details[11],
#                 "ssh_key_path": connection_details[12],
#             }

#             print(f"🔹 External DB Connection Details: {db_details}")

#             host = db_details["host"]
#             port = db_details["port"]

#             # ✅ Start SSH tunnel if required
#             if db_details["use_ssh"]:
#                 print("🔐 Establishing SSH tunnel manually (Paramiko)...")

#                 private_key = paramiko.RSAKey.from_private_key_file(db_details["ssh_key_path"])
#                 ssh_client = paramiko.SSHClient()
#                 ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#                 ssh_client.connect(
#                     db_details["ssh_host"],
#                     username=db_details["ssh_username"],
#                     pkey=private_key,
#                     port=db_details["ssh_port"],
#                     timeout=10
#                 )

#                 # Find a free local port
#                 local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                 local_sock.bind(('127.0.0.1', 0))
#                 local_port = local_sock.getsockname()[1]
#                 local_sock.listen(1)
#                 print(f"✅ Local forwarder listening on 127.0.0.1:{local_port}")

#                 transport = ssh_client.get_transport()

#                 def pipe(src, dst):
#                     try:
#                         while True:
#                             data = src.recv(1024)
#                             if not data:
#                                 break
#                             dst.sendall(data)
#                     except Exception:
#                         pass
#                     finally:
#                         src.close()
#                         dst.close()

#                 def forward_tunnel():
#                     while not stop_event.is_set():
#                         client_sock, _ = local_sock.accept()
#                         try:
#                             chan = transport.open_channel(
#                                 "direct-tcpip",
#                                 ("127.0.0.1", 5432),  # 🔹 Always map to PostgreSQL inside EC2
#                                 client_sock.getsockname()
#                             )
#                             threading.Thread(target=pipe, args=(client_sock, chan)).start()
#                             threading.Thread(target=pipe, args=(chan, client_sock)).start()
#                         except Exception as e:
#                             print(f"❌ Channel open failed: {e}")
#                             client_sock.close()

#                 tunnel_thread = threading.Thread(target=forward_tunnel, daemon=True)
#                 tunnel_thread.start()

#                 host = "127.0.0.1"
#                 port = local_port

#             # ✅ Connect to external PostgreSQL through tunnel/local port
#             print(f"🧩 Connecting to external PostgreSQL at {host}:{port} ...")
#             conn = psycopg2.connect(
#                 dbname=db_details['database'],
#                 user=db_details['user'],
#                 password=db_details['password'],
#                 host=db_details['host'],
#                 port=db_details['port']
#             )

#         cursor = conn.cursor()

#         # Query information_schema to get column names and their data types
#         # This is the key change to avoid MemoryError by not fetching all rows.
#         # It's crucial that your application has permissions to read information_schema.
#         # current_schema() will use the default schema (e.g., 'public'),
#         # if your tables are in a different schema, you'll need to specify it.
#         cursor.execute(f"""
#             SELECT column_name, data_type
#             FROM information_schema.columns
#             WHERE table_schema = current_schema()
#             AND table_name = %s;
#         """, (table_name,)) # Use parameterized query to prevent SQL injection

#         columns_metadata = cursor.fetchall()

#         numeric_columns = []
#         text_columns = []

#         # Define common numeric and text data types for PostgreSQL.
#         # You can expand these lists based on the specific data types in your database.
#         numeric_types = [
#             'smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real',
#             'double precision', 'serial', 'bigserial', 'money'
#         ]
#         text_types = [
#             'character varying', 'varchar', 'character', 'char', 'text', 'citext',
#             'json', 'jsonb', 'xml', 'uuid', 'bytea', 'tsquery', 'tsvector',
#             'inet', 'cidr', 'macaddr' # Network address types often treated as text
#         ]
#         # Date/Time types
#         datetime_types = [
#             'date', 'timestamp', 'timestamptz', 'time', 'timetz', 'interval'
#         ]
#         # Boolean type
#         boolean_type = ['boolean']

#         for column_name, data_type in columns_metadata:
#             # PostgreSQL data types are generally lowercase.
#             # Convert to lowercase to ensure consistent matching.
#             data_type_lower = data_type.lower()
#             if data_type_lower in numeric_types:
#                 numeric_columns.append(column_name)
#             elif data_type_lower in text_types or data_type_lower in datetime_types or data_type_lower in boolean_type:
#                 # Group date/time and boolean as text for charting purposes if not numeric
#                 text_columns.append(column_name)
#             else:
#                 # Default unknown types to text
#                 text_columns.append(column_name)

#         # print("Identified Numeric columns:", numeric_columns)
#         # print("Identified Text columns:", text_columns)

#         return {
#             'numeric_columns': numeric_columns,
#             'text_columns': text_columns
#         }

#     except psycopg2.Error as e:
#         print(f"Database error occurred: {e}")
#         return {'numeric_columns': [], 'text_columns': [], 'error': f"Database error: {e}"}
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         return {'numeric_columns': [], 'text_columns': [], 'error': f"An unexpected error occurred: {e}"}
#     finally:
#         # Ensure cursor and connection are closed
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
# def get_column_names(db_name, username, password, table_name, selected_user,
#                      host='localhost', port='5432', connection_type='local'):
#     """
#     Retrieve numeric and text column names from a given table
#     for both local and external (SSH) PostgreSQL connections.
#     """
#     print("kconnect",connection_type)
#     conn = None
#     cursor = None
#     ssh_client = None
#     local_sock = None
#     stop_event = threading.Event()
#     tunnel_thread = None

#     try:
#         print("connection_type:", connection_type)

#         # ✅ 1️⃣ LOCAL DATABASE CONNECTION
#         if not connection_type or connection_type.lower() in ('local', 'null', 'none'):
#             conn = psycopg2.connect(
#                 dbname=db_name,
#                 user=username,
#                 password=password,
#                 host=host,
#                 port=port
#             )

#         # ✅ 2️⃣ EXTERNAL DATABASE CONNECTION (via SSH tunnel)
#         else:
#             connection_details = fetch_external_db_connection(db_name, selected_user)
#             print("fetched connection details:", connection_details)
#             if not connection_details:
#                 raise Exception(f"Unable to fetch external database connection details for user '{selected_user}'")
#             use_ssh = bool(connection_details[8])
#             db_details = {
#                 "name": connection_details[1],
#                 "dbType": connection_details[2],
#                 "host": connection_details[3],
#                 "user": connection_details[4],
#                 "password": connection_details[5],
#                 "port": int(connection_details[6]),
#                 "database": connection_details[7],
#                 "use_ssh": use_ssh,
#                 "ssh_host": connection_details[9] if use_ssh else None,
#                 "ssh_port": int(connection_details[10] or 22) if use_ssh else None,
#                 "ssh_username": connection_details[11] if use_ssh else None,
#                 "ssh_key_path": connection_details[12] if use_ssh else None,
#                 # "use_ssh": connection_details[8],
#                 # "ssh_host": connection_details[9],
#                 # "ssh_port": int(connection_details[10]),
#                 # "ssh_username": connection_details[11],
#                 # "ssh_key_path": connection_details[12],
#             }
#             target_host = db_details["host"]
#             target_port = db_details["port"]

#             print(f"🔹 External DB Connection Details: {db_details}")

#             # ✅ Start SSH tunnel if needed
#             if db_details["use_ssh"]:
#                 print("🔐 Establishing SSH tunnel manually (Paramiko)...")
#                 private_key = paramiko.RSAKey.from_private_key_file(db_details["ssh_key_path"])

#                 ssh_client = paramiko.SSHClient()
#                 ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#                 ssh_client.connect(
#                     db_details["ssh_host"],
#                     username=db_details["ssh_username"],
#                     pkey=private_key,
#                     port=db_details["ssh_port"],
#                     timeout=10
#                 )

#                 # Find free local port for tunnel
#                 local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                 local_sock.bind(('127.0.0.1', 0))
#                 local_port = local_sock.getsockname()[1]
#                 local_sock.listen(1)
#                 print(f"✅ Local forwarder listening on 127.0.0.1:{local_port}")

#                 transport = ssh_client.get_transport()

#                 def pipe(src, dst):
#                     try:
#                         while True:
#                             data = src.recv(1024)
#                             if not data:
#                                 break
#                             dst.sendall(data)
#                     except Exception:
#                         pass
#                     finally:
#                         src.close()
#                         dst.close()

#                 def forward_tunnel():
#                     while not stop_event.is_set():
#                         try:
#                             client_sock, _ = local_sock.accept()
#                             chan = transport.open_channel(
#                                 "direct-tcpip",
#                                 ("127.0.0.1", 5432),
#                                 client_sock.getsockname()
#                             )
#                             if chan is None:
#                                 client_sock.close()
#                                 continue
#                             threading.Thread(target=pipe, args=(client_sock, chan), daemon=True).start()
#                             threading.Thread(target=pipe, args=(chan, client_sock), daemon=True).start()
#                         except Exception as e:
#                             print(f"❌ Channel open failed: {e}")

#                 tunnel_thread = threading.Thread(target=forward_tunnel, daemon=True)
#                 tunnel_thread.start()

#                 # Override host and port for local tunnel
#                 target_host = "127.0.0.1"
#                 target_port = local_port

#             print(f"🧩 Connecting to external PostgreSQL at {target_host}:{target_port} ...")
            
#         #     conn = psycopg2.connect(
#         #         dbname=db_details['database'],
#         #         user=db_details['user'],
#         #         password=db_details['password'],
#         #         host=host,       # ✅ Use tunneled host
#         #         port=port        # ✅ Use tunneled port
#         #     )

#         # # ✅ Fetch column names and types
#         # cursor = conn.cursor()
#         # cursor.execute("""
#         #     SELECT column_name, data_type
#         #     FROM information_schema.columns
#         #     WHERE table_schema = current_schema()
#         #     AND table_name = %s;
#         # """, (table_name,))

#         # columns_metadata = cursor.fetchall()

#         if db_details['dbType'] == 'MSSQL':
#             conn_str = (
#                 f"DRIVER={{ODBC Driver 18 for SQL Server}};"
#                 f"SERVER={target_host},{target_port};"
#                 f"DATABASE={db_details['database']};"
#                 f"UID={db_details['user']};"
#                 f"PWD={db_details['password']};"
#                 f"Timeout=10;"
#             )
#             conn = pyodbc.connect(conn_str)
#             query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
#             query_params = (table_name,)
#         else:
#             conn = psycopg2.connect(
#                 dbname=db_details['database'],
#                 user=db_details['user'],
#                 password=db_details['password'],
#                 host=target_host,
#                 port=target_port
#             )
#             # PostgreSQL uses current_schema() to be safe
#             query = """
#                 SELECT column_name, data_type 
#                 FROM information_schema.columns 
#                 WHERE table_name = %s AND table_schema = current_schema()
#             """
#             query_params = (table_name,)

#         # --- 4. FETCH AND PROCESS COLUMNS ---
#         cursor = conn.cursor()
#         cursor.execute(query, query_params)
#         columns_metadata = cursor.fetchall()
#         numeric_columns = []
#         text_columns = []

#         numeric_types = [
#             'smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real',
#             'double precision', 'serial', 'bigserial', 'money'
#         ]
#         text_types = [
#             'character varying', 'varchar', 'character', 'char', 'text', 'citext',
#             'json', 'jsonb', 'xml', 'uuid', 'bytea', 'tsquery', 'tsvector',
#             'inet', 'cidr', 'macaddr'
#         ]
#         datetime_types = ['date', 'timestamp', 'timestamptz', 'time', 'timetz', 'interval']
#         boolean_type = ['boolean']

#         for column_name, data_type in columns_metadata:
#             data_type_lower = data_type.lower()
#             if data_type_lower in numeric_types:
#                 numeric_columns.append(column_name)
#             elif data_type_lower in text_types or data_type_lower in datetime_types or data_type_lower in boolean_type:
#                 text_columns.append(column_name)
#             else:
#                 text_columns.append(column_name)

#         return {'numeric_columns': numeric_columns, 'text_columns': text_columns}

#     except psycopg2.Error as e:
#         print(f"Database error occurred: {e}")
#         return {'numeric_columns': [], 'text_columns': [], 'error': f"Database error: {e}"}

#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         return {'numeric_columns': [], 'text_columns': [], 'error': f"An unexpected error occurred: {e}"}

#     finally:
#         # ✅ Close resources
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#         if ssh_client:
#             stop_event.set()
#             ssh_client.close()
#             print("🔒 SSH Tunnel closed.")
def prefer_masked_columns(columns_metadata):
    """
    If <col>_masked exists, hide <col> and use masked version instead
    """
    seen = {}
    final_columns = []

    for column_name, data_type in columns_metadata:
        if column_name.endswith('_masked'):
            base_col = column_name.replace('_masked', '')
            seen[base_col] = (column_name, data_type)
        else:
            if column_name not in seen:
                seen[column_name] = (column_name, data_type)

    return list(seen.values())

def get_column_names(db_name, username, password, table_name, selected_user, 
                     host='localhost', port='5432', connection_type='local'):
    """
    Retrieve numeric and text column names from a given table 
    for both local and external (SSH) PostgreSQL/MSSQL connections.
    """
    conn = None
    cursor = None
    ssh_client = None
    local_sock = None
    stop_event = threading.Event()
    tunnel_thread = None
    
    # Initialize query variables
    query = ""
    query_params = ()

    try:
        print("connection_type:", connection_type)

        # =======================================================
        # ✅ 1️⃣ LOCAL DATABASE CONNECTION (Assumes PostgreSQL)
        # =======================================================
        if not connection_type or connection_type.lower() in ('local', 'null', 'none'):
            print("🔹 Establishing Local PostgreSQL Connection...")
            conn = psycopg2.connect(
                dbname=db_name,
                user=username,
                password=password,
                host=host,
                port=port
            )
            
            # Set PostgreSQL Query
            query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = current_schema()
            """
            query_params = (table_name,)

        # =======================================================
        # ✅ 2️⃣ EXTERNAL DATABASE CONNECTION (MSSQL or PostgreSQL)
        # =======================================================
        else:
            connection_details = fetch_external_db_connection(db_name, selected_user)
            if not connection_details:
                raise Exception(f"Unable to fetch external database connection details for user '{selected_user}'")

            # Extract details safely
            use_ssh = bool(connection_details[8])
            db_details = {
                "name": connection_details[1],
                "dbType": connection_details[2],
                "host": connection_details[3],
                "user": connection_details[4],
                "password": connection_details[5],
                "port": int(connection_details[6]),
                "database": connection_details[7],
                "use_ssh": use_ssh,
                "ssh_host": connection_details[9] if use_ssh else None,
                "ssh_port": int(connection_details[10] or 22) if use_ssh else None,
                "ssh_username": connection_details[11] if use_ssh else None,
                "ssh_key_path": connection_details[12] if use_ssh else None,
            }
            
            target_host = db_details["host"]
            target_port = db_details["port"]

            print(f"🔹 External DB Details: {db_details}")

            # --- Start SSH Tunnel if needed ---
            if db_details["use_ssh"]:
                print("🔐 Establishing SSH tunnel manually (Paramiko)...")
                private_key = paramiko.RSAKey.from_private_key_file(db_details["ssh_key_path"])

                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(
                    db_details["ssh_host"],
                    username=db_details["ssh_username"],
                    pkey=private_key,
                    port=db_details["ssh_port"],
                    timeout=10
                )

                # Find free local port
                local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                local_sock.bind(('127.0.0.1', 0))
                local_port = local_sock.getsockname()[1]
                local_sock.listen(1)
                
                transport = ssh_client.get_transport()

                def pipe(src, dst):
                    try:
                        while True:
                            data = src.recv(1024)
                            if not data: break
                            dst.sendall(data)
                    except: pass
                    finally:
                        src.close()
                        dst.close()

                def forward_tunnel():
                    while not stop_event.is_set():
                        try:
                            client_sock, _ = local_sock.accept()
                            chan = transport.open_channel(
                                "direct-tcpip",
                                (target_host, target_port), # Forward to remote DB
                                client_sock.getsockname()
                            )
                            if chan is None:
                                client_sock.close()
                                continue
                            threading.Thread(target=pipe, args=(client_sock, chan), daemon=True).start()
                            threading.Thread(target=pipe, args=(chan, client_sock), daemon=True).start()
                        except Exception as e:
                            print(f"❌ Channel open failed: {e}")

                tunnel_thread = threading.Thread(target=forward_tunnel, daemon=True)
                tunnel_thread.start()

                # Update target to point to the local tunnel
                target_host = "127.0.0.1"
                target_port = local_port

            print(f"🧩 Connecting to External {db_details['dbType']} at {target_host}:{target_port} ...")

            # --- Connect based on DB Type ---
            if db_details['dbType'] == 'MSSQL':
                conn_str = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={target_host},{target_port};"
                    f"DATABASE={db_details['database']};"
                    f"UID={db_details['user']};"
                    f"PWD={db_details['password']};"
                    f"Timeout=10;"
                )
                conn = pyodbc.connect(conn_str)
                query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
                query_params = (table_name,)
            
            else: # Defaults to PostgreSQL
                conn = psycopg2.connect(
                    dbname=db_details['database'],
                    user=db_details['user'],
                    password=db_details['password'],
                    host=target_host,
                    port=target_port
                )
                query = """
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND table_schema = current_schema()
                """
                query_params = (table_name,)

        # =======================================================
        # ✅ 3️⃣ FETCH METADATA (Common for all)
        # =======================================================
        cursor = conn.cursor()
        cursor.execute(query, query_params)
        columns_metadata = cursor.fetchall()
        columns_metadata = prefer_masked_columns(columns_metadata)

        numeric_columns = []
        text_columns = []

        # Data Type Mappings
        numeric_types = [
            'smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real',
            'double precision', 'serial', 'bigserial', 'money', 'float', 'int', 'tinyint', 'bit'
        ]
        text_types = [
            'character varying', 'varchar', 'character', 'char', 'text', 'citext',
            'json', 'jsonb', 'xml', 'uuid', 'bytea', 'nvarchar', 'nchar'
        ]
        datetime_types = ['date', 'timestamp', 'timestamptz', 'time', 'timetz', 'interval', 'datetime', 'datetime2', 'smalldatetime']
        boolean_type = ['boolean', 'bool']

        for column_name, data_type in columns_metadata:
            data_type_lower = data_type.lower()
            if data_type_lower in numeric_types:
                numeric_columns.append(column_name)
            else:
                # Treat text, dates, and booleans as text/categorical for now
                text_columns.append(column_name)

        return {'numeric_columns': numeric_columns, 'text_columns': text_columns}

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {'numeric_columns': [], 'text_columns': [], 'error': f"Error: {e}"}

    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        if ssh_client:
            stop_event.set()
            ssh_client.close()
            print("🔒 SSH Tunnel closed.")





def fetch_external_db_connection(db_name,selectedUser):
    try:
        print("company_name", db_name)
        print("selectedUser",selectedUser)
        # Connect to local PostgreSQL to get external database connection details
        conn = psycopg2.connect(
            dbname=db_name,  # Ensure this is the correct company database
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        print("conn", conn)
        cursor = conn.cursor()
        query = """
            SELECT * 
            FROM external_db_connections 
            WHERE savename = %s 
            ORDER BY created_at DESC 
            LIMIT 1;
        """
        print("query",query)
        print("selectedUser",selectedUser)
        cursor.execute(query, (selectedUser,))
        connection_details = cursor.fetchone()
        print('connection',connection_details)
        conn.close()
        return connection_details
    except Exception as e:
        print(f"Error fetching connection details: {e}")
        return None


def edit_fetch_data(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name, selectedUser):
    global_df = None

    if global_df is None:
        print("Fetching data from the database...")
        try:
            # Establish database connection
            # if not selectedUser or str(selectedUser).lower() == 'null':
            #     print("Using default database connection...")
            #     connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
            #     conn = psycopg2.connect(connection_string)
            # else:
            #     print(f"Using connection for user: {selectedUser}")
            #     connection_string = fetch_external_db_connection(db_name, selectedUser)
            #     if not connection_string:
            #         raise Exception("Unable to fetch external database connection details.")

            #     db_details = {
            #         "host": connection_string[3],
            #         "database": connection_string[7],
            #         "user": connection_string[4],
            #         "password": connection_string[5],
            #         "port": int(connection_string[6])
            #     }

            #     conn = psycopg2.connect(
            #         dbname=db_details['database'],
            #         user=db_details['user'],
            #         password=db_details['password'],
            #         host=db_details['host'],
            #         port=db_details['port']
            #     )
            # if not selectedUser or selectedUser.lower() == 'null':
            #     print("🟢 Using default local database connection...")
            #     connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST} port={PORT}"
            #     conn = psycopg2.connect(connection_string)

            # else:
            #     print(f"🟡 Using external database connection for user: {selectedUser}")
            #     connection_details = fetch_external_db_connection(db_name, selectedUser)

            #     if not connection_details:
            #         raise Exception(f"❌ Unable to fetch external database connection details for user '{selectedUser}'")

            #     db_details = {
            #         "name": connection_details[1],
            #         "dbType": connection_details[2],
            #         "host": connection_details[3],
            #         "user": connection_details[4],
            #         "password": connection_details[5],
            #         "port": int(connection_details[6]),
            #         "database": connection_details[7],
            #         "use_ssh": connection_details[8],
            #         "ssh_host": connection_details[9],
            #         "ssh_port": int(connection_details[10]),
            #         "ssh_username": connection_details[11],
            #         "ssh_key_path": connection_details[12],
            #     }

            #     print(f"🔹 External DB Connection Details: {db_details}")

            #     # Initialize SSH tunnel-related variables
            #     ssh_client = None
            #     local_sock = None
            #     stop_event = threading.Event()
            #     tunnel_thread = None

            #     # ✅ SSH Tunnel Setup if required
            #     if db_details["use_ssh"]:
            #         print("🔐 Establishing SSH tunnel manually (Paramiko)...")
            #         private_key = paramiko.RSAKey.from_private_key_file(db_details["ssh_key_path"])

            #         ssh_client = paramiko.SSHClient()
            #         ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #         ssh_client.connect(
            #             db_details["ssh_host"],
            #             username=db_details["ssh_username"],
            #             pkey=private_key,
            #             port=db_details["ssh_port"],
            #             timeout=10
            #         )

            #         # Find a free local port for tunnel forwarding
            #         local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #         local_sock.bind(('127.0.0.1', 0))
            #         local_port = local_sock.getsockname()[1]
            #         local_sock.listen(1)
            #         print(f"✅ Local forwarder listening on 127.0.0.1:{local_port}")

            #         transport = ssh_client.get_transport()

            #         def pipe(src, dst):
            #             try:
            #                 while True:
            #                     data = src.recv(1024)
            #                     if not data:
            #                         break
            #                     dst.sendall(data)
            #             except Exception:
            #                 pass
            #             finally:
            #                 src.close()
            #                 dst.close()

            #         def forward_tunnel():
            #             while not stop_event.is_set():
            #                 try:
            #                     client_sock, _ = local_sock.accept()
            #                     chan = transport.open_channel(
            #                         "direct-tcpip",
            #                         ("127.0.0.1", 5432),
            #                         client_sock.getsockname()
            #                     )
            #                     if chan is None:
            #                         client_sock.close()
            #                         continue
            #                     threading.Thread(target=pipe, args=(client_sock, chan), daemon=True).start()
            #                     threading.Thread(target=pipe, args=(chan, client_sock), daemon=True).start()
            #                 except Exception as e:
            #                     print(f"❌ Channel open failed: {e}")

            #         tunnel_thread = threading.Thread(target=forward_tunnel, daemon=True)
            #         tunnel_thread.start()

            #         # Override host and port to tunnel
            #         host = "127.0.0.1"
            #         port = local_port
            #     else:
            #         host = db_details["host"]
            #         port = db_details["port"]

            #     print(f"🧩 Connecting to external PostgreSQL at {host}:{port} ...")

            #     conn = psycopg2.connect(
            #         dbname=db_details["database"],
            #         user=db_details["user"],
            #         password=db_details["password"],
            #         host=host,
            #         port=port
            #     )
            conn = get_db_connection_or_path(selectedUser, db_name)
            cur = conn.cursor()
            query = f"SELECT {', '.join([x_axis_columns[0], y_axis_column[0]])} FROM {table_name}"
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            # Create a pandas DataFrame from the fetched data
            global_df = pd.DataFrame(data, columns=colnames)

            print("Full DataFrame:")
            print(global_df)

            # Ensure the y-axis column is numeric
            y_axis = y_axis_column[0] if isinstance(y_axis_column, list) else y_axis_column
            if y_axis in global_df.columns:
                global_df[y_axis] = pd.to_numeric(global_df[y_axis], errors='coerce')
                print(f"Converted {y_axis} to numeric values.")
            else:
                raise KeyError(f"Column '{y_axis}' not found in the table.")

        except Exception as e:
            print(f"Error while fetching data from the database: {e}")
            return None
        finally:
            if 'cur' in locals() and cur:
                cur.close()
            if 'conn' in locals() and conn:
                conn.close()

    try:
        # Define the aggregation function
        aggregation_func_map = {
            "sum": "sum",
            "average": "mean",
            "count": "count",
            "maximum": "max",
            "minimum": "min"
        }

        if aggregation.lower() not in aggregation_func_map:
            raise ValueError(f"Invalid aggregation type: {aggregation}")

        aggregation_func = aggregation_func_map[aggregation.lower()]

        # Validate x-axis columns and options
        if not x_axis_columns or not y_axis_column:
            raise ValueError("x_axis_columns and y_axis_column must not be empty.")

        if x_axis_columns[0] not in global_df.columns:
            raise KeyError(f"Column '{x_axis_columns[0]}' not found in the DataFrame.")

        # Filter and group the DataFrame
        options = checked_option.get(x_axis_columns[0], [])
        filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
        grouped_df = filtered_df.groupby(x_axis_columns[0]).agg({y_axis: aggregation_func}).reset_index()

        # Convert the grouped DataFrame to a list of tuples
        result = [tuple(x) for x in grouped_df.to_numpy()]
        return result

    except Exception as e:
        print(f"Error during data processing: {e}")
        return None




def count_function(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name):
    global global_df

    if global_df is None:
        print("Fetching data from the database...")
        conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
        cur = conn.cursor()
        query = f"SELECT * FROM {table_name}"
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        global_df = pd.DataFrame(data, columns=colnames)
        global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

    else:
        global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

    x_axis_columns_str = x_axis_columns
    options = [option.strip() for option in checked_option.split(',')]
    filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
    
    # Perform aggregation based on the selected aggregation type
    if aggregation == "sum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].sum().reset_index()
    elif aggregation == "average":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].mean().reset_index()
    elif aggregation == "count":
        # Count without considering decimal values
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].apply(lambda x: x.dropna().astype(int).count()).reset_index()
        print("grouped_df:", grouped_df)
    elif aggregation == "maximum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].max().reset_index()
    elif aggregation == "minimum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].min().reset_index()
    else:
        raise ValueError(f"Unsupported aggregation type: {aggregation}")

    # Convert the result to a list of tuples for easy output
    result = [tuple(x) for x in grouped_df.to_numpy()]
    
    return result

def apply_all_calculations(temp_df, calculationData, x_axis_columns=None, y_axis_column=None):
    if not calculationData:
        return temp_df, x_axis_columns, y_axis_column

    for calc in calculationData:
        calc_formula = calc.get("calculation", "").strip()
        new_col_name = calc.get("columnName", "").strip()
        replace_col = calc.get("replaceColumn", new_col_name)

        if not calc_formula or not new_col_name:
            continue

        # ---------- Axis replacement ----------
        if y_axis_column:
            y_axis_column = [
                new_col_name if c == replace_col else c
                for c in y_axis_column
            ]

        if x_axis_columns:
            x_axis_columns = [
                new_col_name if c == replace_col else c
                for c in x_axis_columns
            ]

        # ---------- SINGLE CALL ----------
        temp_df = perform_calculation(
            dataframe=temp_df,
            columnName=new_col_name,
            calculation=calc_formula
        )

    return temp_df, x_axis_columns, y_axis_column


def fetch_data(table_name, x_axis_columns, filter_options, y_axis_column, aggregation, db_name, selectedUser, calculationData, dateGranularity):
    import numpy as np
    import json
    import re
    print("dateGranularity......................", dateGranularity)
    print("data",table_name, x_axis_columns, filter_options, y_axis_column, aggregation, db_name, selectedUser, calculationData, dateGranularity)

    global global_df
    # print("global_df",global_df)
    if global_df is None:
        print("Fetching data from the database...")
        if not selectedUser or selectedUser.lower() == 'null':
            print("Using default database connection...")
            # connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
            # connection = psycopg2.connect(connection_string)
        else:
            connection_details = fetch_external_db_connection(db_name, selectedUser)
            if not connection_details:
                raise Exception("Unable to fetch external database connection details.")
            db_details = {
                "host": connection_details[3],
                "database": connection_details[7],
                "user": connection_details[4],
                "password": connection_details[5],
                "port": int(connection_details[6])
            }
            connection = psycopg2.connect(
                dbname=db_details['database'],
                user=db_details['user'],
                password=db_details['password'],
                host=db_details['host'],
                port=db_details['port'],
            )
        cur = connection.cursor()
        query = f"SELECT * FROM {table_name}"
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        connection.close()

        global_df = pd.DataFrame(data, columns=colnames)

    temp_df = global_df.copy()
    # ------------------- PROTECT DATE COLUMNS -------------------
    # ------------------- PROTECT DATE COLUMNS -------------------
    date_cols = [col for col in temp_df.columns if "date" in col.lower()]
    for dcol in date_cols:
        # Try converting safely WITHOUT using errors='ignore'
        try:
            temp_df[dcol] = pd.to_datetime(temp_df[dcol], dayfirst=True)
        except Exception:
            # If conversion fails, do NOT modify the column
            print(f"{dcol} is not a valid date column. No conversion applied.")
            pass


        

    # Handle calculation logic
    # if calculationData and calculationData.get('calculation') and calculationData.get('columnName'):
    # At the top, before looping over calculationData
    skip_calculated_column = False
    y_base_column = None

    if calculationData and isinstance(calculationData, list):
        for calc_entry in calculationData:
            calc_formula = calc_entry.get('calculation', '').strip()
            new_col_name = calc_entry.get('columnName', '').strip()
            replace_col = calc_entry.get('replaceColumn', new_col_name)
            temp_df, x_axis_columns, y_axis_column = apply_all_calculations(
                temp_df,
                calculationData,
                x_axis_columns,
                y_axis_column
            )

            # if not calc_formula or not new_col_name:
            #     continue  # Skip incomplete entries

            # # Apply only if the column is involved in x or y axis
            # if new_col_name not in (x_axis_columns or []) and new_col_name not in (y_axis_column or []):
            #     continue

            # def replace_column(match):
            #     col_name = match.group(1)
            #     if col_name in temp_df.columns:
            #         # Ensure numeric columns are treated as such for math operations
            #         # This might need refinement based on exact column types and operations
            #         # For string operations, keep as is
            #         if temp_df[col_name].dtype in [np.int64, np.float64]:
            #             return f"temp_df['{col_name}']"
            #         else:
            #             return f"temp_df['{col_name}']" # Treat as string if not numeric
            #     else:
            #         raise ValueError(f"Column '{col_name}' not found in DataFrame for calculation.")

            # if y_axis_column:
            #     y_axis_column = [new_col_name if col == replace_col else col for col in y_axis_column]

            # if x_axis_columns:
            #     x_axis_columns = [new_col_name if col == replace_col else col for col in x_axis_columns]

            #     # if new_col_name in y_axis_column:

            # # Handle "if (...) then ... else ..." expressions
            # if calc_formula.strip().lower().startswith("if"):
            #     match = (
            #         re.match(
            #             r"if\s*\(\s*(.+?)\s*\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$",
            #             calc_formula.strip(),
            #             re.IGNORECASE
            #         )
            #         or
            #         re.match(
            #             r"if\s*\(\s*(.+?)\s*,\s*'?(.*?)'?\s*,\s*'?(.*?)'?\s*\)$",
            #             calc_formula.strip(),
            #             re.IGNORECASE
            #         )
            #     )

            #     # match = re.match(r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$", calc_formula.strip(), re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid if-then-else format in calculation.")

            #     condition_expr, then_val, else_val = match.groups()

            #     condition_expr_python = re.sub(r'\[(.*?)\]', replace_column, condition_expr)
            #     then_val = then_val.strip('"').strip("'")
            #     else_val = else_val.strip('"').strip("'")

            #     print("Evaluating formula as np.where:", f"np.where({condition_expr_python}, {then_val}, {else_val})")
            #     temp_df[new_col_name] = np.where(eval(condition_expr_python),f"{then_val}",f"{else_val}")
            # elif calc_formula.lower().startswith("switch"):
            #     switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calc_formula, re.IGNORECASE)
            #     if not switch_match:
            #         raise ValueError("Invalid SWITCH syntax")

            #     col_name, rest = switch_match.groups()
            #     if col_name not in temp_df.columns:
            #         raise ValueError(f"Column '{col_name}' not found in DataFrame")

            #     cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
            #     default_match = re.search(r'["\']?default["\']?\s*,\s*["\']?(.*?)["\']?\s*$', rest, re.IGNORECASE)
            #     default_value = default_match.group(1) if default_match else None
            # elif calc_formula.lower().startswith("iferror"):
            #     match = re.match(r"iferror\s*\((.+?)\s*,\s*(.+?)\)", calc_formula.strip(), re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid IFERROR format")

            #     expr, fallback = match.groups()
            #     expr_python = re.sub(r'\[(.*?)\]', replace_column, expr)
            #     fallback = fallback.strip()
            #     print("Evaluating IFERROR formula:", expr_python)

            #     try:
            #         temp_df[new_col_name] = eval(expr_python)
            #         temp_df[new_col_name] = temp_df[new_col_name].fillna(fallback)
            #     except Exception as e:
            #         print("Error in IFERROR eval:", e)
            #         temp_df[new_col_name] = fallback

            # # Case 4: CALCULATE(SUM([col]), [filter] = 'X')
            # elif calc_formula.lower().startswith("calculate"):
            #     match = re.match(r"calculate\s*\(\s*(sum|avg|count|max|min)\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*\[([^\]]+)\]\s*=\s*['\"](.*?)['\"]\s*\)", calc_formula.strip(), re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid CALCULATE format")

            #     agg_func, value_col, filter_col, filter_val = match.groups()
            #     print(f"Applying CALCULATE: {agg_func.upper()}({value_col}) WHERE {filter_col} = {filter_val}")

            #     df_filtered = temp_df[temp_df[filter_col] == filter_val]
            #     if agg_func == "sum":
            #         result_val = df_filtered[value_col].astype(float).sum()
            #     elif agg_func == "avg":
            #         result_val = df_filtered[value_col].astype(float).mean()
            #     elif agg_func == "count":
            #         result_val = df_filtered[value_col].count()
            #     elif agg_func == "distinct count":
            #         result_val = df_filtered[value_col].nunique()
            #     elif agg_func == "max":
            #         result_val = df_filtered[value_col].astype(float).max()
            #     elif agg_func == "min":
            #         result_val = df_filtered[value_col].astype(float).min()
            #     else:
            #         raise ValueError("Unsupported aggregate in CALCULATE")

            #     temp_df[new_col_name] = result_val
            # elif calc_formula.lower().startswith("maxx") or calc_formula.lower().startswith("minx"):
            #     match = re.match(r'(maxx|minx)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid MAXX/MINX syntax.")
            #     func, col = match.groups()
            #     if col not in temp_df.columns:
            #         raise ValueError(f"Column '{col}' not found.")
            #     result_val = temp_df[col].max() if func.lower() == "maxx" else temp_df[col].min()
            #     temp_df[new_col_name] = result_val
            # elif calc_formula.lower().startswith("abs"):
            #     match = re.match(r'abs\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid ABS syntax.")
            #     col = match.group(1)
            #     if col not in temp_df.columns:
            #         raise ValueError(f"Column '{col}' not found.")
            #     temp_df[new_col_name] = temp_df[col].abs()
            # elif calc_formula.lower().startswith("len"):
            #     match = re.match(r'len\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', calc_formula, re.IGNORECASE)
            #     col = match.group(1) or match.group(2)
            #     if col not in temp_df.columns:
            #         raise ValueError(f"Column '{col}' not found.")
            #     temp_df[new_col_name] = temp_df[col].astype(str).str.len()
            # elif calc_formula.lower().startswith("lower"):
            #     match = re.match(r'lower\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
            #     col = match.group(1)
            #     temp_df[new_col_name] = temp_df[col].astype(str).str.lower()

            # elif calc_formula.lower().startswith("upper"):
            #     match = re.match(r'upper\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
            #     col = match.group(1)
            #     temp_df[new_col_name] = temp_df[col].astype(str).str.upper()
            # elif calc_formula.lower().startswith("concat"):
            #     match = re.match(r'concat\s*\((.+)\)', calc_formula, re.IGNORECASE)
            #     if match:
            #         parts = [p.strip() for p in re.split(r',(?![^\[]*\])', match.group(1))]
            #         concat_parts = []
            #         for part in parts:
            #             if part.startswith('[') and part.endswith(']'):
            #                 col = part[1:-1]
            #                 if col not in temp_df.columns:
            #                     raise ValueError(f"Column '{col}' not found.")
            #                 concat_parts.append(temp_df[col].astype(str))
            #             else:
            #                 concat_parts.append(part.strip('"').strip("'"))
            #         from functools import reduce
            #         temp_df[new_col_name] = reduce(lambda x, y: x + y, [p if isinstance(p, pd.Series) else pd.Series([p]*len(temp_df)) for p in concat_parts])

            # elif re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE):
            #     match = re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
            #     func, col = match.groups()
            #     if col not in temp_df.columns:
            #         raise ValueError(f"Column '{col}' not found.")
            #     temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
            #     if func.lower() == "year":
            #         temp_df[new_col_name] = temp_df[col].dt.year
            #     elif func.lower() == "month":
            #         temp_df[new_col_name] = temp_df[col].dt.month
            #     elif func.lower() == "day":
            #         temp_df[new_col_name] = temp_df[col].dt.day

            # elif calc_formula.lower().startswith("isnull"):
            #     match = re.match(r'isnull\s*\(\s*\[([^\]]+)\]\s*,\s*["\']?(.*?)["\']?\s*\)', calc_formula, re.IGNORECASE)
            #     if match:
            #         col, fallback = match.groups()
            #         if col not in temp_df.columns:
            #             raise ValueError(f"Column '{col}' not found.")
            #         temp_df[new_col_name] = temp_df[col].fillna(fallback)
            # elif re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', calc_formula, re.IGNORECASE):
            #     match = re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', calc_formula, re.IGNORECASE)
            #     col = match.group(1) or match.group(2)
            #     raw_values = match.group(3)

            #     # Parse the values correctly
            #     cleaned_values = []
            #     for v in raw_values.split(','):
            #         v = v.strip().strip('"').strip("'")
            #         cleaned_values.append(v)

            #     if col not in temp_df.columns:
            #         raise ValueError(f"Column '{col}' not found in DataFrame.")

            #     temp_df[new_col_name] = temp_df[col].isin(cleaned_values)
            #     print("temp_df[new_col_name]",temp_df[new_col_name])
            # elif calc_formula.lower().startswith("datediff"):
            #     match = re.match(r'datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid DATEDIFF format.")
            #     end_col, start_col = match.groups()
            #     temp_df[end_col] = pd.to_datetime(temp_df[end_col], errors='coerce')
            #     temp_df[start_col] = pd.to_datetime(temp_df[start_col], errors='coerce')
            #     temp_df[new_col_name] = (temp_df[end_col] - temp_df[start_col]).dt.days

            # # elif calc_formula.lower().startswith("today()"):
            # #     temp_df[new_col_name] = pd.Timestamp.today().normalize()
            # elif calc_formula.lower().startswith("today()"):
            #     # Assign today's date (normalized to midnight) to each row
            #     temp_df[new_col_name] = pd.to_datetime(pd.Timestamp.today().normalize())


            # elif calc_formula.lower().startswith("now()"):
            #     temp_df[new_col_name] = pd.Timestamp.now()

        
            # elif calc_formula.lower().startswith("dateadd"):
            #     match = re.match(
            #         r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)',
            #         calc_formula,
            #         re.IGNORECASE
            #     )
            #     if not match:
            #         raise ValueError("Invalid DATEADD format. Use: dateadd([column], number, 'unit')")

            #     col, interval, unit = match.groups()
            #     interval = int(interval)

            #     # Step 1: Ensure the source column exists
            #     if col not in temp_df.columns:
            #         raise ValueError(f"DATEADD error: Column '{col}' not found in dataframe")

            #     # Step 2: Convert to datetime (NaNs will be handled)
            #     temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')

            #     # Step 3: Apply the offset
            #     if unit == "day":
            #         temp_df[new_col_name] = temp_df[col] + pd.to_timedelta(interval, unit='d')
            #     elif unit == "month":
            #         temp_df[new_col_name] = temp_df[col] + pd.DateOffset(months=interval)
            #     elif unit == "year":
            #         temp_df[new_col_name] = temp_df[col] + pd.DateOffset(years=interval)
            #     else:
            #         raise ValueError("DATEADD error: Unsupported time unit. Use 'day', 'month', or 'year'")

            #     # Step 4: Normalize the new date column (remove time for consistent filtering)
            #     temp_df[new_col_name] = temp_df[new_col_name].dt.normalize()

            
            #     print("DATEADD applied — preview:")
            #     print(temp_df[[col, new_col_name]].dropna().head(10))
            #     print("Nulls in source column:", temp_df[col].isna().sum())
            #     print("Nulls in new column:", temp_df[new_col_name].isna().sum())



            # elif calc_formula.lower().startswith("formatdate"):
            #     match = re.match(r'formatdate\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\'](.+?)["\']\s*\)', calc_formula, re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid FORMATDATE format.")
                
            #     col = match.group(1) or match.group(2)
            #     fmt = match.group(3)

            #     temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
            #     # temp_df[new_col_name] = temp_df[col].dt.strftime(fmt)
            #     temp_df[new_col_name] = temp_df[col].dt.strftime(fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d"))



            # elif calc_formula.lower().startswith("replace"):
            #     match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)', calc_formula, re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid REPLACE format.")
            #     col, old, new = match.groups()
            #     temp_df[new_col_name] = temp_df[col].astype(str).str.replace(old, new, regex=False)
            # # round_match = re.match(r'round\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)', calculation, re.IGNORECASE)
            # elif calc_formula.lower().startswith("round"):
            #     # Match round formula: round(<expression>, <decimals>)
            #     match = re.match(r'round\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)', calc_formula, re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid ROUND format. Use round([col], decimals) or round([col1]/[col2], decimals)")
                
            #     expr, decimals = match.groups()
            #     decimals = int(decimals)

            #     # Replace [column] with numeric dataframe references
            #     def replace_column(match):
            #         col_name = match.group(1)
            #         if col_name not in temp_df.columns:
            #             raise ValueError(f"Missing column: {col_name}")
            #         # Convert to numeric
            #         temp_df[col_name] = pd.to_numeric(temp_df[col_name], errors='coerce')
            #         return f"temp_df['{col_name}']"

            #     expr_python = re.sub(r'\[([^\]]+)\]', replace_column, expr)

            #     # Handle division by zero safely
            #     expr_python = re.sub(
            #         r"temp_df\['([^']+)'\]\s*/\s*temp_df\['([^']+)'\]",
            #         r"np.divide(temp_df['\1'], temp_df['\2'].replace(0, np.nan))",
            #         expr_python
            #     )

            #     # Evaluate the expression safely and round
            #     try:
            #         temp_df[new_col_name] = np.round(eval(expr_python), decimals)
            #     except Exception as e:
            #         print("Error evaluating ROUND formula:", e)
            #         temp_df[new_col_name] = np.nan

            # elif calc_formula.lower().startswith("trim"):
            #     match = re.match(r'trim\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
            #     if not match:
            #         raise ValueError("Invalid TRIM format.")
            #     col = match.group(1)
            #     temp_df[new_col_name] = temp_df[col].astype(str).str.strip()
            # # Case 5: Math formula like [A] * [B] - [C]
            # elif calc_formula.lower().startswith(("sum", "avg", "min", "max")):
            #     match = re.match(
            #         r'(sum|avg|min|max)\s*\(\s*\[([^\]]+)\]\s*\)',
            #         calc_formula,
            #         re.IGNORECASE
            #     )
            #     if not match:
            #         raise ValueError("Invalid aggregation format.")

            #     agg_func, col = match.groups()
            #     agg_func = agg_func.lower()

            #     # DO NOT create calculated column
            #     # Just mark aggregation intent
            #     aggregation = agg_func
            #     y_base_column = col

            #     print(f"Detected aggregation: {aggregation}({col})")

            #     # IMPORTANT: skip dataframe eval
            #     skip_calculated_column = True

            # else:
            #     # calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
            #     # print("Evaluating math formula:", calc_formula_python)
            #     # Replace column references [col] → temp_df['col']
            #     calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)

            #     # Handle COUNT
            #     calc_formula_python = re.sub(
            #         r'count\s*\(\s*(temp_df\[.*?\])\s*\)',
            #         r'\1.count()',
            #         calc_formula_python,
            #         flags=re.IGNORECASE
            #     )
            #     # Handle DISTINCT COUNT
            #     calc_formula_python = re.sub(
            #         r'distinct count\s*\(\s*(temp_df\[.*?\])\s*\)',
            #         r'\1.nunique()',
            #         calc_formula_python,
            #         flags=re.IGNORECASE
            #     )   

            #     # Handle SUM
            #     calc_formula_python = re.sub(
            #         r'sum\s*\(\s*(temp_df\[.*?\])\s*\)',
            #         r'\1.sum()',
            #         calc_formula_python,
            #         flags=re.IGNORECASE
            #     )

            #     # Handle AVG
            #     calc_formula_python = re.sub(
            #         r'avg\s*\(\s*(temp_df\[.*?\])\s*\)',
            #         r'\1.mean()',
            #         calc_formula_python,
            #         flags=re.IGNORECASE
            #     )

            #     print("Evaluating math formula:", calc_formula_python)
            #     temp_df[new_col_name] = eval(calc_formula_python)

                # temp_df[new_col_name] = eval(calc_formula_python)

            # print(f"New column '{new_col_name}' created.")
            # y_axis_column = [new_col_name]
            if y_axis_column:
                y_axis_column = [new_col_name if col == replace_col else col for col in y_axis_column]
            if x_axis_columns:
                x_axis_columns = [new_col_name if col == replace_col else col for col in x_axis_columns]



    # Apply filters
    if isinstance(filter_options, str):
        filter_options = json.loads(filter_options)
    for col, filters in filter_options.items():
        if col in temp_df.columns:
            if temp_df[col].dtype != 'datetime64[ns]':  
                temp_df[col] = temp_df[col].astype(str)

        # if col in temp_df.columns:
        #     temp_df[col] = temp_df[col].astype(str)
                filters = list(map(str, filters))  # <-- convert filter values to string too
                temp_df = temp_df[temp_df[col].isin(filters)]

    # ============== DATE GRANULARITY PROCESSING ==============
    # Handle date granularity - convert date columns to specified granularity
    if dateGranularity and isinstance(dateGranularity, dict):
        for date_col, granularity in dateGranularity.items():
            if date_col in temp_df.columns and date_col in x_axis_columns:
                print(f"Applying date granularity: {date_col} -> {granularity}")
                
                # Ensure the column is datetime
                # temp_df[date_col] = pd.to_datetime(temp_df[date_col], errors='coerce')
                temp_df[date_col] = pd.to_datetime(temp_df[date_col], errors='coerce', dayfirst=True)

                
                # Create new column name for the granularity
                granularity_col = f"{date_col}_{granularity}"
                
                # Extract based on granularity
                granularity_lower = granularity.lower()
                if granularity_lower == 'year':
                    temp_df[granularity_col] = temp_df[date_col].dt.year.astype(str)
                elif granularity_lower == 'quarter':
                    temp_df[granularity_col] = 'Q' + temp_df[date_col].dt.quarter.astype(str)
                elif granularity_lower == 'month':
                    # Extract month as full name (January, February, etc.)
                    temp_df[granularity_col] = temp_df[date_col].dt.strftime('%B')
                elif granularity_lower == 'week':
                    # Week number with year (e.g., "Week 1", "Week 2")
                    temp_df[granularity_col] = 'Week ' + temp_df[date_col].dt.isocalendar().week.astype(str)
                elif granularity_lower == 'day':
                    temp_df[granularity_col] = temp_df[date_col].dt.date.astype(str)
                # FORMAT X-AXIS DATE PROPERLY AND MAKE SURE GROUPING WORKS
                # if granularity_lower == 'year':
                #     temp_df[granularity_col] = temp_df[date_col].dt.to_period("Y").dt.to_timestamp()

                # elif granularity_lower == 'quarter':
                #     temp_df[granularity_col] = temp_df[date_col].dt.to_period("Q").dt.to_timestamp()

                # elif granularity_lower == 'month':
                #     temp_df[granularity_col] = temp_df[date_col].dt.to_period("M").dt.to_timestamp()

                # elif granularity_lower == 'week':
                #     temp_df[granularity_col] = temp_df[date_col].dt.to_period("W").dt.to_timestamp()

                # elif granularity_lower == 'day':
                #     temp_df[granularity_col] = temp_df[date_col].dt.normalize()

                else:
                    raise ValueError(f"Unsupported date granularity: {granularity}")
                
                # Replace the date column in x_axis_columns with the granularity column
                x_axis_columns = [granularity_col if col == date_col else col for col in x_axis_columns]
                
                print(f"Created granularity column '{granularity_col}' from '{date_col}'")
                print(f"Sample values: {temp_df[granularity_col].head()}")
    # ============== END DATE GRANULARITY PROCESSING ==============
        # ========== APPLY FILTERS AFTER GRANULARITY ==========
    for col, values in filter_options.items():
        values = list(map(str, values))

        # If granularity column exists, filter on it
        gran_col = None
        if dateGranularity and col in dateGranularity:
            gran_col = f"{col}_{dateGranularity[col]}"

        if gran_col and gran_col in temp_df.columns:
            temp_df = temp_df[temp_df[gran_col].isin(values)]
        elif col in temp_df.columns:
            temp_df[col] = temp_df[col].astype(str)
            temp_df = temp_df[temp_df[col].isin(values)]


    x_axis_columns_str = x_axis_columns

    # Build filter options for x-axis
    options = []
    for col in x_axis_columns:
        if col in filter_options:
            options.extend(filter_options[col])
    options = list(map(str, options))
    # print("options:", options)

    # Filter again based on x-axis
    if options:
        filtered_df = temp_df[temp_df[x_axis_columns[0]].isin(options)]
    else:
        filtered_df = temp_df
    if isinstance(y_axis_column[0], dict):
        y_axis_column = y_axis_column[0].get('column')
    else:
        y_axis_column = y_axis_column[0]
    if skip_calculated_column:
        y_axis_column = y_base_column

        # Perform aggregation
    if aggregation.lower() == "sum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column].sum().reset_index()
    elif aggregation.lower() in ("avg", "average", "mean"):
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column].mean().reset_index()
    elif aggregation.lower() == "count":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0]).size().reset_index(name="count")
    elif aggregation.lower() == "distinct count":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column].nunique().reset_index(name="distinct_count")
    elif aggregation.lower() in ("max", "maximum"):
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column].max().reset_index()
    elif aggregation.lower() in ("min", "minimum"):
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column].min().reset_index()
    elif aggregation.lower() == "variance":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column].var().reset_index()
    else:
        raise ValueError(f"Unsupported aggregation type: {aggregation}")


    result = [tuple(x) for x in grouped_df.to_numpy()]
    return result
# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     and_mask = pd.Series(True, index=df.index)
#     or_mask = pd.Series(False, index=df.index)

#     has_and = False
#     has_or = False

#     for col, filter_data in filter_options.items():
#         if col not in df.columns:
#             continue

#         # Normalize
#         if isinstance(filter_data, dict):
#             values = filter_data.get("values", [])
#             operator = filter_data.get("operator", "AND").upper()
#         else:
#             values = filter_data
#             operator = "AND"

#         if not values:
#             continue

#         # Date handling
#         is_date_col = (
#             pd.api.types.is_datetime64_any_dtype(df[col])
#             or "date" in col.lower()
#         )

#         if is_date_col:
#             temp_dates = pd.to_datetime(df[col], errors="coerce")
#             sample_val = str(values[0])

#             if sample_val.isdigit():  # YEAR
#                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])

#             elif sample_val.startswith("Q"):  # QUARTER
#                 col_mask = temp_dates.dt.quarter.isin(
#                     [int(v.replace("Q", "")) for v in values]
#                 )

#             else:  # DATE STRING
#                 col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
#         else:
#             df[col] = df[col].astype(str).str.strip()
#             values = [str(v).strip() for v in values]
#             col_mask = df[col].isin(values)

#         # 🔥 Apply operator
#         if operator == "OR":
#             or_mask |= col_mask
#             has_or = True
#         else:
#             and_mask &= col_mask
#             has_and = True

#     # 🔥 FINAL DECISION LOGIC
#     if has_and and has_or:
#         return df[and_mask & or_mask]
#     elif has_or:
#         return df[or_mask]          # ✅ FIX: pure OR
#     else:
#         return df[and_mask]
# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     final_mask = pd.Series(False, index=df.index)
#     current_group_mask = pd.Series(True, index=df.index)

#     for col, filter_data in filter_options.items():
#         if col not in df.columns:
#             continue

#         # Normalize
#         if isinstance(filter_data, dict):
#             values = filter_data.get("values", [])
#             operator = filter_data.get("operator", "AND").upper()
#         else:
#             values = filter_data
#             operator = "AND"

#         if not values:
#             continue

#         # Date handling
#         is_date_col = (
#             pd.api.types.is_datetime64_any_dtype(df[col])
#             or "date" in col.lower()
#         )

#         if is_date_col:
#             temp_dates = pd.to_datetime(df[col], errors="coerce")
#             sample_val = str(values[0])

#             if sample_val.isdigit():  # YEAR
#                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])
#             elif sample_val.startswith("Q"):  # QUARTER
#                 col_mask = temp_dates.dt.quarter.isin(
#                     [int(v.replace("Q", "")) for v in values]
#                 )
#             else:  # DATE STRING
#                 col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
#         else:
#             df[col] = df[col].astype(str).str.strip()
#             values = [str(v).strip() for v in values]
#             col_mask = df[col].isin(values)

#         # 🔥 CORE LOGIC (THIS IS THE FIX)
#         current_group_mask &= col_mask

#         if operator == "OR":
#             final_mask |= current_group_mask
#             current_group_mask = pd.Series(True, index=df.index)

#     # 🔥 ADD LAST GROUP
#     final_mask |= current_group_mask

#     return df[final_mask]
# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     and_mask = pd.Series(True, index=df.index)
#     or_mask = pd.Series(False, index=df.index)
#     has_or = False

#     for col, filter_data in filter_options.items():
#         if col not in df.columns:
#             continue

#         # Normalize
#         if isinstance(filter_data, dict):
#             values = filter_data.get("values", [])
#             operator = filter_data.get("operator", "AND").upper()
#         else:
#             values = filter_data
#             operator = "AND"

#         if not values:
#             continue

#         # Date handling
#         if pd.api.types.is_datetime64_any_dtype(df[col]) or "date" in col.lower():
#             temp_dates = pd.to_datetime(df[col], errors="coerce")
#             sample_val = str(values[0])

#             if sample_val.isdigit():  # YEAR
#                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])
#             elif sample_val.startswith("Q"):  # QUARTER
#                 col_mask = temp_dates.dt.quarter.isin(
#                     [int(v.replace("Q", "")) for v in values]
#                 )
#             else:  # FULL DATE
#                 col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
#         else:
#             df[col] = df[col].astype(str).str.strip()
#             values = [str(v).strip() for v in values]
#             col_mask = df[col].isin(values)

#         # 🔥 SQL-ACCURATE LOGIC
#         if operator == "OR":
#             or_mask |= col_mask
#             has_or = True
#         else:
#             and_mask &= col_mask

#     # ✅ FINAL SQL-EQUIVALENT RESULT
#     if has_or:
#         return df[and_mask & or_mask]
#     else:
#         return df[and_mask]

# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     and_mask = pd.Series(True, index=df.index)
#     or_mask = pd.Series(False, index=df.index)

#     # Identify OR columns
#     or_columns = [col for col, val in filter_options.items()
#                   if isinstance(val, dict) and val.get("operator", "AND").upper() == "OR"]

#     for col, val in filter_options.items():
#         if col not in df.columns:
#             continue

#         # Normalize
#         if isinstance(val, dict):
#             values = val.get("values", [])
#             operator = val.get("operator", "AND").upper()
#         else:
#             values = val
#             operator = "AND"

#         if not values:
#             continue

#         # Make string comparison
#         df[col] = df[col].astype(str).str.strip()
#         values = [str(v).strip() for v in values]
#         mask = df[col].isin(values)

#         if col in or_columns:
#             or_mask |= mask
#         else:
#             and_mask &= mask

#     final_mask = and_mask & (or_mask if or_mask.any() else pd.Series(True, index=df.index))
#     return df[final_mask]
# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     and_mask = pd.Series(True, index=df.index)
#     or_mask_list = []

#     for col, val in filter_options.items():
#         if col not in df.columns:
#             continue

#         # Normalize values and operator
#         if isinstance(val, dict):
#             values = val.get("values", [])
#             operator = val.get("operator", "AND").upper()
#         else:
#             values = val
#             operator = "AND"

#         if not values:
#             continue

#         # Ensure string comparison
#         df[col] = df[col].astype(str).str.strip()
#         values = [str(v).strip() for v in values]
#         mask = df[col].isin(values)

#         if operator == "OR":
#             or_mask_list.append(mask)
#         else:
#             and_mask &= mask

#     # Combine OR masks
#     if or_mask_list:
#         combined_or_mask = pd.Series(False, index=df.index)
#         for m in or_mask_list:
#             combined_or_mask |= m
#     else:
#         combined_or_mask = pd.Series(True, index=df.index)

#     final_mask = and_mask & combined_or_mask
#     return df[final_mask]

# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     or_masks = []
#     post_and_mask = pd.Series(True, index=df.index)

#     for col, val in filter_options.items():
#         if col not in df.columns:
#             continue

#         if isinstance(val, dict):
#             values = val.get("values", [])
#             operator = val.get("operator", "AND").upper()
#         else:
#             values = val
#             operator = "AND"

#         if not values:
#             continue

#         df[col] = df[col].astype(str).str.strip()
#         values = [str(v).strip() for v in values]

#         mask = df[col].isin(values)

#         if operator == "OR":
#             or_masks.append(mask)
#         else:
#             post_and_mask &= mask   # ← apply AFTER OR

#     # Combine OR group
#     if or_masks:
#         or_mask = or_masks[0]
#         for m in or_masks[1:]:
#             or_mask |= m
#     else:
#         or_mask = pd.Series(True, index=df.index)

#     final_mask = or_mask & post_and_mask
#     return df[final_mask]

def apply_and_or_filters(df: pd.DataFrame, filter_options: dict) -> pd.DataFrame:
    """
    Apply AND/OR filters on a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to filter.
        filter_options (dict): Dictionary of filters.
            Example:
            {
                'country': ['Albania', 'India'],                  # AND group
                'region': {'values': ['Asia'], 'operator': 'OR'}, # OR group
                'product': {'values': ['Debit card'], 'operator': 'AND'}
            }

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if not filter_options or not isinstance(filter_options, dict):
        return df

    print("filter_options:", filter_options)

    # Initialize masks
    and_mask = pd.Series(True, index=df.index)
    or_mask = pd.Series(False, index=df.index)

    # Detect if ANY OR exists
    has_or = any(
        isinstance(v, dict) and v.get("operator", "").upper() == "OR"
        for v in filter_options.values()
    )

    for col, filter_data in filter_options.items():
        if col not in df.columns:
            continue

        # Normalize filter_data
        if isinstance(filter_data, dict):
            values = filter_data.get("values", [])
            operator = filter_data.get("operator", "AND").upper()
        else:
            values = filter_data
            operator = "AND"

        if not values:
            continue

        # Build column mask
        if pd.api.types.is_datetime64_any_dtype(df[col]) or "date" in col.lower():
            temp_dates = pd.to_datetime(df[col], errors="coerce")
            sample_val = str(values[0])

            if sample_val.isdigit():
                col_mask = temp_dates.dt.year.isin([int(v) for v in values])
            elif sample_val.startswith("Q"):
                col_mask = temp_dates.dt.quarter.isin(
                    [int(v.replace("Q", "")) for v in values]
                )
            else:
                col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
        else:
            col_mask = df[col].astype(str).isin(map(str, values))

        # ✅ AUTO GROUPING: OR group if any OR exists and column is not 'country'
        if has_or and operator in ("AND", "OR") and col != "country":
            or_mask |= col_mask
        else:
            and_mask &= col_mask

    # ✅ FINAL MASK APPLICATION
    if has_or:
        df_filtered = df[or_mask & and_mask]
    else:
        df_filtered = df[and_mask]

    print("df after filter:", df_filtered)
    return df_filtered

def fetch_data_tree(table_name, x_axis_columns, filter_options, y_axis_column, aggregation, db_name, selectedUser,calculationData):
    import pandas as pd
    import json
    import psycopg2

    print("Tree ")
    print("table_name:", table_name)
    print("x_axis_columns:", x_axis_columns)
    print("y_axis_column:", y_axis_column)
    print("aggregation:", aggregation)
    print("filter_options:", filter_options)
    

    try:
        if isinstance(filter_options, str):
            try:
                filter_options = json.loads(filter_options)
            except json.JSONDecodeError:
                raise ValueError("filter_options must be a valid JSON object")

        if not isinstance(filter_options, dict):
            raise ValueError("filter_options should be a dictionary")

        # Fetch data from database
        print("Fetching data from the database...")
        connection = get_db_connection_or_path(selectedUser, db_name)
        print("connection..........")
        cur = connection.cursor()
        query = f'SELECT * FROM "{table_name}"'
        print("query:", query)
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=colnames)
        cur.close()
        connection.close()
        print(f"Fetched {len(df)} rows from table {table_name}")
        # Work on a copy
        temp_df = df.copy()
        print("temp_df", temp_df)
        # Handle calculation logic
        # if calculationData and calculationData.get('calculation') and calculationData.get('columnName'):
        if calculationData and isinstance(calculationData, list):
            for calc_entry in calculationData:
                calc_formula = calc_entry.get('calculation', '').strip()
                new_col_name = calc_entry.get('columnName', '').strip()
                replace_col = calc_entry.get('replaceColumn', new_col_name)
                temp_df, x_axis_columns, y_axis_column = apply_all_calculations(
                    temp_df,
                    calculationData,
                    x_axis_columns,
                    y_axis_column
                )
                # if not calc_formula or not new_col_name:
                #     continue  # Skip incomplete entries
                # # Apply only if the column is involved in x or y axis
                # if new_col_name not in (x_axis_columns or []) and new_col_name not in (y_axis_column or []):
                #     continue
                # def replace_column(match):
                #     col_name = match.group(1)
                #     if col_name in temp_df.columns:
                #         # Ensure numeric columns are treated as such for math operations
                #         # This might need refinement based on exact column types and operations
                #         # For string operations, keep as is
                #         if temp_df[col_name].dtype in [np.int64, np.float64]:
                #             return f"temp_df['{col_name}']"
                #         else:
                #             return f"temp_df['{col_name}']" # Treat as string if not numeric
                #     else:
                #         raise ValueError(f"Column '{col_name}' not found in DataFrame for calculation.")
                # if y_axis_column:
                #     y_axis_column = [new_col_name if col == replace_col else col for col in y_axis_column]
                # if x_axis_columns:
                #     x_axis_columns = [new_col_name if col == replace_col else col for col in x_axis_columns]
                #     # Handle "if (...) then ... else ..." expressions
                #     if calc_formula.strip().lower().startswith("if"):
                #         match = (
                #             re.match(
                #                 r"if\s*\(\s*(.+?)\s*\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$",
                #                 calc_formula.strip(),
                #                 re.IGNORECASE
                #             )
                #             or
                #             re.match(
                #                 r"if\s*\(\s*(.+?)\s*,\s*'?(.*?)'?\s*,\s*'?(.*?)'?\s*\)$",
                #                 calc_formula.strip(),
                #                 re.IGNORECASE
                #             )
                #         )
                #         if not match:
                #             raise ValueError("Invalid if-then-else format in calculation.")

                #         condition_expr, then_val, else_val = match.groups()
                #         condition_expr_python = re.sub(r'\[(.*?)\]', replace_column, condition_expr)
                #         then_val = then_val.strip('"').strip("'")
                #         else_val = else_val.strip('"').strip("'")

                #         print("Evaluating formula as np.where:", f"np.where({condition_expr_python}, {then_val}, {else_val})")
                #         temp_df[new_col_name] = np.where(eval(condition_expr_python),f"{then_val}",f"{else_val}")

                #     elif calc_formula.lower().startswith("switch"):
                #         switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calc_formula, re.IGNORECASE)
                #         if not switch_match:
                #             raise ValueError("Invalid SWITCH syntax")

                #         col_name, rest = switch_match.groups()
                #         if col_name not in temp_df.columns:
                #             raise ValueError(f"Column '{col_name}' not found in DataFrame")

                #         cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
                #         default_match = re.search(r'["\']?default["\']?\s*,\s*["\']?(.*?)["\']?\s*$', rest, re.IGNORECASE)
                #         default_value = default_match.group(1) if default_match else None
                #     elif calc_formula.lower().startswith("iferror"):
                #         match = re.match(r"iferror\s*\((.+?)\s*,\s*(.+?)\)", calc_formula.strip(), re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid IFERROR format")

                #         expr, fallback = match.groups()
                #         expr_python = re.sub(r'\[(.*?)\]', replace_column, expr)
                #         fallback = fallback.strip()
                #         print("Evaluating IFERROR formula:", expr_python)

                #         try:
                #             temp_df[new_col_name] = eval(expr_python)
                #             temp_df[new_col_name] = temp_df[new_col_name].fillna(fallback)
                #         except Exception as e:
                #             print("Error in IFERROR eval:", e)
                #             temp_df[new_col_name] = fallback

                #     # Case 4: CALCULATE(SUM([col]), [filter] = 'X')
                #     elif calc_formula.lower().startswith("calculate"):
                #         match = re.match(r"calculate\s*\(\s*(sum|avg|count|max|min)\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*\[([^\]]+)\]\s*=\s*['\"](.*?)['\"]\s*\)", calc_formula.strip(), re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid CALCULATE format")

                #         agg_func, value_col, filter_col, filter_val = match.groups()
                #         print(f"Applying CALCULATE: {agg_func.upper()}({value_col}) WHERE {filter_col} = {filter_val}")

                #         df_filtered = temp_df[temp_df[filter_col] == filter_val]
                #         if agg_func == "sum":
                #             result_val = df_filtered[value_col].astype(float).sum()
                #         elif agg_func == "avg":
                #             result_val = df_filtered[value_col].astype(float).mean()
                #         elif agg_func == "count":
                #             result_val = df_filtered[value_col].count()
                #         elif agg_func == "max":
                #             result_val = df_filtered[value_col].astype(float).max()
                #         elif agg_func == "min":
                #             result_val = df_filtered[value_col].astype(float).min()
                #         else:
                #             raise ValueError("Unsupported aggregate in CALCULATE")

                #         temp_df[new_col_name] = result_val
                #     elif calc_formula.lower().startswith("maxx") or calc_formula.lower().startswith("minx"):
                #         match = re.match(r'(maxx|minx)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid MAXX/MINX syntax.")
                #         func, col = match.groups()
                #         if col not in temp_df.columns:
                #             raise ValueError(f"Column '{col}' not found.")
                #         result_val = temp_df[col].max() if func.lower() == "maxx" else temp_df[col].min()
                #         temp_df[new_col_name] = result_val
                #     elif calc_formula.lower().startswith("abs"):
                #         match = re.match(r'abs\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid ABS syntax.")
                #         col = match.group(1)
                #         if col not in temp_df.columns:
                #             raise ValueError(f"Column '{col}' not found.")
                #         temp_df[new_col_name] = temp_df[col].abs()
                #     elif calc_formula.lower().startswith("len"):
                #         match = re.match(r'len\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', calc_formula, re.IGNORECASE)
                #         col = match.group(1) or match.group(2)
                #         if col not in temp_df.columns:
                #             raise ValueError(f"Column '{col}' not found.")
                #         temp_df[new_col_name] = temp_df[col].astype(str).str.len()
                #     elif calc_formula.lower().startswith("lower"):
                #         match = re.match(r'lower\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                #         col = match.group(1)
                #         temp_df[new_col_name] = temp_df[col].astype(str).str.lower()

                #     elif calc_formula.lower().startswith("upper"):
                #         match = re.match(r'upper\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                #         col = match.group(1)
                #         temp_df[new_col_name] = temp_df[col].astype(str).str.upper()
                #     elif calc_formula.lower().startswith("concat"):
                #         match = re.match(r'concat\s*\((.+)\)', calc_formula, re.IGNORECASE)
                #         if match:
                #             parts = [p.strip() for p in re.split(r',(?![^\[]*\])', match.group(1))]
                #             concat_parts = []
                #             for part in parts:
                #                 if part.startswith('[') and part.endswith(']'):
                #                     col = part[1:-1]
                #                     if col not in temp_df.columns:
                #                         raise ValueError(f"Column '{col}' not found.")
                #                     concat_parts.append(temp_df[col].astype(str))
                #                 else:
                #                     concat_parts.append(part.strip('"').strip("'"))
                #             from functools import reduce
                #             temp_df[new_col_name] = reduce(lambda x, y: x + y, [p if isinstance(p, pd.Series) else pd.Series([p]*len(temp_df)) for p in concat_parts])

                #     elif re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE):
                #         match = re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                #         func, col = match.groups()
                #         if col not in temp_df.columns:
                #             raise ValueError(f"Column '{col}' not found.")
                #         temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
                #         if func.lower() == "year":
                #             temp_df[new_col_name] = temp_df[col].dt.year
                #         elif func.lower() == "month":
                #             temp_df[new_col_name] = temp_df[col].dt.month
                #         elif func.lower() == "day":
                #             temp_df[new_col_name] = temp_df[col].dt.day

                #     elif calc_formula.lower().startswith("isnull"):
                #         match = re.match(r'isnull\s*\(\s*\[([^\]]+)\]\s*,\s*["\']?(.*?)["\']?\s*\)', calc_formula, re.IGNORECASE)
                #         if match:
                #             col, fallback = match.groups()
                #             if col not in temp_df.columns:
                #                 raise ValueError(f"Column '{col}' not found.")
                #             temp_df[new_col_name] = temp_df[col].fillna(fallback)
                #     elif re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', calc_formula, re.IGNORECASE):
                #         match = re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', calc_formula, re.IGNORECASE)
                #         col = match.group(1) or match.group(2)
                #         raw_values = match.group(3)

                #         # Parse the values correctly
                #         cleaned_values = []
                #         for v in raw_values.split(','):
                #             v = v.strip().strip('"').strip("'")
                #             cleaned_values.append(v)

                #         if col not in temp_df.columns:
                #             raise ValueError(f"Column '{col}' not found in DataFrame.")

                #         temp_df[new_col_name] = temp_df[col].isin(cleaned_values)
                #         print("temp_df[new_col_name]",temp_df[new_col_name])
                #     elif calc_formula.lower().startswith("datediff"):
                #         match = re.match(r'datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid DATEDIFF format.")
                #         end_col, start_col = match.groups()
                #         temp_df[end_col] = pd.to_datetime(temp_df[end_col], errors='coerce')
                #         temp_df[start_col] = pd.to_datetime(temp_df[start_col], errors='coerce')
                #         temp_df[new_col_name] = (temp_df[end_col] - temp_df[start_col]).dt.days
                #     elif calc_formula.lower().startswith("today()"):
                #         # Assign today's date (normalized to midnight) to each row
                #         temp_df[new_col_name] = pd.to_datetime(pd.Timestamp.today().normalize())
                #     elif calc_formula.lower().startswith("now()"):
                #         temp_df[new_col_name] = pd.Timestamp.now()
                #     elif calc_formula.lower().startswith("dateadd"):
                #         match = re.match(
                #             r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)',
                #             calc_formula,
                #             re.IGNORECASE
                #         )
                #         if not match:
                #             raise ValueError("Invalid DATEADD format. Use: dateadd([column], number, 'unit')")

                #         col, interval, unit = match.groups()
                #         interval = int(interval)

                #         # Step 1: Ensure the source column exists
                #         if col not in temp_df.columns:
                #             raise ValueError(f"DATEADD error: Column '{col}' not found in dataframe")
                #         # Step 2: Convert to datetime (NaNs will be handled)
                #         temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
                #         # Step 3: Apply the offset
                #         if unit == "day":
                #             temp_df[new_col_name] = temp_df[col] + pd.to_timedelta(interval, unit='d')
                #         elif unit == "month":
                #             temp_df[new_col_name] = temp_df[col] + pd.DateOffset(months=interval)
                #         elif unit == "year":
                #             temp_df[new_col_name] = temp_df[col] + pd.DateOffset(years=interval)
                #         else:
                #             raise ValueError("DATEADD error: Unsupported time unit. Use 'day', 'month', or 'year'")
                #         # Step 4: Normalize the new date column (remove time for consistent filtering)
                #         temp_df[new_col_name] = temp_df[new_col_name].dt.normalize()                    
                #         print("DATEADD applied — preview:")
                #         print(temp_df[[col, new_col_name]].dropna().head(10))
                #         print("Nulls in source column:", temp_df[col].isna().sum())
                #         print("Nulls in new column:", temp_df[new_col_name].isna().sum())
                #     elif calc_formula.lower().startswith("formatdate"):
                #         match = re.match(r'formatdate\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\'](.+?)["\']\s*\)', calc_formula, re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid FORMATDATE format.")
                #         col = match.group(1) or match.group(2)
                #         fmt = match.group(3)
                #         temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
                #         # temp_df[new_col_name] = temp_df[col].dt.strftime(fmt)
                #         temp_df[new_col_name] = temp_df[col].dt.strftime(fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d"))
                #     elif calc_formula.lower().startswith("replace"):
                #         match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)', calc_formula, re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid REPLACE format.")
                #         col, old, new = match.groups()
                #         temp_df[new_col_name] = temp_df[col].astype(str).str.replace(old, new, regex=False)

                #     elif calc_formula.lower().startswith("trim"):
                #         match = re.match(r'trim\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                #         if not match:
                #             raise ValueError("Invalid TRIM format.")
                #         col = match.group(1)
                #         temp_df[new_col_name] = temp_df[col].astype(str).str.strip()
                #     # Case 5: Math formula like [A] * [B] - [C]
                #     else:
                #         calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
                #         print("Evaluating math formula:", calc_formula_python)
                #         temp_df[new_col_name] = eval(calc_formula_python)
                if y_axis_column:
                    y_axis_column = [new_col_name if col == replace_col else col for col in y_axis_column]
                if x_axis_columns:
                    x_axis_columns = [new_col_name if col == replace_col else col for col in x_axis_columns]
        # Apply filters
        if isinstance(filter_options, str):
            filter_options = json.loads(filter_options)
        for col in x_axis_columns:
            if col in temp_df.columns:
                temp_df[col] = temp_df[col].astype(str)
        filtered_df = apply_and_or_filters(temp_df, filter_options)
        print("filter_options:", filter_options,"filtered_df",filtered_df)
        if y_axis_column and aggregation and y_axis_column[0] in filtered_df.columns:
            if aggregation.lower() == "sum":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].sum()
            elif aggregation.lower() == "average":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].mean()
            elif aggregation.lower() == "minimum":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].min()
            elif aggregation.lower() == "maximum":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].max()
            elif aggregation.lower() == "count":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].count()
            elif aggregation.lower() == "distinct count":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].nunique()
            else:
                raise ValueError("Unsupported aggregation type. Use 'sum', 'avg', 'min', 'max', or 'count'.")

        print("filtered_df:", filtered_df)
        categories = []
        values = []
        for index, row in filtered_df.iterrows():
            category = {col: row[col] for col in x_axis_columns}
            categories.append(category)
            values.append(row[y_axis_column[0]] if y_axis_column else 1)

        result = {
            "categories": categories,
            "values": values,
            "chartType": "treeHierarchy",
            "dataframe": filtered_df.to_dict(orient='records')
        }

        print("result:", result)
        return result

    except Exception as e:
        print("Error preparing Tree Hierarchy data: bar_chart.py file", e)
        return {"error": str(e)}


def drill_down(clicked_category, x_axis_columns, y_axis_column, aggregation):
    global global_df   
    if global_df is None:
        return []
    print(global_df)
    # print("-----------------------------------------------------------------------------------------------------------------------------------------------------",y_axis_column[0])
    global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
    if aggregation == "SUM":
        aggregation_func = "sum"
    elif aggregation == "AVG":
        aggregation_func = "mean"
    elif aggregation == "COUNT":
        aggregation_func = "count"
    elif aggregation == "MAX":
        aggregation_func = "max"
    elif aggregation == "MIN":
        aggregation_func = "min"
    else:
        print("Invalid aggregation type.")
        return []
    filtered_df = global_df[global_df[x_axis_columns[0]] == clicked_category]
    if len(x_axis_columns) > 1:
        target_column = x_axis_columns[1]
    else:
        print("Not enough columns in x_axis_columns for drill down.")
        return []

    grouped_df = filtered_df.groupby(target_column).agg({y_axis_column[0]: aggregation_func}).reset_index()
    result = [tuple(x) for x in grouped_df.to_numpy()]
    
    return result





# def fetch_data_for_duel(table_name, x_axis_columns, filter_options, y_axis_columns, aggregation, db_nameeee, selectedUser, calculationData=None):
#     print("data====================", table_name, x_axis_columns, filter_options, y_axis_columns, aggregation, db_nameeee, selectedUser,calculationData)

    
#     try:
        
#         conn = get_db_connection_or_path(selectedUser, db_nameeee)

#         cur = conn.cursor()

#         # Populate global_df.columns with actual column names from the table
#         # This is crucial for formula parsing functions to know valid column names.
#         global global_df
#         cur.execute(f"SELECT * FROM {table_name} LIMIT 0") # Limit 0 to get schema without fetching data
#         colnames = [desc[0] for desc in cur.description]
#         global_df = pd.DataFrame(columns=colnames)
#         print(f"Schema columns loaded: {global_df.columns.tolist()}")

#         # Initialize filter_clause BEFORE the if block
#         filter_clause = ""
#         # Build WHERE clause
#         if filter_options:
#             where_clauses = []
#             # for col, filters in filter_options.items():
#             #     if col in global_df.columns: # Ensure the column exists in the table schema
#             #         # Ensure filters_str is not empty
#             #         valid_filters = [f for f in filters if f is not None]
#             #         if valid_filters:
#             #             filters_str = ', '.join(["'{}'".format(str(f).replace("'", "''")) for f in valid_filters]) # Convert to string before replace
#             #             where_clauses.append(f'"{col}" IN ({filters_str})')
#             #         else:
#             #             print(f"Warning: No valid filters provided for column '{col}'. Skipping filter.")
#             #     else:
#             #         print(f"Warning: Filter column '{col}' not found in table '{table_name}' schema. Skipping filter.")
#             for col, filters in filter_options.items():
#                 matched_calc = next((calc for calc in calculationData or [] if calc.get("columnName") == col and calc.get("calculation")), None)
#                 valid_filters = [f for f in filters if f is not None]
#                 if not valid_filters:
#                     continue

#                 filters_str = ', '.join(["'{}'".format(str(f).replace("'", "''")) for f in valid_filters])

#                 if matched_calc:
#                     try:
#                         raw_formula = matched_calc["calculation"].strip()
#                         formula_sql = convert_calculation_to_sql(raw_formula, dataframe_columns=global_df.columns.tolist())
#                         where_clauses.append(f'({formula_sql}) IN ({filters_str})')
#                     except Exception as e:
#                         print(f"Error parsing formula for filter column '{col}': {e}")
#                 elif col in global_df.columns:
#                     where_clauses.append(f'"{col}" IN ({filters_str})')
#                 else:
#                     print(f"Warning: Filter column '{col}' not found in table and not calculated. Skipping.")

#             if where_clauses:
#                 filter_clause = "WHERE " + " AND ".join(where_clauses)
#             # else: filter_clause remains "" which is correct

#         # Validate aggregation
#         agg_func = {
#             "sum": "SUM",
#             "average": "AVG",
#             "count": "COUNT",
#             "maximum": "MAX",
#             "minimum": "MIN"
#         }.get(aggregation.lower())
#         if not agg_func:
#             raise ValueError(f"Unsupported aggregation type: {aggregation}")

#         if not x_axis_columns:
#             raise ValueError("x_axis_columns cannot be empty.")

#         x_axis_exprs = []
#         group_by_x_axis_aliases = [] # Store aliases for GROUP BY clause

#         # Move X-axis processing outside any Y-axis loop
#         for x_col in x_axis_columns:
          
#         # select_exprs = []
#             for x_col in x_axis_columns:
#                 matched_calc = next((calc for calc in calculationData or [] if calc.get("columnName") == x_col and calc.get("calculation")), None)
            
#                 if matched_calc:
#                     raw_formula = matched_calc["calculation"].strip()
#                     # raw_formula = calculationData["calculation"].strip()
#                     formula_sql = convert_calculation_to_sql(raw_formula, dataframe_columns=global_df.columns.tolist())
#                     alias_name = f"{x_col}_calculated"
#                     x_axis_exprs.append(f'({formula_sql}) AS "{alias_name}"')
#                     group_by_x_axis_aliases.append(f'"{alias_name}"')
#                 else:
#                     # Include normal column if not in calculation
#                     x_axis_exprs.append(f'"{x_col}"')
#                     group_by_x_axis_aliases.append(f'"{x_col}"')

#         print("y_axis_columns", y_axis_columns)
#         select_x_axis_str = ', '.join(x_axis_exprs)
#         group_by_x_axis_str = ', '.join(group_by_x_axis_aliases)

#         select_exprs = []
#         # Y-axis processing loop
#         for y_col in y_axis_columns:
#             # if calculationData and y_col == calculationData.get("columnName") and calculationData.get("calculation"):
#             matched_calc = next((calc for calc in calculationData or [] if calc.get("columnName") == y_col and calc.get("calculation")), None)
            
#             if matched_calc:
#                 raw_formula = matched_calc["calculation"].strip()

#                 # raw_formula = calculationData["calculation"].strip()
#                 try:
#                     formula_sql = convert_calculation_to_sql(raw_formula, dataframe_columns=global_df.columns.tolist())
#                     alias_name = f"{y_col}_calculated"
#                     print("formula_sql",formula_sql)
#                     # Apply aggregation if not already present
#                     if re.search(r'\b(SUM|AVG|COUNT|MAX|MIN)\b', formula_sql, re.IGNORECASE):
#                         select_exprs.append(f'{formula_sql} AS "{alias_name}"')
#                     else:
#                         select_exprs.append(f'{agg_func}(({formula_sql})::numeric) AS "{alias_name}"')
#                 except Exception as e:
#                     raise ValueError(f"Error parsing formula for {y_col}: {e}")
#             else:
#                 if agg_func == "COUNT":
#                     select_exprs.append(f'{agg_func}("{y_col}") AS "{y_col}"')
#                 else:
#                     select_exprs.append(f'{agg_func}("{y_col}"::numeric) AS "{y_col}"')

        
#         # Final SQL
#         query = f"""
#         SELECT {select_x_axis_str}, {", ".join(select_exprs)}
#         FROM {table_name}
#         {filter_clause}
#         GROUP BY {group_by_x_axis_str};
#         """
#         print("Constructed Query:", cur.mogrify(query).decode('utf-8'))
#         cur.execute(query)
#         rows = cur.fetchall()
#         print("rows", rows)
#         return rows

#     except Exception as e:
#         print(f"An error occurred: {e}")
#         raise # Re-raise the exception after printing
#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()


def fetch_data_for_duel(
    table_name,
    x_axis_columns,
    filter_options,
    y_axis_columns,
    aggregation,
    db_nameeee,
    selectedUser,
    calculationData=None,
    dateGranularity=None
):
    import pandas as pd
    import psycopg2
    import re
        # 🔥 FIX: Convert JSON string → Python dict
    if isinstance(dateGranularity, str):
        try:
            import json
            dateGranularity = json.loads(dateGranularity)
        except:
            dateGranularity = {}


    print("data====================", table_name, x_axis_columns, filter_options, y_axis_columns,
          aggregation, db_nameeee, selectedUser, calculationData, dateGranularity)

    try:
        conn = get_db_connection_or_path(selectedUser, db_nameeee)
        cur = conn.cursor()

        # Load schema for calculation parsing
        cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
        colnames = [desc[0] for desc in cur.description]
        global_df = pd.DataFrame(columns=colnames)
        print(f"Schema columns loaded: {global_df.columns.tolist()}")

        # ---------------------- DATE GRANULARITY (SQL-BASED) ----------------------
        def build_date_granularity_sql(col, gran):
            g = gran.lower()
            alias = f"{col}_{g}"

            if g == "year":
                return f"EXTRACT(YEAR FROM \"{col}\")::text AS \"{alias}\"", f"\"{alias}\""

            elif g == "quarter":
                return f"'Q' || EXTRACT(QUARTER FROM \"{col}\") AS \"{alias}\"", f"\"{alias}\""

            elif g == "month":
                return f"TO_CHAR(\"{col}\", 'Month') AS \"{alias}\"", f"\"{alias}\""

            elif g == "week":
                return f"'Week ' || EXTRACT(WEEK FROM \"{col}\") AS \"{alias}\"", f"\"{alias}\""

            elif g == "day":
                return f"\"{col}\"::date AS \"{alias}\"", f"\"{alias}\""

            else:
                raise ValueError(f"Unsupported date granularity: {gran}")

        # ---------------------- FILTER CLAUSE ----------------------
        # filter_clause = ""
        # if filter_options:
        #     where_clauses = []

        #     for col, filters in filter_options.items():
        #         if not filters:
        #             continue

        #         valid_filters = [f for f in filters if f is not None]
        #         if not valid_filters:
        #             continue

        #         filters_str = ", ".join(["'{}'".format(str(f).replace("'", "''")) for f in valid_filters])

        #         matched_calc = next(
        #             (calc for calc in (calculationData or []) if calc.get("columnName") == col and calc.get("calculation")),
        #             None
        #         )

        #         if matched_calc:
        #             formula_sql = convert_calculation_to_sql(
        #                 matched_calc["calculation"].strip(),
        #                 dataframe_columns=global_df.columns.tolist()
        #             )
        #             where_clauses.append(f"({formula_sql}) IN ({filters_str})")

        #         # elif col in global_df.columns:
        #         #     where_clauses.append(f"\"{col}\" IN ({filters_str})")
        #         elif col in global_df.columns:

        #             # 🔥 If date granularity applied, filter on alias — NOT original column
        #             if dateGranularity and col in dateGranularity:
        #                 gran = dateGranularity[col].lower()
        #                 alias = f"{col}_{gran}"

        #                 if gran == "year":
        #                     where_clauses.append(f"EXTRACT(YEAR FROM \"{col}\")::text IN ({filters_str})")

        #                 elif gran == "quarter":
        #                     where_clauses.append(f"'Q' || EXTRACT(QUARTER FROM \"{col}\") IN ({filters_str})")

        #                 elif gran == "month":
        #                     where_clauses.append(f"TO_CHAR(\"{col}\", 'Month') IN ({filters_str})")

        #                 elif gran == "week":
        #                     where_clauses.append(f"'Week ' || EXTRACT(WEEK FROM \"{col}\") IN ({filters_str})")

        #                 elif gran == "day":
        #                     where_clauses.append(f"\"{col}\"::date IN ({filters_str})")

        #             else:
        #                 where_clauses.append(f"\"{col}\" IN ({filters_str})")


        #     if where_clauses:
        #         filter_clause = "WHERE " + " AND ".join(where_clauses)
        # ---------------------- FILTER CLAUSE ----------------------
        # filter_clause = ""
        # if filter_options:
        #     where_and = []
        #     where_or = []

        #     for col, data in filter_options.items():
        #         if col not in global_df.columns:
        #             continue

        #         # 🔹 Normalize input
        #         if isinstance(data, dict):
        #             values = data.get("values", [])
        #             operator = data.get("operator", "AND").upper()
        #         else:
        #             values = data
        #             operator = "AND"

        #         if not values:
        #             continue

        #         # SQL-safe values
        #         filters_str = ", ".join(["'{}'".format(str(f).replace("'", "''")) for f in values])

        #         # filters_str = ", ".join(
        #         #     f"'{str(v).replace(\"'\", \"''\")}'" for v in values
        #         # )

        #         # 🔹 Check calculated column
        #         matched_calc = next(
        #             (calc for calc in (calculationData or [])
        #             if calc.get("columnName") == col and calc.get("calculation")),
        #             None
        #         )

        #         # 🔹 Build condition
        #         if matched_calc:
        #             formula_sql = convert_calculation_to_sql(
        #                 matched_calc["calculation"].strip(),
        #                 dataframe_columns=global_df.columns.tolist()
        #             )
        #             condition = f"({formula_sql}) IN ({filters_str})"

        #         else:
        #             # 🔹 Date granularity support
        #             if dateGranularity and col in dateGranularity:
        #                 gran = dateGranularity[col].lower()

        #                 if gran == "year":
        #                     condition = f"EXTRACT(YEAR FROM \"{col}\")::text IN ({filters_str})"
        #                 elif gran == "quarter":
        #                     condition = f"'Q' || EXTRACT(QUARTER FROM \"{col}\") IN ({filters_str})"
        #                 elif gran == "month":
        #                     condition = f"TO_CHAR(\"{col}\", 'Month') IN ({filters_str})"
        #                 elif gran == "week":
        #                     condition = f"'Week ' || EXTRACT(WEEK FROM \"{col}\") IN ({filters_str})"
        #                 elif gran == "day":
        #                     condition = f"\"{col}\"::date IN ({filters_str})"
        #                 else:
        #                     continue
        #             else:
        #                 condition = f"\"{col}\" IN ({filters_str})"

        #         # 🔹 Push to AND / OR bucket
        #         if operator == "OR":
        #             where_or.append(condition)
        #         else:
        #             where_and.append(condition)

        #     # 🔹 Final WHERE clause
        #     if where_and and where_or:
        #         filter_clause = (
        #             "WHERE " + " AND ".join(where_and)
        #             + " AND (" + " OR ".join(where_or) + ")"
        #         )
        #     elif where_or:
        #         filter_clause = "WHERE " + " OR ".join(where_or)
        #     elif where_and:
        #         filter_clause = "WHERE " + " AND ".join(where_and)
        filter_clause = ""

        if filter_options:
            and_groups = []          # stores completed AND groups
            current_group = []       # current AND group

            for col, data in filter_options.items():
                if col not in global_df.columns:
                    continue

                # 🔹 Normalize input
                if isinstance(data, dict):
                    values = data.get("values", [])
                    operator = data.get("operator", "AND").upper()
                else:
                    values = data
                    operator = "AND"

                if not values:
                    continue

                # 🔹 SQL-safe values
                filters_str = ", ".join(["'{}'".format(str(f).replace("'", "''")) for f in values])

                # 🔹 Check calculated column
                matched_calc = next(
                    (
                        calc for calc in (calculationData or [])
                        if calc.get("columnName") == col and calc.get("calculation")
                    ),
                    None
                )

                # 🔹 Build condition
                if matched_calc:
                    formula_sql = convert_calculation_to_sql(
                        matched_calc["calculation"].strip(),
                        dataframe_columns=global_df.columns.tolist()
                    )
                    condition = f"({formula_sql}) IN ({filters_str})"
                else:
                    if dateGranularity and col in dateGranularity:
                        gran = dateGranularity[col].lower()

                        if gran == "year":
                            condition = f"EXTRACT(YEAR FROM \"{col}\")::text IN ({filters_str})"
                        elif gran == "quarter":
                            condition = f"'Q' || EXTRACT(QUARTER FROM \"{col}\") IN ({filters_str})"
                        elif gran == "month":
                            condition = f"TO_CHAR(\"{col}\", 'Month') IN ({filters_str})"
                        elif gran == "week":
                            condition = f"'Week ' || EXTRACT(WEEK FROM \"{col}\") IN ({filters_str})"
                        elif gran == "day":
                            condition = f"\"{col}\"::date IN ({filters_str})"
                        else:
                            continue
                    else:
                        condition = f"\"{col}\" IN ({filters_str})"

                # 🔥 ADD to current AND group
                current_group.append(condition)

                # 🔥 CLOSE GROUP on OR
                if operator == "OR":
                    and_groups.append("(" + " AND ".join(current_group) + ")")
                    current_group = []

            # 🔥 ADD LAST GROUP
            if current_group:
                and_groups.append("(" + " AND ".join(current_group) + ")")

            # 🔥 FINAL WHERE
            if and_groups:
                filter_clause = "WHERE " + " OR ".join(and_groups)



        # ---------------------- AGGREGATION ----------------------
        # agg_func = {
        #     "sum": "SUM",
        #     "average": "AVG",
        #     "count": "COUNT",
        #     "maximum": "MAX",
        #     "minimum": "MIN"
        # }.get(aggregation.lower())

        # if not agg_func:
        #     raise ValueError(f"Unsupported aggregation: {aggregation}")

        # ---------------------- X-AXIS EXPRESSIONS ----------------------
        x_axis_exprs = []
        group_by_aliases = []

        for x_col in x_axis_columns:
            # If date granularity applies
            if dateGranularity and x_col in dateGranularity:
                gran = dateGranularity[x_col]
                print(f"Applying date granularity: {x_col} -> {gran}")

                expr, alias = build_date_granularity_sql(x_col, gran)
                x_axis_exprs.append(expr)
                group_by_aliases.append(alias)
                continue

            # Otherwise normal column
            matched_calc = next(
                (calc for calc in (calculationData or []) if calc.get("columnName") == x_col and calc.get("calculation")),
                None
            )

            if matched_calc:
                formula_sql = convert_calculation_to_sql(
                    matched_calc["calculation"].strip(),
                    dataframe_columns=global_df.columns.tolist()
                )
                alias = f"{x_col}_calculated"
                x_axis_exprs.append(f"({formula_sql}) AS \"{alias}\"")
                group_by_aliases.append(f"\"{alias}\"")
            else:
                x_axis_exprs.append(f"\"{x_col}\"")
                group_by_aliases.append(f"\"{x_col}\"")

        if not x_axis_exprs:
            raise ValueError("x_axis_columns cannot be empty.")

        # ---------------------- Y-AXIS EXPRESSIONS ----------------------
        select_exprs = []
        agg_map = {}
        if isinstance(aggregation, list):
            for item in aggregation:
                y = item.get("yAxis")
                a = item.get("aggregation", "sum")
                if y:
                    agg_map[y] = a.lower()


        for y_col in y_axis_columns:
            selected_agg = agg_map.get(y_col, "sum").lower()

            agg_func = {
                "sum": "SUM",
                "average": "AVG",
                "count": "COUNT",
                "distinct count": "COUNT(DISTINCT)",
                "maximum": "MAX",
                "minimum": "MIN"
            }.get(selected_agg, "SUM")

            # detect datatype based on sample row
            if y_col not in global_df.columns:
                matched_calc = next(
                    (calc for calc in (calculationData or []) if calc.get("columnName") == y_col and calc.get("calculation")),
                    None
                )
                if matched_calc:
                    # OK, it’s a calculated column — skip direct SELECT
                    sample_value = 0  # dummy, will cast to numeric later
                    is_numeric = True
                else:
                    raise ValueError(f"Column '{y_col}' does not exist in table '{table_name}'")
            else:
                cur.execute(f'SELECT "{y_col}" FROM {table_name} LIMIT 1')
                sample_value = cur.fetchone()[0]
                is_numeric = True
                try:
                    float(sample_value)
                except Exception:
                    is_numeric = False
            # cur.execute(f'SELECT "{y_col}" FROM {table_name} LIMIT 1')
            # sample_value = cur.fetchone()[0]
            # is_numeric = True
            try:
                float(sample_value)
            except Exception:
                is_numeric = False

            matched_calc = next(
                (calc for calc in (calculationData or []) if calc.get("columnName") == y_col and calc.get("calculation")),
                None
            )


            if matched_calc:
                formula_sql = convert_calculation_to_sql(
                    matched_calc["calculation"].strip(),
                    dataframe_columns=global_df.columns.tolist()
                )
                alias = f"{y_col}_calculated"

                # If formula already contains aggregation
                if re.search(r"\b(SUM|AVG|COUNT|MAX|MIN)\b", formula_sql, re.IGNORECASE):
                    select_exprs.append(f"{formula_sql} AS \"{alias}\"")
                else:
                    # DISTINCT COUNT for calculated column
                    # if agg_func.lower() == "count_distinct":
                    #     select_exprs.append(f"COUNT(DISTINCT ({formula_sql})) AS \"{alias}\"")
                    if selected_agg == "distinct count":
                        select_exprs.append(f"COUNT(DISTINCT ({formula_sql})::numeric) AS \"{alias}\"")

                    elif is_numeric:
                        select_exprs.append(f"{agg_func}(({formula_sql})::numeric) AS \"{alias}\"")
                    else:
                        select_exprs.append(f"COUNT(({formula_sql})) AS \"{alias}\"")

            else:
                # DISTINCT COUNT for normal column
                if selected_agg == "distinct count":
                     select_exprs.append(f"COUNT(DISTINCT \"{y_col}\"::numeric) AS \"{y_col}\"")

                elif is_numeric:
                    select_exprs.append(f"{agg_func}(\"{y_col}\"::numeric) AS \"{y_col}\"")
                else:
                    select_exprs.append(f"COUNT(\"{y_col}\") AS \"{y_col}\"")


            # if matched_calc:
            #     formula_sql = convert_calculation_to_sql(
            #         matched_calc["calculation"].strip(),
            #         dataframe_columns=global_df.columns.tolist()
            #     )
            #     alias = f"{y_col}_calculated"

            #     if re.search(r"\b(SUM|AVG|COUNT|MAX|MIN)\b", formula_sql, re.IGNORECASE):
            #         select_exprs.append(f"{formula_sql} AS \"{alias}\"")
            #     else:
            #         if is_numeric:
            #             select_exprs.append(f"{agg_func}(({formula_sql})::numeric) AS \"{alias}\"")
            #         else:
            #             select_exprs.append(f"COUNT(({formula_sql})) AS \"{alias}\"")

            # else:
            #     if is_numeric:
            #         select_exprs.append(f"{agg_func}(\"{y_col}\"::numeric) AS \"{y_col}\"")
            #     else:
            #         select_exprs.append(f"COUNT(\"{y_col}\") AS \"{y_col}\"")

        # ---------------------- FINAL QUERY ----------------------
        query = f"""
        SELECT {', '.join(x_axis_exprs)}, {', '.join(select_exprs)}
        FROM {table_name}
        {filter_clause}
        GROUP BY {', '.join(group_by_aliases)};
        """

        print("Constructed Query:", cur.mogrify(query).decode("utf-8"))

        cur.execute(query)
        rows = cur.fetchall()
        return rows

    except Exception as e:
        print("❌ Error:", e)
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

import psycopg2
import pandas as pd
import re
import json

def convert_switch_to_case(expression: str) -> str:
    import re

    match = re.match(r'switch\s*\(\s*\[([^\]]+)\](.*)\)', expression.strip(), re.IGNORECASE)
    if not match:
        raise ValueError("Invalid SWITCH expression format")

    col, remainder = match.groups()
    cases = re.findall(r'"([^"]+)"\s*,\s*"([^"]+)"', remainder)
    default_match = re.search(r'default\s*,\s*"([^"]+)"', remainder, re.IGNORECASE)

    if not cases:
        raise ValueError("No valid SWITCH cases found")

    case_sql = f'CASE "{col}"'
    for match_val, result_val in cases:
        case_sql += f" WHEN '{match_val}' THEN '{result_val}'"
    
    if default_match:
        case_sql += f" ELSE '{default_match.group(1)}'"
    else:
        case_sql += " ELSE NULL"
    
    case_sql += " END"
    return case_sql

def convert_if_to_case(formula: str) -> str:
    """
    Converts a formula like: IF([Region] == "Asia" OR [Country] == "India") THEN "A" ELSE "B"
    Into a SQL CASE expression.
    """
    import re

    pattern_if = r'if\s*\(\s*(.+?)\s*\)\s*then\s*[\'"]?(.*?)[\'"]?\s*else\s*[\'"]?(.*?)[\'"]?$'
    match = re.match(pattern_if, formula.strip(), re.IGNORECASE)
    if not match:
        raise ValueError(f"Unsupported IF format: {formula}")

    condition_expr, then_val, else_val = match.groups()

    # Replace [column] with "column" for SQL
    condition_expr_sql = re.sub(r'\[([^\]]+)\]', r'"\1"', condition_expr)

    # Ensure proper SQL syntax (Python uses 'or', SQL uses 'OR')
    condition_expr_sql = condition_expr_sql.replace(" and ", " AND ").replace(" or ", " OR ")
    condition_expr_sql = condition_expr_sql.replace("==", "=")

    # Construct SQL CASE expression
    case_sql = f"CASE WHEN {condition_expr_sql} THEN '{then_val}' ELSE '{else_val}' END"
    return case_sql
# def convert_calculation_to_sql(formula: str, dataframe_columns=None) -> str:
#     formula = formula.strip()
#     # Replace [column] with "column"
#     if dataframe_columns:
#         for col in dataframe_columns:
#             formula = formula.replace(f"[{col}]", f'"{col}"')

#     # # IF condition: if([Region] == "Asia") then "Result1" else "Result2"
#     # if match := re.match(r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$", formula, re.IGNORECASE):
#     #     condition_expr, then_val, else_val = match.groups()
#     #     for col in re.findall(r'\[([^\]]+)\]', condition_expr):
#     #         condition_expr = condition_expr.replace(f"[{col}]", f'"{col}"')
#     #     condition_expr = condition_expr.replace("==","=")
#     #     return f"(CASE WHEN {condition_expr} THEN '{then_val}' ELSE '{else_val}' END)"

#     # match = re.match(
#     #     r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$",
#     #     formula, re.IGNORECASE
#     # )
#     match = (
#         re.match(
#             r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$",
#             formula, re.IGNORECASE
#         )
#         or
#         re.match(
#             r"if\s*\(\s*(.+?)\s*,\s*['\"]?(.*?)['\"]?\s*,\s*['\"]?(.*?)['\"]?\s*\)$",
#             formula, re.IGNORECASE
#         )
#     )

#     if match:
#         condition_expr, then_val, else_val = match.groups()

#         # Replace [col] with "col"
#         for col in re.findall(r'\[([^\]]+)\]', condition_expr):
#             condition_expr = condition_expr.replace(f"[{col}]", f'"{col}"')

#         # ✅ Replace `==` with `=` (must come before quoting literals)
#         condition_expr = condition_expr.replace("==", "=")

#         # ✅ Wrap unquoted string literals with single quotes
#         condition_expr = re.sub(r'=\s*"([^"]+)"', r"= '\1'", condition_expr)  # double-quoted
#         condition_expr = re.sub(r'=\s*([A-Za-z_][A-Za-z0-9_]*)', r"= '\1'", condition_expr)  # bare words

#         return f"(CASE WHEN {condition_expr} THEN '{then_val}' ELSE '{else_val}' END)"
#     # SWITCH
#     if match := re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", formula, re.IGNORECASE):
#         col, rest = match.groups()
#         cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
#         default_match = re.search(r'["\']?default["\']?\s*,\s*["\']?(.*?)["\']?$', rest, re.IGNORECASE)
#         case_sql = "CASE"
#         for val, result in cases:
#             case_sql += f" WHEN \"{col}\" = '{val}' THEN '{result}'"
#         if default_match:
#             case_sql += f" ELSE '{default_match.group(1)}'"
#         case_sql += " END"
#         return case_sql

#     # IFERROR(expr, fallback)
#     if match := re.match(r"iferror\s*\(\s*(.+?)\s*,\s*(.+?)\)", formula, re.IGNORECASE):
#         expr, fallback = match.groups()
#         for col in re.findall(r'\[([^\]]+)\]', expr):
#             expr = expr.replace(f"[{col}]", f'"{col}"')
#         return f"COALESCE({expr}, {fallback})"

#     # MAXX, MINX
#     # if match := re.match(r'(maxx|minx)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#     #     func, col = match.groups()
#     #     return f"{func.upper()}(\"{col}\")"
#     if match := re.match(r'(maxx|minx)\s*\(\s*(.+)\s*\)', formula, re.IGNORECASE):
#         func, expr = match.groups()

#         # Replace [col] → "col"
#         for col in re.findall(r'\[([^\]]+)\]', expr):
#             expr = expr.replace(f'[{col}]', f'"{col}"')

#         sql_func = "MAX" if func.lower() == "maxx" else "MIN"
#         return f"{sql_func}({expr})"

#     # ABS
#     if match := re.match(r'abs\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#         return f"ABS(\"{match.group(1)}\")"

#     # Aggregates (wrapped later if needed)
#     if match := re.match(r'(sum|avg|mean|min|max|count)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#         func, col = match.groups()
#         sql_func = {
#             'sum': 'SUM',
#             'avg': 'AVG',
#             'mean': 'AVG',
#             'min': 'MIN',
#             'max': 'MAX',
#             'count': 'COUNT'
#         }.get(func.lower())
#         return f"{sql_func}(\"{col}\")"

#     # CALCULATE(SUM([column]), [filter_column]="value")
#     # if match := re.match(
#     #     r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*\[([^\]]+)\]\s*=\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE):
#     #     col_to_sum, filter_col, filter_val = match.groups()
#     #     return f"SUM(CASE WHEN \"{filter_col}\" = '{filter_val}' THEN \"{col_to_sum}\" ELSE 0 END)"
#     calc_match = re.match(
#         r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*(.+)\)',
#         formula,
#         re.IGNORECASE
#     )

#     if calc_match:
#         col_to_sum, filters_str = calc_match.groups()

#         # Parse all [col] = "value"
#         filter_pairs = re.findall(
#             r'\[([^\]]+)\]\s*=\s*[\'"](.+?)[\'"]',
#             filters_str
#         )

#         conditions = []
#         for f_col, f_val in filter_pairs:
#             conditions.append(f"\"{f_col}\" = '{f_val}'")

#         where_clause = " AND ".join(conditions) if conditions else "1=1"

#         return f'SUM(CASE WHEN {where_clause} THEN "{col_to_sum}" ELSE 0 END)'


#     # LEN, LOWER, UPPER, CONCAT
#     # if match := re.match(r'len\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#     #     return f"LENGTH(\"{match.group(1)}\")"
#     if match := re.match(r'len\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
#         col = match.group(1) or match.group(2)
#         return f'LENGTH("{col}")'

#     if match := re.match(r'lower\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#         return f"LOWER(\"{match.group(1)}\")"
#     if match := re.match(r'upper\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#         return f"UPPER(\"{match.group(1)}\")"
  
#     concat_match = re.match(r'concat\s*\((.+)\)', formula, re.IGNORECASE)
#     if concat_match:
#         raw_parts = concat_match.group(1)
#         parts = [p.strip() for p in re.split(r',(?![^\[]*\])', raw_parts)]

#         parsed_parts = []
#         for part in parts:
#             if part.startswith('[') and part.endswith(']'):
#                 # ✅ Correct: treat as column name, wrap in double quotes
#                 col_name = part[1:-1].strip()
#                 parsed_parts.append(f'"{col_name}"')
#             else:
#                 # ✅ Correct: treat as string literal
#                 const_str = part.strip('"').strip("'")
#                 parsed_parts.append(f"'{const_str}'")
#         return " || ".join(parsed_parts)
#     # Date functions
#     # if match := re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#     #     func, col = match.groups()
#     #     return f"EXTRACT({func.upper()} FROM \"{col}\"::DATE)"
#     # if match := re.match(r'(year|month|day)\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
#     #     func, col1, col2 = match.groups()
#     #     col = col1 or col2
#     #     return f"EXTRACT({func.upper()} FROM \"{col}\"::DATE)"
#     # if match := re.match(r'(year|month|day)\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
#     #     func, col1, col2 = match.groups()
#     #     col = col1 or col2
#     #     return f'EXTRACT({func.upper()} FROM "{col}"::DATE)'
#     if match := re.match(r'(year|month|day)\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
#         func, col1, col2 = match.groups()
#         col = col1 or col2
#         return f'EXTRACT({func.upper()} FROM "{col}"::DATE)'




#     # ROUND
#     if match := re.match(r'round\s*\(\s*\[([^\]]+)\]\s*,\s*(\d+)\s*\)', formula, re.IGNORECASE):
#         return f"ROUND(\"{match.group(1)}\", {match.group(2)})"

#     # # ISNULL
#     # if match := re.match(r'isnull\s*\(\s*\[([^\]]+)\]\s*,\s*["\']?(.*?)["\']?\s*\)', formula, re.IGNORECASE):
#     #     return f"COALESCE(\"{match.group(1)}\", '{match.group(2)}')"
#     match = re.match(r'isnull\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\']?(.*?)["\']?\s*\)', formula, re.IGNORECASE)
#     if match:
#         col1, col2, fallback = match.groups()
#         col = col1 or col2
#         fallback = fallback.strip()
#         return f'COALESCE("{col}", \'{fallback}\')'


   

#     if match := re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', formula, re.IGNORECASE):
#         col = match.group(1) or match.group(2)
#         values = match.group(3)
#         cleaned_values = []
#         for v in values.split(','):
#             v = v.strip().strip('"').strip("'")
#             cleaned_values.append(f"'{v}'")
#         value_list = ', '.join(cleaned_values)
#         return f'"{col}" IN ({value_list})'
#         # DATEDIFF([end_date], [start_date])
#     if match := re.match(r'datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#         end_col, start_col = match.groups()
#         return f"DATE_PART('day', \"{end_col}\"::timestamp - \"{start_col}\"::timestamp)"

#     # TODAY()
#     if match := re.match(r'today\(\)', formula, re.IGNORECASE):
#         return "CURRENT_DATE"

#     # NOW()
#     if match := re.match(r'now\(\)', formula, re.IGNORECASE):
#         return "CURRENT_TIMESTAMP"

#     # DATEADD([date_column], 7, "day")
#     # if match := re.match(
#     #     r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)', 
#     #     formula, 
#     #     re.IGNORECASE
#     # ):
#     #     col, interval, unit = match.groups()
#     #     interval = int(interval)
#     #     return f'"{col}" + INTERVAL \'{interval} {unit}\''
    
#     # ==== DATEADD([column], interval, "unit") ====
#     if match := re.match(
#         r'dateadd\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)',
#         formula,
#         re.IGNORECASE
#     ):
#         col = match.group(1) or match.group(2)
#         interval = int(match.group(3))
#         unit = match.group(4)

#         return f'CAST("{col}" AS timestamp) + INTERVAL \'{interval} {unit}\''



#     # ===== 2. FORMATDATE =====
#     if match := re.match(
#         r'formatdate\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\'](.+?)["\']\s*\)', 
#         formula, 
#         re.IGNORECASE
#     ):
#         col = match.group(1) or match.group(2)
#         fmt = match.group(3)

#         # Optionally convert JS-like format to PostgreSQL
#         fmt = (
#             fmt.replace("YYYY", "YYYY")
#                .replace("MM", "MM")
#                .replace("DD", "DD")
#         )

#         return f'TO_CHAR("{col}"::timestamp, \'{fmt}\')'

#     # FORMATDATE([date_column], "YYYY-MM-DD")
#     # if match := re.match(r'formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE):
#     #     col, fmt = match.groups()
#     #     return f"TO_CHAR(\"{col}\"::timestamp, '{fmt}')"
#     if match := re.match(r'^\[([^\]]+)\]$', formula.strip()):
#         return f'"{match.group(1)}"'

#     # REPLACE([column], "old", "new")

#     # if match := re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*,\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE):
#     #     col, old_val, new_val = match.groups()
#     #     print("val",old_val,new_val,col)
#     #     return f"REPLACE(\"{col}\", '{old_val}', '{new_val}')"
#     match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)', formula, re.IGNORECASE)
   
#     # match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*,\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE)
#     if match:
#         col, old_val, new_val = match.groups()
#         print("val", old_val, new_val, col)
#         # Use double quotes for column, single quotes for strings
#         return f'REPLACE("{col}", \'{old_val}\', \'{new_val}\')'
      


    






#     # TRIM([column])
#     if match := re.match(r'trim\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#         col = match.group(1)
#         return f"TRIM(\"{col}\")"
#     if match := re.match(r'(sum|avg|min|max)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
#         agg_func, col = match.groups()
#         agg_func = agg_func.lower()
#         # Instead of returning a new column placeholder, return the **base column**
#         # The aggregation will be applied later
#         print(f"Detected aggregation formula: {agg_func}([{col}]) → using base column '{col}'")
#         return col  # <- key fix: return the real column name



#         # return f"\"{col}\" IN ({value_list})"

#     # Arithmetic and logical operations (basic)
#     for col in re.findall(r'\[([^\]]+)\]', formula):
#         formula = formula.replace(f"[{col}]", f'"{col}"')
#         print("formula",formula)
#     return formula  # Default return for generic expressions
def convert_calculation_to_sql(formula: str, dataframe_columns=None) -> str:
    formula = formula.strip()
    # Replace [column] with "column"
    if dataframe_columns:
        for col in dataframe_columns:
            formula = formula.replace(f"[{col}]", f'"{col}"')

    # # IF condition: if([Region] == "Asia") then "Result1" else "Result2"
    # if match := re.match(r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$", formula, re.IGNORECASE):
    #     condition_expr, then_val, else_val = match.groups()
    #     for col in re.findall(r'\[([^\]]+)\]', condition_expr):
    #         condition_expr = condition_expr.replace(f"[{col}]", f'"{col}"')
    #     condition_expr = condition_expr.replace("==","=")
    #     return f"(CASE WHEN {condition_expr} THEN '{then_val}' ELSE '{else_val}' END)"

    # match = re.match(
    #     r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$",
    #     formula, re.IGNORECASE
    # )
    match = (
        re.match(
            r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$",
            formula, re.IGNORECASE
        )
        or
        re.match(
            r"if\s*\(\s*(.+?)\s*,\s*['\"]?(.*?)['\"]?\s*,\s*['\"]?(.*?)['\"]?\s*\)$",
            formula, re.IGNORECASE
        )
    )

    if match:
        condition_expr, then_val, else_val = match.groups()

        # Replace [col] with "col"
        for col in re.findall(r'\[([^\]]+)\]', condition_expr):
            condition_expr = condition_expr.replace(f"[{col}]", f'"{col}"')

        # ✅ Replace `==` with `=` (must come before quoting literals)
        condition_expr = condition_expr.replace("==", "=")

        # ✅ Wrap unquoted string literals with single quotes
        condition_expr = re.sub(r'=\s*"([^"]+)"', r"= '\1'", condition_expr)  # double-quoted
        condition_expr = re.sub(r'=\s*([A-Za-z_][A-Za-z0-9_]*)', r"= '\1'", condition_expr)  # bare words

        return f"(CASE WHEN {condition_expr} THEN '{then_val}' ELSE '{else_val}' END)"
    # SWITCH
    if match := re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", formula, re.IGNORECASE):
        col, rest = match.groups()
        cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
        default_match = re.search(r'["\']?default["\']?\s*,\s*["\']?(.*?)["\']?$', rest, re.IGNORECASE)
        case_sql = "CASE"
        for val, result in cases:
            case_sql += f" WHEN \"{col}\" = '{val}' THEN '{result}'"
        if default_match:
            case_sql += f" ELSE '{default_match.group(1)}'"
        case_sql += " END"
        return case_sql

    # IFERROR(expr, fallback)
    if match := re.match(r"iferror\s*\(\s*(.+?)\s*,\s*(.+?)\)", formula, re.IGNORECASE):
        expr, fallback = match.groups()
        for col in re.findall(r'\[([^\]]+)\]', expr):
            expr = expr.replace(f"[{col}]", f'"{col}"')
        return f"COALESCE({expr}, {fallback})"

    # MAXX, MINX
    # if match := re.match(r'(maxx|minx)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
    #     func, col = match.groups()
    #     return f"{func.upper()}(\"{col}\")"
    # MAXX(expr) / MINX(expr)
    if match := re.match(r'(maxx|minx)\s*\(\s*(.+)\s*\)', formula, re.IGNORECASE):
        func, expr = match.groups()

        # Replace [col] → "col"
        for col in re.findall(r'\[([^\]]+)\]', expr):
            expr = expr.replace(f'[{col}]', f'"{col}"')

        sql_func = "MAX" if func.lower() == "maxx" else "MIN"
        return f"{sql_func}({expr})"


    # ABS
    if match := re.match(r'abs\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
        return f"ABS(\"{match.group(1)}\")"

    # Aggregates (wrapped later if needed)
    if match := re.match(r'(sum|avg|mean|min|max|count)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
        func, col = match.groups()
        sql_func = {
            'sum': 'SUM',
            'avg': 'AVG',
            'mean': 'AVG',
            'min': 'MIN',
            'max': 'MAX',
            'count': 'COUNT'
        }.get(func.lower())
        return f"{sql_func}(\"{col}\")"

    # CALCULATE(SUM([column]), [filter_column]="value")
    # if match := re.match(
    #     r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*\[([^\]]+)\]\s*=\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE):
    #     col_to_sum, filter_col, filter_val = match.groups()
    #     return f"SUM(CASE WHEN \"{filter_col}\" = '{filter_val}' THEN \"{col_to_sum}\" ELSE 0 END)"
    calc_match = re.match(
        r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*(.+)\)',
        formula,
        re.IGNORECASE
    )

    if calc_match:
        col_to_sum, filters_str = calc_match.groups()

        # Parse all [col] = "value"
        filter_pairs = re.findall(
            r'\[([^\]]+)\]\s*=\s*[\'"](.+?)[\'"]',
            filters_str
        )

        conditions = []
        for f_col, f_val in filter_pairs:
            conditions.append(f"\"{f_col}\" = '{f_val}'")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        return f'SUM(CASE WHEN {where_clause} THEN "{col_to_sum}" ELSE 0 END)'


    # LEN, LOWER, UPPER, CONCAT
    # if match := re.match(r'len\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
    #     return f"LENGTH(\"{match.group(1)}\")"
    if match := re.match(r'len\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
        col = match.group(1) or match.group(2)
        return f'LENGTH("{col}")'

    if match := re.match(r'lower\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
        return f"LOWER(\"{match.group(1)}\")"
    if match := re.match(r'upper\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
        return f"UPPER(\"{match.group(1)}\")"
  
    # concat_match = re.match(r'concat\s*\((.+)\)', formula, re.IGNORECASE)
    # if concat_match:
    #     raw_parts = concat_match.group(1)
    #     parts = [p.strip() for p in re.split(r',(?![^\[]*\])', raw_parts)]

    #     parsed_parts = []
    #     for part in parts:
    #         if part.startswith('[') and part.endswith(']'):
    #             # ✅ Correct: treat as column name, wrap in double quotes
    #             col_name = part[1:-1].strip()
    #             parsed_parts.append(f'"{col_name}"')
    #         else:
    #             # ✅ Correct: treat as string literal
    #             const_str = part.strip('"').strip("'")
    #             parsed_parts.append(f"'{const_str}'")
    #     return " || ".join(parsed_parts)
    concat_match = re.match(r'concat\s*\((.+)\)', formula, re.IGNORECASE)
    if concat_match:
        inner = concat_match.group(1)

        # Text functions
        inner = re.sub(
            r'upper\(\s*\[([^\]]+)\]\s*\)',
            r'UPPER("\1")',
            inner, flags=re.IGNORECASE
        )
        inner = re.sub(
            r'lower\(\s*\[([^\]]+)\]\s*\)',
            r'LOWER("\1")',
            inner, flags=re.IGNORECASE
        )
        inner = re.sub(
            r'trim\(\s*\[([^\]]+)\]\s*\)',
            r'TRIM("\1")',
            inner, flags=re.IGNORECASE
        )

        # Split by commas (not inside parentheses)
        parts = [p.strip() for p in re.split(r',(?![^\(\)]*\))', inner)]

        sql_parts = []
        for p in parts:
            if p.startswith('"') and p.endswith('"'):
                sql_parts.append(p)  # already column
            elif re.match(r'^(UPPER|LOWER|TRIM)\(', p, re.IGNORECASE):
                sql_parts.append(p)
            elif p.startswith("'") and p.endswith("'"):
                sql_parts.append(p)
            else:
                # treat as string literal
                val = p.strip('"').strip("'")
                sql_parts.append(f"'{val}'")

        return " || ".join(sql_parts)

    # Date functions
    # if match := re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
    #     func, col = match.groups()
    #     return f"EXTRACT({func.upper()} FROM \"{col}\"::DATE)"
    # if match := re.match(r'(year|month|day)\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
    #     func, col1, col2 = match.groups()
    #     col = col1 or col2
    #     return f"EXTRACT({func.upper()} FROM \"{col}\"::DATE)"
    # if match := re.match(r'(year|month|day)\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
    #     func, col1, col2 = match.groups()
    #     col = col1 or col2
    #     return f'EXTRACT({func.upper()} FROM "{col}"::DATE)'
    if match := re.match(r'(year|month|day)\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', formula, re.IGNORECASE):
        func, col1, col2 = match.groups()
        col = col1 or col2
        return f'EXTRACT({func.upper()} FROM "{col}"::DATE)'




    # ROUND
    if match := re.match(r'round\s*\(\s*\[([^\]]+)\]\s*,\s*(\d+)\s*\)', formula, re.IGNORECASE):
        return f"ROUND(\"{match.group(1)}\", {match.group(2)})"

    # # ISNULL
    # if match := re.match(r'isnull\s*\(\s*\[([^\]]+)\]\s*,\s*["\']?(.*?)["\']?\s*\)', formula, re.IGNORECASE):
    #     return f"COALESCE(\"{match.group(1)}\", '{match.group(2)}')"
    match = re.match(r'isnull\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\']?(.*?)["\']?\s*\)', formula, re.IGNORECASE)
    if match:
        col1, col2, fallback = match.groups()
        col = col1 or col2
        fallback = fallback.strip()
        return f'COALESCE("{col}", \'{fallback}\')'


   

    if match := re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', formula, re.IGNORECASE):
        col = match.group(1) or match.group(2)
        values = match.group(3)
        cleaned_values = []
        for v in values.split(','):
            v = v.strip().strip('"').strip("'")
            cleaned_values.append(f"'{v}'")
        value_list = ', '.join(cleaned_values)
        return f'"{col}" IN ({value_list})'
        # DATEDIFF([end_date], [start_date])
    if match := re.match(r'datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
        end_col, start_col = match.groups()
        return f"DATE_PART('day', \"{end_col}\"::timestamp - \"{start_col}\"::timestamp)"

    # TODAY()
    if match := re.match(r'today\(\)', formula, re.IGNORECASE):
        return "CURRENT_DATE"

    # NOW()
    if match := re.match(r'now\(\)', formula, re.IGNORECASE):
        return "CURRENT_TIMESTAMP"

    # DATEADD([date_column], 7, "day")
    # if match := re.match(
    #     r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)', 
    #     formula, 
    #     re.IGNORECASE
    # ):
    #     col, interval, unit = match.groups()
    #     interval = int(interval)
    #     return f'"{col}" + INTERVAL \'{interval} {unit}\''
    
    # ==== DATEADD([column], interval, "unit") ====
    # if match := re.match(
    #     r'dateadd\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)',
    #     formula,
    #     re.IGNORECASE
    # ):
    #     col = match.group(1) or match.group(2)
    #     interval = int(match.group(3))
    #     unit = match.group(4)

    #     return f'CAST("{col}" AS timestamp) + INTERVAL \'{interval} {unit}\''
    if match := re.match(
        r'dateadd\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year|quarter)["\']\s*\)',
        formula,
        re.IGNORECASE
    ):
        col = match.group(1) or match.group(2)
        interval = int(match.group(3))
        unit = match.group(4).lower()

        if unit == "quarter":
            interval = interval * 3
            unit = "month"

        return f'CAST("{col}" AS timestamp) + INTERVAL \'{interval} {unit}\''




    # ===== 2. FORMATDATE =====
    # if match := re.match(
    #     r'formatdate\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\'](.+?)["\']\s*\)', 
    #     formula, 
    #     re.IGNORECASE
    # ):
    #     col = match.group(1) or match.group(2)
    #     fmt = match.group(3)

    #     # Optionally convert JS-like format to PostgreSQL
    #     fmt = (
    #         fmt.replace("YYYY", "YYYY")
    #            .replace("MM", "MM")
    #            .replace("DD", "DD")
    #     )

    #     return f'TO_CHAR("{col}"::timestamp, \'{fmt}\')'
    if match := re.match(
        r'formatdate\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\'](.+?)["\']\s*\)',
        formula,
        re.IGNORECASE
    ):
        col = match.group(1) or match.group(2)
        fmt = match.group(3)

        # Keep PostgreSQL-style format
        fmt = (
            fmt.replace("YYYY", "YYYY")
            .replace("MMM", "Mon")
            .replace("MM", "MM")
            .replace("DD", "DD")
        )

        return f'TO_CHAR("{col}"::timestamp, \'{fmt}\')'
    def convert_nested_if_sql(expr: str) -> str:
        pattern = r'if\s*\(\s*(.*?)\s*\)\s*then\s*(.*?)\s*else\s*(.*)'
        while re.search(pattern, expr, re.IGNORECASE | re.DOTALL):
            expr = re.sub(
                pattern,
                lambda m: f'(CASE WHEN {m.group(1)} THEN {m.group(2)} ELSE {m.group(3)} END)',
                expr,
                flags=re.IGNORECASE | re.DOTALL
            )
        return expr


    if re.search(r'\bif\s*\(', formula, re.IGNORECASE):
        expr = convert_nested_if_sql(formula)

        # Replace [col] → "col"
        for col in re.findall(r'\[([^\]]+)\]', expr):
            expr = expr.replace(f"[{col}]", f'"{col}"')

        # == → =
        expr = expr.replace("==", "=")

        # Quote string literals after =
        expr = re.sub(r'=\s*"([^"]+)"', r"= '\1'", expr)
        expr = re.sub(r'=\s*([A-Za-z_][A-Za-z0-9_]*)', r"= '\1'", expr)

        return expr


    # FORMATDATE([date_column], "YYYY-MM-DD")
    # if match := re.match(r'formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE):
    #     col, fmt = match.groups()
    #     return f"TO_CHAR(\"{col}\"::timestamp, '{fmt}')"
    if match := re.match(r'^\[([^\]]+)\]$', formula.strip()):
        return f'"{match.group(1)}"'

    # REPLACE([column], "old", "new")

    # if match := re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*,\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE):
    #     col, old_val, new_val = match.groups()
    #     print("val",old_val,new_val,col)
    #     return f"REPLACE(\"{col}\", '{old_val}', '{new_val}')"
    match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)', formula, re.IGNORECASE)
   
    # match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*,\s*["\'](.+?)["\']\s*\)', formula, re.IGNORECASE)
    if match:
        col, old_val, new_val = match.groups()
        print("val", old_val, new_val, col)
        # Use double quotes for column, single quotes for strings
        return f'REPLACE("{col}", \'{old_val}\', \'{new_val}\')'
      


    






    # TRIM([column])
    if match := re.match(r'trim\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
        col = match.group(1)
        return f"TRIM(\"{col}\")"
    if match := re.match(r'(sum|avg|min|max)\s*\(\s*\[([^\]]+)\]\s*\)', formula, re.IGNORECASE):
        agg_func, col = match.groups()
        agg_func = agg_func.lower()
        # Instead of returning a new column placeholder, return the **base column**
        # The aggregation will be applied later
        print(f"Detected aggregation formula: {agg_func}([{col}]) → using base column '{col}'")
        return col  # <- key fix: return the real column name



        # return f"\"{col}\" IN ({value_list})"

    # Arithmetic and logical operations (basic)
    for col in re.findall(r'\[([^\]]+)\]', formula):
        formula = formula.replace(f"[{col}]", f'"{col}"')
        print("formula",formula)
    return formula  # Default return for generic expressions



# def convert_dax_function_to_sql(expression: str) -> str:
#     # Replace MAXX([col]) with MAX("col")
#     expression = re.sub(r'\bMAXX\s*\(\s*(\[.+?\])\s*\)', lambda m: f"MAX({replace_brackets(m.group(1))})", expression, flags=re.IGNORECASE)
#     expression = re.sub(r'\bSUMX\s*\(\s*(\[.+?\])\s*\)', lambda m: f"SUM({replace_brackets(m.group(1))})", expression, flags=re.IGNORECASE)
#     return expression

# def convert_calculate_to_case_sum(expression: str) -> str:
#     # Example: CALCULATE(SUM([Sales]), [Country] = "India")
#     match = re.match(r'calculate\s*\(\s*(sum|avg|max|min|count)\s*\(\s*(\[.+?\])\s*\)\s*,\s*(.+?)\s*\)', expression, re.IGNORECASE)
#     if not match:
#         raise ValueError("Invalid CALCULATE format")

#     agg_func, col, condition = match.groups()
#     col_sql = replace_brackets(col)
#     condition_sql = replace_brackets(condition)
#     return f"{agg_func.upper()}(CASE WHEN {condition_sql} THEN {col_sql} ELSE NULL END)"

# def replace_brackets(expr: str) -> str:
#     # Replace [ColName] with "ColName"::numeric (default)
#     return re.sub(r'\[([^\]]+)\]', r'"\1"::numeric', expr)


# def fetch_data_for_duel_bar(table_name, x_axis_columns, filter_options, y_axis_columns, aggregation, db_nameeee, selectedUser, calculationData=None,dateGranularity=None):
#     # print("data====================", table_name, x_axis_columns, filter_options, y_axis_columns, aggregation, db_nameeee, selectedUser)
#     print("data====================", dateGranularity)
#     conn = get_db_connection_or_path(selectedUser, db_nameeee)
#     cur = conn.cursor()
#     global global_df
#     cur.execute(f"SELECT * FROM {table_name} LIMIT 0") # Limit 0 to get schema without fetching data
#     colnames = [desc[0] for desc in cur.description]
#     global_df = pd.DataFrame(columns=colnames)
#     print(f"Schema columns loaded: {global_df.columns.tolist()}")


#     # Format x-axis
#     x_axis_columns_str = ', '.join(f'"{col}"' for col in x_axis_columns)

#     # Get aggregation function
#     agg_func = {
#         "sum": "SUM",
#         "average": "AVG",
#         "count": "COUNT",
#         "maximum": "MAX",
#         "minimum": "MIN"
#     }.get(aggregation.lower())
#     if not agg_func:
#         raise ValueError(f"Unsupported aggregation type: {aggregation}")

#     # Build WHERE clause
#     filter_clause = ""
#     if filter_options:
#         where_clauses = []
#         # for col, filters in filter_options.items():
#         #     filters_str = ', '.join(f"'{val}'" for val in filters if val is not None)
#         #     where_clauses.append(f'"{col}" IN ({filters_str})')
#         for col, filters in filter_options.items():
#             matched_calc = next((calc for calc in calculationData or [] if calc.get("columnName") == col and calc.get("calculation")), None)
#             # filters_str = ', '.join(f"'{val}'" for val in filters if val is not None)
#             filters_str = ', '.join("'" + str(val).replace("'", "''") + "'" for val in filters if val is not None)

#             if matched_calc:
#                 raw_formula = matched_calc["calculation"].strip()
#                 formula_sql = convert_calculation_to_sql(raw_formula, dataframe_columns=global_df.columns.tolist())
#                 where_clauses.append(f'({formula_sql}) IN ({filters_str})')
#             else:
#                 where_clauses.append(f'"{col}" IN ({filters_str})')

#         if where_clauses:
#             filter_clause = "WHERE " + " AND ".join(where_clauses)

#     y_axis_exprs = []

#     for y_col in y_axis_columns:
#             # if calculationData and y_col == calculationData.get("columnName") and calculationData.get("calculation"):
#             matched_calc = next((calc for calc in calculationData or [] if calc.get("columnName") == y_col and calc.get("calculation")), None)
            
#             if matched_calc:
#                 raw_formula = matched_calc["calculation"].strip()
#                 # raw_formula = calculationData["calculation"].strip()
#                 try:
#                     formula_sql = convert_calculation_to_sql(raw_formula, dataframe_columns=global_df.columns.tolist())
#                     alias_name = f"{y_col}"
#                     print("formula_sql",formula_sql)
#                     # Apply aggregation if not already present
#                     if re.search(r'\b(SUM|AVG|COUNT|MAX|MIN)\b', formula_sql, re.IGNORECASE):
#                         y_axis_exprs.append(f'{formula_sql} AS "{alias_name}"')
#                     else:
#                         y_axis_exprs.append(f'{agg_func}(({formula_sql})::numeric) AS "{alias_name}"')
#                 except Exception as e:
#                     raise ValueError(f"Error parsing formula for {y_col}: {e}")
#             else:
#                 if agg_func == "COUNT":
#                     y_axis_exprs.append(f'{agg_func}("{y_col}") AS "{y_col}"')
#                 else:
#                     y_axis_exprs.append(f'{agg_func}("{y_col}"::numeric) AS "{y_col}"')
#     x_axis_exprs = []
#     for x_col in x_axis_columns:
       
        
#             # if calculationData and x_col == calculationData.get("columnName") and calculationData.get("calculation"):
#             matched_calc = next((calc for calc in calculationData or [] if calc.get("columnName") == x_col and calc.get("calculation")), None)
    
#             if matched_calc:
#                 raw_formula = matched_calc["calculation"].strip()
#                 # raw_formula = calculationData["calculation"].strip()
#                 formula_sql = convert_calculation_to_sql(raw_formula, dataframe_columns=global_df.columns.tolist())
#                 # alias_name = f"{x_col}_calculated"
#                 # x_axis_exprs.append(f'({formula_sql}) AS "{alias_name}"')
#                 x_axis_exprs.append(f'({formula_sql}) AS "{x_col}"')

                
#             else:
#                     # Include normal column if not in calculation
#                 x_axis_exprs.append(f'"{x_col}"')
                   


    
#     select_x_axis_str = ', '.join(x_axis_exprs)
#     # group_by_x_axis_str = ', '.join([f'"{col}_calculated"' if col == calculationData.get("columnName") and calculationData.get("calculation") else f'"{col}"' for col in x_axis_columns])
#     # group_by_x_axis_str = ', '.join([
#     #     f'"{col}_calculated"' if isinstance(calculationData, dict) and col == calculationData.get("columnName") and calculationData.get("calculation") else f'"{col}"'
#     #     for col in x_axis_columns
#     # ])
#     # group_by_x_axis_str = ', '.join([
#     #     f'"{col}_calculated"' if any(calc.get("columnName") == col and calc.get("calculation") for calc in calculationData or []) else f'"{col}"'
#     #     for col in x_axis_columns
#     # ])
#     group_by_x_axis_str = ', '.join(f'"{col}"' for col in x_axis_columns)



#     # Final SQL
#     query = f"""
#     SELECT {select_x_axis_str}, {', '.join(y_axis_exprs)}
#     FROM {table_name}
#     {filter_clause}
#     GROUP BY {group_by_x_axis_str};
#     """

#     print("Constructed Query:", cur.mogrify(query).decode("utf-8"))
#     cur.execute(query)
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()
#     # print("Rows from database:", rows)
#     return rows
def fetch_data_for_duel_bar(
    table_name,
    x_axis_columns,
    filter_options,
    y_axis_columns,
    aggregation,
    db_nameeee,
    selectedUser,
    calculationData=None,
    dateGranularity=None
):
    import pandas as pd
    import psycopg2
    import re
    print("data====================", dateGranularity)

    # 🔥 FIX: Convert JSON string → Python dict
    if isinstance(dateGranularity, str):
        try:
            import json
            dateGranularity = json.loads(dateGranularity)
        except:
            dateGranularity = {}


    conn = get_db_connection_or_path(selectedUser, db_nameeee)
    cur = conn.cursor()

    # Load schema
    cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
    colnames = [desc[0] for desc in cur.description]
    global_df = pd.DataFrame(columns=colnames)
    print(f"Schema columns loaded: {global_df.columns.tolist()}")

    # ------------------------- AGG FUNCTION -------------------------
    agg_func = {
        "sum": "SUM",
        "average": "AVG",
        "count": "COUNT",
        "maximum": "MAX",
        "minimum": "MIN",
        "distinct count": "distinct count"
    }.get(aggregation.lower())

    if not agg_func:
        raise ValueError(f"Unsupported aggregation type: {aggregation}")

    # ------------------------- DATE GRANULARITY -------------------------
    def build_date_granularity_sql(col, gran):
        g = gran.lower()
        alias = f"{col}_{g}"

        if g == "year":
            return f"EXTRACT(YEAR FROM \"{col}\")::text AS \"{alias}\"", alias
        elif g == "quarter":
            return f"'Q' || EXTRACT(QUARTER FROM \"{col}\") AS \"{alias}\"", alias
        elif g == "month":
            return f"TO_CHAR(\"{col}\", 'Month') AS \"{alias}\"", alias
        elif g == "week":
            return f"'Week ' || EXTRACT(WEEK FROM \"{col}\") AS \"{alias}\"", alias
        elif g == "day":
            return f"\"{col}\"::date AS \"{alias}\"", alias
        else:
            raise ValueError(f"Unsupported date granularity: {gran}")

    # ------------------------- WHERE FILTERS -------------------------
    filter_clause = ""

    if filter_options:
        and_groups = []          # stores completed AND groups
        current_group = []       # current AND group

        for col, data in filter_options.items():
            if col not in global_df.columns:
                continue

                # 🔹 Normalize input
            if isinstance(data, dict):
                values = data.get("values", [])
                operator = data.get("operator", "AND").upper()
            else:
                values = data
                operator = "AND"

            if not values:
                continue

                # 🔹 SQL-safe values
            filters_str = ", ".join(["'{}'".format(str(f).replace("'", "''")) for f in values])

                # 🔹 Check calculated column
            matched_calc = next(
                (
                    calc for calc in (calculationData or [])
                    if calc.get("columnName") == col and calc.get("calculation")
                ),
                None
            )

                # 🔹 Build condition
            if matched_calc:
                formula_sql = convert_calculation_to_sql(
                    matched_calc["calculation"].strip(),
                    dataframe_columns=global_df.columns.tolist()
                )
                condition = f"({formula_sql}) IN ({filters_str})"
            else:
                if dateGranularity and col in dateGranularity:
                    gran = dateGranularity[col].lower()

                    if gran == "year":
                        condition = f"EXTRACT(YEAR FROM \"{col}\")::text IN ({filters_str})"
                    elif gran == "quarter":
                        condition = f"'Q' || EXTRACT(QUARTER FROM \"{col}\") IN ({filters_str})"
                    elif gran == "month":
                        condition = f"TO_CHAR(\"{col}\", 'Month') IN ({filters_str})"
                    elif gran == "week":
                        condition = f"'Week ' || EXTRACT(WEEK FROM \"{col}\") IN ({filters_str})"
                    elif gran == "day":
                        condition = f"\"{col}\"::date IN ({filters_str})"
                    else:
                        continue
                else:
                    condition = f"\"{col}\" IN ({filters_str})"

                # 🔥 ADD to current AND group
            current_group.append(condition)

                # 🔥 CLOSE GROUP on OR
            if operator == "OR":
                and_groups.append("(" + " AND ".join(current_group) + ")")
                current_group = []

            # 🔥 ADD LAST GROUP
        if current_group:
            and_groups.append("(" + " AND ".join(current_group) + ")")

            # 🔥 FINAL WHERE
        if and_groups:
            filter_clause = "WHERE " + " OR ".join(and_groups)

    # filter_clause = ""
    # if filter_options:
    #     where_clauses = []

    #     for col, filters in filter_options.items():

    #         matched_calc = next(
    #             (calc for calc in (calculationData or []) if calc.get("columnName") == col and calc.get("calculation")),
    #             None
    #         )

    #         valid_filters = [f for f in filters if f is not None]
    #         filters_str = ", ".join("'" + str(v).replace("'", "''") + "'" for v in valid_filters)

    #         if matched_calc:
    #             fsql = convert_calculation_to_sql(
    #                 matched_calc["calculation"].strip(),
    #                 dataframe_columns=global_df.columns.tolist()
    #             )
    #             where_clauses.append(f"({fsql}) IN ({filters_str})")

    #         elif col in global_df.columns:
    #         #     where_clauses.append(f"\"{col}\" IN ({filters_str})")
    #                         # 🔥 If date granularity applied, filter on alias — NOT original column
    #             if dateGranularity and col in dateGranularity:
    #                 gran = dateGranularity[col].lower()
    #                 alias = f"{col}_{gran}"

    #                 if gran == "year":
    #                     where_clauses.append(f"EXTRACT(YEAR FROM \"{col}\")::text IN ({filters_str})")

    #                 elif gran == "quarter":
    #                     where_clauses.append(f"'Q' || EXTRACT(QUARTER FROM \"{col}\") IN ({filters_str})")

    #                 elif gran == "month":
    #                     where_clauses.append(f"TO_CHAR(\"{col}\", 'Month') IN ({filters_str})")

    #                 elif gran == "week":
    #                     where_clauses.append(f"'Week ' || EXTRACT(WEEK FROM \"{col}\") IN ({filters_str})")

    #                 elif gran == "day":
    #                     where_clauses.append(f"\"{col}\"::date IN ({filters_str})")

    #             else:
    #                 where_clauses.append(f"\"{col}\" IN ({filters_str})")


    #     if where_clauses:
    #         filter_clause = "WHERE " + " AND ".join(where_clauses)

    # ------------------------- Y-AXIS EXPRESSIONS -------------------------
    y_axis_exprs = []

    for y_col in y_axis_columns:
        matched_calc = next(
            (calc for calc in (calculationData or []) if calc.get("columnName") == y_col and calc.get("calculation")),
            None
        )

        # Detect numeric values
        # cur.execute(f'SELECT "{y_col}" FROM {table_name} LIMIT 1')
        # sample_value = cur.fetchone()[0]
        # is_numeric = True
        if y_col not in global_df.columns:
            matched_calc = next(
                (calc for calc in (calculationData or []) if calc.get("columnName") == y_col and calc.get("calculation")),
                None
            )
            if matched_calc:
                # OK, it’s a calculated column — skip direct SELECT
                sample_value = 0  # dummy, will cast to numeric later
                is_numeric = True
            else:
                raise ValueError(f"Column '{y_col}' does not exist in table '{table_name}'")
        else:
            cur.execute(f'SELECT "{y_col}" FROM {table_name} LIMIT 1')
            sample_value = cur.fetchone()[0]
            is_numeric = True
            try:
                float(sample_value)
            except Exception:
                is_numeric = False
        try:
            float(sample_value)
        except:
            is_numeric = False

        if matched_calc:
            formula_sql = convert_calculation_to_sql(
                matched_calc["calculation"].strip(),
                dataframe_columns=global_df.columns.tolist()
            )

            alias = f"{y_col}"

            if re.search(r"\b(SUM|AVG|COUNT|MAX|MIN)\b", formula_sql, re.IGNORECASE):
                y_axis_exprs.append(f"{formula_sql} AS \"{alias}\"")
            else:
                if agg_func == "distinct count":
                    y_axis_exprs.append(f"COUNT(DISTINCT ({formula_sql})::numeric) AS \"{alias}\"")
                elif is_numeric:
                    y_axis_exprs.append(f"{agg_func}(({formula_sql})::numeric) AS \"{alias}\"")
                else:
                    y_axis_exprs.append(f"COUNT(({formula_sql})) AS \"{alias}\"")

        else:
            if agg_func == "distinct count":
                y_axis_exprs.append(f"COUNT(DISTINCT \"{y_col}\"::numeric) AS \"{y_col}\"")
            elif is_numeric:
                y_axis_exprs.append(f"{agg_func}(\"{y_col}\"::numeric) AS \"{y_col}\"")
            else:
                y_axis_exprs.append(f"COUNT(\"{y_col}\") AS \"{y_col}\"")

    # ------------------------- X-AXIS EXPRESSIONS -------------------------
    x_axis_exprs = []
    group_by_aliases = []

    for x_col in x_axis_columns:

        # DATE GRANULARITY
        if dateGranularity and x_col in dateGranularity:
            gran = dateGranularity[x_col]
            print(f"Applying date granularity: {x_col} -> {gran}")

            expr, alias = build_date_granularity_sql(x_col, gran)
            x_axis_exprs.append(expr)
            group_by_aliases.append(f"\"{alias}\"")
            continue

        # CALCULATED X
        matched_calc = next(
            (calc for calc in (calculationData or []) if calc.get("columnName") == x_col and calc.get("calculation")),
            None
        )

        if matched_calc:
            formula_sql = convert_calculation_to_sql(
                matched_calc["calculation"].strip(),
                dataframe_columns=global_df.columns.tolist()
            )
            alias = f"{x_col}"
            x_axis_exprs.append(f"({formula_sql}) AS \"{alias}\"")
            group_by_aliases.append(f"\"{alias}\"")
        else:
            x_axis_exprs.append(f"\"{x_col}\"")
            group_by_aliases.append(f"\"{x_col}\"")

    # ------------------------- FINAL QUERY -------------------------
    query = f"""
    SELECT {', '.join(x_axis_exprs)}, {', '.join(y_axis_exprs)}
    FROM {table_name}
    {filter_clause}
    GROUP BY {', '.join(group_by_aliases)};
    """

    print("Constructed Query:", cur.mogrify(query).decode("utf-8"))

    cur.execute(query)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows




# def fetch_column_name(table_name, x_axis_columns, db_name,calculation_expr,calc_column, selectedUser='null'):
#     """
#     Fetch distinct values for one or more columns.
#     If multiple columns are provided as a comma-separated string,
#     returns a dictionary with each column's distinct values.
#     """
#     print("selectedUser:", selectedUser)
    
#     print("calculationData:", calculation_expr,calc_column)
#     # Establish database connection
#     # try:
#     #     if not selectedUser or selectedUser.lower() == 'null':
#     #         conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
#     #     else:
#     #         connection_details = fetch_external_db_connection(db_name, selectedUser)
#     #         if not connection_details:
#     #             raise Exception("Unable to fetch external database connection details.")
            
#     #         db_details = {
#     #             "host": connection_details[3],
#     #             "database": connection_details[7],
#     #             "user": connection_details[4],
#     #             "password": connection_details[5],
#     #             "port": int(connection_details[6])
#     #         }
#     #         conn = psycopg2.connect(
#     #             dbname=db_details['database'],
#     #             user=db_details['user'],
#     #             password=db_details['password'],
#     #             host=db_details['host'],
#     #             port=db_details['port'],
#     #         )
    

#     try:
#         # ✅ 1️⃣ LOCAL DATABASE CONNECTION
#         conn= get_db_connection_or_path(selectedUser, db_name)
#         results = {}

#         with conn.cursor() as cur:  # ✅ Use a regular cursor instead of a named cursor
#             # Split multiple columns if needed
#             columns = [col.strip() for col in x_axis_columns.split(',')] if ',' in x_axis_columns else [x_axis_columns.strip()]
            
#             for col in columns:
#                 # Check the data type of the column
#                 type_query = """
#                     SELECT data_type FROM information_schema.columns 
#                     WHERE table_name = %s AND column_name = %s
#                 """
#                 cur.execute(type_query, (table_name, col))
#                 column_type = cur.fetchone()

#                 # # Build the SQL query dynamically based on data type
#                 # if column_type and column_type[0] in ('date', 'timestamp', 'timestamp with time zone'):
#                 #     query = sql.SQL("SELECT DISTINCT TO_CHAR({col}, 'YYYY-MM-DD') FROM {table}")
#                 # else:
#                 #     query = sql.SQL("SELECT DISTINCT {col} FROM {table}")

#                 from psycopg2 import sql

               
#                 if calculation_expr and col == calc_column:
#                     try:
#                         raw_formula = calculation_expr.strip()
#                         # calculation_expr_sql = convert_calculation_to_sql(raw_formula, global_df.columns)
#                         # calculation_expr_sql = convert_calculation_to_sql(raw_formula, list(global_df.columns))
#                         calculation_expr_sql = convert_calculation_to_sql(raw_formula, list(global_df.columns))


#                         print("Parsed SQL expression:", calculation_expr_sql)

#                         query = sql.SQL(
#                             "SELECT DISTINCT {alias} FROM (SELECT {calc_expr} AS {alias} FROM {table}) AS sub"
#                         ).format(
#                             calc_expr=sql.SQL(calculation_expr_sql),
#                             alias=sql.Identifier(calc_column),
#                             table=sql.Identifier(table_name)
#                         )
#                         print("Generated SQL query:", query.as_string(conn))
#                         cur.execute(query)

#                     except Exception as e:
#                         print("⚠️ Error parsing or executing calculation expression:", str(e))
#                         results[col] = []
#                         return


#                 else:
#                     # Fallback: direct column fetch
#                     if column_type and column_type[0] in ('date', 'timestamp', 'timestamp with time zone'):
#                         query = sql.SQL("SELECT DISTINCT TO_CHAR({col}, 'YYYY-MM-DD') FROM {table}").format(
#                             col=sql.Identifier(col),
#                             table=sql.Identifier(table_name)
#                         )
#                     else:
#                         query = sql.SQL("SELECT DISTINCT {col} FROM {table}").format(
#                             col=sql.Identifier(col),
#                             table=sql.Identifier(table_name)
#                         )
#                     cur.execute(query)

#                 # Final result processing
#                 rows = cur.fetchall()
#                 results[col] = [row[0] for row in rows]
#                 print("results[col]",results[col])


#         conn.close()
#         return results

#     except Exception as e:
#         raise Exception(f"Error fetching distinct column values from {table_name}: {str(e)}")
def fetch_column_name(table_name, x_axis_columns, db_name, calculation_expr, calc_column, selectedUser='null'):
    """
    Fetch distinct values for one or more columns.
    If multiple columns are provided as a comma-separated string,
    returns a dictionary with each column's distinct values.
    
    **NEW**: If a column is a date type, it returns a dictionary 
    of date parts (years, months, quarters, etc.)
    """
    print("selectedUser:", selectedUser)
    print("calculationData:", calculation_expr, calc_column)
    
    conn = None # Initialize conn
    try:
        # Establish database connection
        if not selectedUser or selectedUser.lower() == 'null':
            conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
        else:
            connection_details = fetch_external_db_connection(db_name, selectedUser)
            if not connection_details:
                raise Exception("Unable to fetch external database connection details.")
            
            db_details = {
                "host": connection_details[3],
                "database": connection_details[7],
                "user": connection_details[4],
                "password": connection_details[5],
                "port": int(connection_details[6])
            }
            conn = psycopg2.connect(
                dbname=db_details['database'],
                user=db_details['user'],
                password=db_details['password'],
                host=db_details['host'],
                port=db_details['port'],
            )

        results = {}

        with conn.cursor() as cur:
            columns = [col.strip() for col in x_axis_columns.split(',')] if ',' in x_axis_columns else [x_axis_columns.strip()]
            
            for col in columns:
                # Check the data type of the column
                type_query = """
                    SELECT data_type FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                """
                cur.execute(type_query, (table_name, col))
                column_type_row = cur.fetchone()
                column_type = column_type_row[0] if column_type_row else None

                if calculation_expr and col == calc_column:
                    try:
                        raw_formula = calculation_expr.strip()
                        # Assuming global_df.columns is available or passed differently
                        # This part might need adjustment if global_df is not defined here
                        # For this example, I'll assume convert_calculation_to_sql is defined
                        calculation_expr_sql = convert_calculation_to_sql(raw_formula, []) 

                        print("Parsed SQL expression:", calculation_expr_sql)

                        query = sql.SQL(
                            "SELECT DISTINCT {alias} FROM (SELECT {calc_expr} AS {alias} FROM {table}) AS sub"
                        ).format(
                            calc_expr=sql.SQL(calculation_expr_sql),
                            alias=sql.Identifier(calc_column),
                            table=sql.Identifier(table_name)
                        )
                        print("Generated SQL query:", query.as_string(conn))
                        cur.execute(query)
                        rows = cur.fetchall()
                        results[col] = [row[0] for row in rows]
                        print("results[col]", results[col])

                    except Exception as e:
                        print("⚠️ Error parsing or executing calculation expression:", str(e))
                        results[col] = [] # Set empty on error
                        continue # Move to the next column

                # --- This is the new/modified block ---
                elif column_type and column_type in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'):
                    print(f"Column {col} is a date type. Fetching date parts.")
                    
                    date_parts_data = {
                        "is_date": True,
                        "all_values": [],  
                        "years": [],
                        "quarters": [],
                        "months": [],       # Numeric (1-12)
                        "weeks": [],        # Week of year (1-53)
                        "day_of_month": [], # (1-31)
                        "day_of_week": [],  # (0=Sun, 6=Sat)
                        "month_names": [],  # e.g., {"num": 1, "name": "January"}
                        "day_names": []     # e.g., {"num": 0, "name": "Sunday"}
                    }
                    try:
                        raw_query = sql.SQL(
                            "SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL ORDER BY 1"
                        ).format(
                            col=sql.Identifier(col),
                            table=sql.Identifier(table_name)
                        )

                        cur.execute(raw_query)
                        rows = cur.fetchall()

                        # Convert to ISO string for frontend safety
                        date_parts_data["all_values"] = [
                            row[0].isoformat() if row[0] else None for row in rows
                        ]
                    except Exception as e:
                        print(f"Error fetching raw date values for {col}: {e}")
                    # (dictionary_key, sql_extract_part)
                    parts_to_query = [
                        ('years', 'YEAR'),
                        ('quarters', 'QUARTER'),
                        ('months', 'MONTH'),
                        ('weeks', 'WEEK'),
                        ('day_of_month', 'DAY'),
                        ('day_of_week', 'DOW')
                    ]

                    for part_key, sql_part in parts_to_query:
                        try:
                            part_query = sql.SQL(
                                "SELECT DISTINCT EXTRACT({sql_part} FROM {col}) "
                                "FROM {table} WHERE {col} IS NOT NULL ORDER BY 1"
                            ).format(
                                sql_part=sql.SQL(sql_part),
                                col=sql.Identifier(col),
                                table=sql.Identifier(table_name)
                            )
                            cur.execute(part_query)
                            rows = cur.fetchall()
                            # EXTRACT returns float-like, so cast to int
                            date_parts_data[part_key] = sorted([int(row[0]) for row in rows])
                        except Exception as e:
                            print(f"Error fetching date part {sql_part} for {col}: {e}")
                            date_parts_data[part_key] = []
                    
                    # Get Month Names
                    try:
                        month_name_query = sql.SQL(
                            "SELECT DISTINCT EXTRACT(MONTH FROM {col}) as month_num, TO_CHAR({col}, 'Month') as month_name "
                            "FROM {table} WHERE {col} IS NOT NULL ORDER BY month_num"
                        ).format(col=sql.Identifier(col), table=sql.Identifier(table_name))
                        cur.execute(month_name_query)
                        rows = cur.fetchall()
                        date_parts_data['month_names'] = [{"num": int(row[0]), "name": row[1].strip()} for row in rows]
                    except Exception as e:
                        print(f"Error fetching month names for {col}: {e}")

                    # Get Day of Week Names
                    try:
                        day_name_query = sql.SQL(
                            "SELECT DISTINCT EXTRACT(DOW FROM {col}) as dow_num, TO_CHAR({col}, 'Day') as day_name "
                            "FROM {table} WHERE {col} IS NOT NULL ORDER BY dow_num"
                        ).format(col=sql.Identifier(col), table=sql.Identifier(table_name))
                        cur.execute(day_name_query)
                        rows = cur.fetchall()
                        date_parts_data['day_names'] = [{"num": int(row[0]), "name": row[1].strip()} for row in rows]
                    except Exception as e:
                        print(f"Error fetching day names for {col}: {e}")

                    results[col] = date_parts_data

                else:
                    # Original logic for non-date, non-calculated columns
                    query = sql.SQL("SELECT DISTINCT {col} FROM {table}").format(
                        col=sql.Identifier(col),
                        table=sql.Identifier(table_name)
                    )
                    cur.execute(query)
                    rows = cur.fetchall()
                    results[col] = [row[0] for row in rows]
                    print("results[col]", results[col])
        
        conn.close()

        print("Final results:", results)
        return results

    except Exception as e:
        if conn:
            conn.close()
        # Ensure to handle undefined variables if connection fails early
        raise Exception(f"Error fetching distinct column values: {str(e)}")



def calculationFetch(db_name, dbTableName='book13', selectedUser=None):
    global global_df

    try:
        if 'global_df' in globals() and global_df is not None and not global_df.empty:
            return global_df

        # if selectedUser is None:
        #     print("Using direct connection to company DB:", db_name)
        #     connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
        #     connection = psycopg2.connect(connection_string)
        # else:
        #     print("Using external DB connection for user:", selectedUser)
        #     connection = fetch_external_db_connection(db_name, selectedUser)
        #     if not connection:
        #         raise Exception("Could not get external DB connection")
        connection = get_db_connection_or_path(selectedUser, db_name)

        cursor = connection.cursor()

        query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(dbTableName))
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=column_names)

        cursor.close()
        connection.close()

        global_df = df
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


    
def segregate_string_pattern(calculation):
    words = re.findall(r'\[([^\]]+)\]', calculation)
    symbols = re.findall(r'[+\-*/]', calculation)
    print("words", words)
    print("symbols", symbols)
    return words, symbols


import numpy as np

import pandas as pd
import numpy as np
import re



import re
import numpy as np
import pandas as pd
import re
import numpy as np
import pandas as pd

# def perform_calculation(dataframe, columnName, calculation):
#     global global_df
#     calculation = calculation.strip()
#     def replace_columns(expr):
#         columns = re.findall(r'\[([^\]]+)\]', expr)
#         for col in columns:
#             if col not in dataframe.columns:
#                 raise ValueError(f"Missing column: {col}")
#             # Force numeric conversion if possible
#             dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')
#             expr = expr.replace(f"[{col}]", f'dataframe["{col}"]')
#         return expr


#     # ========== 1. IF condition ==========
   
#     pattern_then_else = (
#         r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$"
#     )

#     # Pattern 2: if(condition, a, b)
#     pattern_comma = (
#         r"if\s*\(\s*(.+?)\s*,\s*['\"]?(.*?)['\"]?\s*,\s*['\"]?(.*?)['\"]?\s*\)$"
#     )

#     match = (
#         re.match(pattern_then_else, calculation, re.IGNORECASE)
#         or re.match(pattern_comma, calculation, re.IGNORECASE)
#     )
#     # pattern_if = r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$"
#     # match = re.match(pattern_if, calculation, re.IGNORECASE)
#     if match:
#         condition_expr, then_val, else_val = match.groups()
#         print("Eval condition string:", condition_expr)

#         # Replace column references like [region] with dataframe["region"]
#         condition_expr = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', condition_expr)

#         try:
#             result = np.where(eval(condition_expr), then_val, else_val)
#             dataframe[columnName] = result
#             global_df = dataframe
#             return dataframe
#         except Exception as e:
#             raise ValueError(f"Error in IF expression: {str(e)}")

#     # ========== 2. SWITCH ==========
#     switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calculation, re.IGNORECASE)
#     if switch_match:
#         col_name, rest = switch_match.groups()
#         if col_name not in dataframe.columns:
#             raise ValueError(f"Missing column: {col_name}")

#         cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
#         default_match = re.search(r'["\']?default["\']?\s*,\s*["\']?(.*?)["\']?\s*$', rest, re.IGNORECASE)
#         default_value = default_match.group(1) if default_match else None

#         def switch_map(val):
#             for match_val, result in cases:
#                 if str(val) == match_val:
#                     return result
#             return default_value

#         dataframe[columnName] = dataframe[col_name].apply(switch_map)
#         global_df = dataframe
#         return dataframe

#     # ========== 3. IFERROR ==========
#     iferror_match = re.match(r"iferror\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)$", calculation, re.IGNORECASE)
#     if iferror_match:
#         expr, fallback = iferror_match.groups()
#         for col in re.findall(r'\[([^\]]+)\]', expr):
#             if col not in dataframe.columns:
#                 raise ValueError(f"Missing column: {col}")
#             expr = expr.replace(f"[{col}]", f'dataframe["{col}"]')
#         try:
#             result = eval(expr)
#             dataframe[columnName] = np.where(pd.isnull(result) | np.isinf(result), fallback, result)
#             global_df = dataframe
#             return dataframe
#         except Exception:
#             dataframe[columnName] = fallback
#             global_df = dataframe
#             return dataframe

#     # ========== 4. MAXX, MINX ==========
#     match_xx = re.match(r'(maxx|minx)\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if match_xx:
#         func, col = match_xx.groups()
#         col = col.strip()
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         value = dataframe[col].max() if func.lower() == 'maxx' else dataframe[col].min()
#         dataframe[columnName] = value
#         global_df = dataframe
#         return dataframe

#     # ========== 5. ABS ==========
#     abs_match = re.match(r'abs\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if abs_match:
#         col = abs_match.group(1)
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         dataframe[columnName] = dataframe[col].abs()
#         global_df = dataframe
#         return dataframe

#     # ========== 6. Aggregates ==========
#     agg_match = re.match(r'(sum|avg|mean|min|max|count)\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if agg_match:
#         func, col = agg_match.groups()
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         func_map = {
#             'sum': dataframe[col].sum,
#             'avg': dataframe[col].mean,
#             'mean': dataframe[col].mean,
#             'min': dataframe[col].min,
#             'max': dataframe[col].max,
#             'count': dataframe[col].count
#         }
#         try:
#             result = func_map[func.lower()]()
#             dataframe[columnName] = result
#             global_df = dataframe
#             return dataframe
#         except Exception as e:
#             raise ValueError(f"Error in aggregate: {str(e)}")

#     # ========== 7. CALCULATE ==========
#     calc_match = re.match(
#         r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*\[([^\]]+)\]\s*=\s*[\'"](.+?)[\'"]\s*\)',
#         calculation, re.IGNORECASE
#     )
#     if calc_match:
#         col_to_sum, filter_col, filter_val = calc_match.groups()
#         if col_to_sum not in dataframe.columns or filter_col not in dataframe.columns:
#             raise ValueError("Missing column(s) for CALCULATE")
#         filtered_df = dataframe[dataframe[filter_col] == filter_val]
#         sum_result = filtered_df[col_to_sum].sum()
#         dataframe[columnName] = sum_result
#         global_df = dataframe
#         return dataframe

#     # ========== 8. Text Functions ==========
#     len_match = re.match(r'len\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if len_match:
#         col = len_match.group(1)
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         dataframe[columnName] = dataframe[col].astype(str).str.len()
#         global_df = dataframe
#         return dataframe
   

#     lower_match = re.match(r'lower\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if lower_match:
#         col = lower_match.group(1)
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         dataframe[columnName] = dataframe[col].astype(str).str.lower()
#         global_df = dataframe
#         return dataframe

#     upper_match = re.match(r'upper\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if upper_match:
#         col = upper_match.group(1)
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         dataframe[columnName] = dataframe[col].astype(str).str.upper()
#         global_df = dataframe
#         return dataframe
#     concat_match = re.match(r'concat\s*\((.+)\)', calculation, re.IGNORECASE)
#     if concat_match:
#         inner = concat_match.group(1)
#         parts = [p.strip() for p in re.split(r',(?![^\[]*\])', inner)]

#         try:
#             concat_parts = []
#             for p in parts:
#                 if p.startswith('[') and p.endswith(']'):
#                     col = p[1:-1]
#                     if col not in dataframe.columns:
#                         raise ValueError(f"Missing column: {col}")
#                     concat_parts.append(dataframe[col].astype(str))
#                 else:
#                     const_str = p.strip('"').strip("'")
#                     concat_parts.append(const_str)

#             # Use `add` from pandas for efficient row-wise string concat
#             result = concat_parts[0]
#             for part in concat_parts[1:]:
#                 result = result + part

#             dataframe[columnName] = result
#             global_df = dataframe
#             return dataframe

#         except Exception as e:
#             raise ValueError(f"Error in CONCAT expression: {str(e)}")


#     # ========== 9. Date Functions ==========
#     date_match = re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if date_match:
#         func, col = date_match.groups()
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         dataframe[columnName] = pd.to_datetime(dataframe[col], errors='coerce')
#         if func.lower() == 'year':
#             dataframe[columnName] = dataframe[columnName].dt.year
#         elif func.lower() == 'month':
#             dataframe[columnName] = dataframe[columnName].dt.month
#         elif func.lower() == 'day':
#             dataframe[columnName] = dataframe[columnName].dt.day
#         global_df = dataframe
#         return dataframe

#     # ========== 10. ROUND ==========
#     # round_match = re.match(r'round\s*\(\s*\[([^\]]+)\]\s*,\s*(\d+)\s*\)', calculation, re.IGNORECASE)
#     # if round_match:
#     #     col, decimals = round_match.groups()
#     #     if col not in dataframe.columns:
#     #         raise ValueError(f"Missing column: {col}")
#     #     dataframe[columnName] = dataframe[col].round(int(decimals))
#     #     global_df = dataframe
#     #     return dataframe

#     round_match = re.match(r'round\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)', calculation, re.IGNORECASE)
#     if round_match:
#         expr, decimals = round_match.groups()

#         # Replace [column] with numeric dataframe references
#         def replace_column(match):
#             col_name = match.group(1)
#             if col_name not in dataframe.columns:
#                 raise ValueError(f"Missing column: {col_name}")
#             # Convert to numeric (errors='coerce' will turn non-numeric to NaN)
#             dataframe[col_name] = pd.to_numeric(dataframe[col_name], errors='coerce')
#             return f"dataframe['{col_name}']"

#         expr_python = re.sub(r'\[([^\]]+)\]', replace_column, expr)

#         # Evaluate expression
#         dataframe[columnName] = eval(expr_python).round(int(decimals))
#         global_df = dataframe
#         return dataframe

#     # ========== 11. ISNULL ==========
#     isnull_match = re.match(r'isnull\s*\(\s*\[([^\]]+)\]\s*,\s*["\']?(.*?)["\']?\s*\)', calculation, re.IGNORECASE)
#     if isnull_match:
#         col, replacement = isnull_match.groups()
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         dataframe[columnName] = dataframe[col].fillna(replacement)
#         global_df = dataframe
#         return dataframe

#     # ========== 12. IN expression ==========
#     in_match = re.match(r'\[([^\]]+)\]\s+in\s+\((.*?)\)', calculation, re.IGNORECASE)
#     if in_match:
#         col, values = in_match.groups()
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         value_list = [v.strip().strip('"').strip("'") for v in values.split(',')]
#         result = dataframe[col].astype(str).isin(value_list)
#         dataframe[columnName] = result
#         global_df = dataframe
#         return dataframe
#     # ========== 14. DATEDIFF ==========
#     datediff_match = re.match(r'datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if datediff_match:
#         end_col, start_col = datediff_match.groups()
#         if end_col not in dataframe.columns or start_col not in dataframe.columns:
#             raise ValueError("Missing column(s) for DATEDIFF")
#         dataframe[columnName] = (pd.to_datetime(dataframe[end_col]) - pd.to_datetime(dataframe[start_col])).dt.days
#         global_df = dataframe
#         return dataframe

#     # ========== 15. TODAY ==========
#     today_match = re.match(r'today\(\)', calculation, re.IGNORECASE)
#     if today_match:
#         dataframe[columnName] = pd.to_datetime('today').normalize()
#         global_df = dataframe
#         return dataframe

#     # ========== 16. NOW ==========
#     now_match = re.match(r'now\(\)', calculation, re.IGNORECASE)
#     if now_match:
#         dataframe[columnName] = pd.to_datetime('now')
#         global_df = dataframe
#         return dataframe

#     # ========== 17. DATEADD ==========
#     # ===== 17. DATEADD =====
#     dateadd_match = re.match(
#         r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)', 
#         calculation, 
#         re.IGNORECASE
#     )

#     if dateadd_match:
#         date_col, interval, unit = dateadd_match.groups()

#         # Only fetch this column from SQL:
#         required_columns = [date_col]

#         if date_col not in dataframe.columns:
#             raise ValueError(f"Missing column '{date_col}' for DATEADD")

#         # Convert to datetime
#         date_series = pd.to_datetime(dataframe[date_col], errors='coerce')
#         interval = int(interval)

#         # Add the offset based on unit
#         if unit == 'day':
#             result = date_series + pd.to_timedelta(interval, unit='d')
#         elif unit == 'month':
#             result = date_series + pd.DateOffset(months=interval)
#         elif unit == 'year':
#             result = date_series + pd.DateOffset(years=interval)
#         else:
#             raise ValueError("Invalid unit for DATEADD")

#         # Create the new column
#         dataframe[columnName] = result
#         global_df = dataframe
#         return dataframe


#     # ===== 18. FORMATDATE =====
#     formatdate_match = re.match(
#         r'formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*\)', 
#         calculation, 
#         re.IGNORECASE
#     )

#     if formatdate_match:
#         col, fmt = formatdate_match.groups()

#         # Only fetch this column from SQL:
#         required_columns = [col]

#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column '{col}' for FORMATDATE")

#         # Replace JavaScript-style format to Python format if needed
#         fmt = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")

#         dataframe[columnName] = pd.to_datetime(dataframe[col], errors='coerce').dt.strftime(fmt)
#         global_df = dataframe
#         return dataframe


#     # ========== 19. REPLACE ==========
#     replace_match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)', calculation, re.IGNORECASE)
#     if replace_match:
#         col, old, new = replace_match.groups()
#         if col not in dataframe.columns:
#             raise ValueError("Missing column for REPLACE")
#         dataframe[columnName] = dataframe[col].astype(str).str.replace(old, new, regex=False)
#         global_df = dataframe
#         return dataframe

#     # ========== 20. TRIM ==========
#     trim_match = re.match(r'trim\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
#     if trim_match:
#         col = trim_match.group(1)
#         if col not in dataframe.columns:
#             raise ValueError("Missing column for TRIM")
#         dataframe[columnName] = dataframe[col].astype(str).str.strip()
#         global_df = dataframe
#         return dataframe
#         # ========== Extended IF-ELSEIF-ELSE ==========
#     extended_if_match = re.match(
#         r'^if\s*\((.*?)\)\s*then\s*(.*?)((?:\s*else\s*if\s*\(.*?\)\s*then\s*.*?)*)(?:\s*else\s*(.*?))?\s*end\s*$',
#         calculation,
#         re.IGNORECASE | re.DOTALL
#     )

#     if extended_if_match:
#         try:
#             conditions = []
#             then_values = []
#             else_value = None

#             # First if
#             conditions.append(extended_if_match.group(1).strip())
#             then_values.append(extended_if_match.group(2).strip())

#             # else if blocks
#             elseif_part = extended_if_match.group(3)
#             if elseif_part:
#                 elseif_matches = re.findall(r'else\s*if\s*\((.*?)\)\s*then\s*(.*?)($|\s*else)', elseif_part, re.IGNORECASE | re.DOTALL)
#                 for cond, val, _ in elseif_matches:
#                     conditions.append(cond.strip())
#                     then_values.append(val.strip())

#             # else block
#             else_value = extended_if_match.group(4).strip() if extended_if_match.group(4) else None

#             # Evaluate each condition
#             result = None
#             for i, cond in enumerate(conditions):
#                 # Replace [column] with dataframe["column"]
#                 cond_eval = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', cond)
#                 mask = eval(cond_eval)
#                 if result is None:
#                     result = np.where(mask, then_values[i], None)
#                 else:
#                     result = np.where(mask, then_values[i], result)

#             # Fill remaining with else_value
#             if else_value is not None:
#                 result = np.where(pd.isnull(result), else_value, result)

#             dataframe[columnName] = result
#             global_df = dataframe
#             return dataframe

#         except Exception as e:
#             raise ValueError(f"Error in extended IF-ELSEIF expression: {str(e)}")
#     # ===== CASE WHEN =====
#     # ===== CASE WHEN =====
#     case_pattern = r'case\s+(when.+?)\s+else\s+(.+?)\s+end'
#     case_match = re.match(case_pattern, calculation, re.IGNORECASE | re.DOTALL)
#     if case_match:
#         when_part, else_val = case_match.groups()
#         else_val = else_val.strip()
#         try:
#             # Initialize empty Series
#             result = pd.Series([np.nan] * len(dataframe))

#             # Split all WHEN ... THEN ... clauses
#             when_clauses = re.findall(r'when\s+(.+?)\s+then\s+(.+?)(?=when|$)', when_part, re.IGNORECASE | re.DOTALL)

#             for condition, then_val in when_clauses:
#                 # Replace [columns] with dataframe references
#                 condition_expr = replace_columns(condition.strip())
#                 then_val_expr = replace_columns(then_val.strip())

#                 # Evaluate condition
#                 mask = pd.eval(condition_expr, engine='python')

#                 # Evaluate THEN value if possible
#                 try:
#                     then_val_eval = pd.eval(then_val_expr, engine='python')
#                 except:
#                     then_val_eval = then_val.strip()

#                 # Assign values safely using pandas indexing
#                 result.loc[mask] = then_val_eval

#             # ELSE value
#             else_val_expr = replace_columns(else_val)
#             try:
#                 else_val_eval = pd.eval(else_val_expr, engine='python')
#             except:
#                 else_val_eval = else_val

#             # Fill remaining NaNs with ELSE
#             result.fillna(else_val_eval, inplace=True)

#             # Force numeric if possible
#             dataframe[columnName] = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
#             global_df = dataframe
#             return dataframe

#         except Exception as e:
#             raise ValueError(f"Error evaluating CASE WHEN: {str(e)}")

#     # ===== Extended IF-ELSEIF-ELSE =====
#     extended_if_match = re.match(
#         r'^if\s*\((.*?)\)\s*then\s*(.*?)((?:\s*else\s*if\s*\(.*?\)\s*then\s*.*?)*)(?:\s*else\s*(.*?))?\s*end\s*$',
#         calculation,
#         re.IGNORECASE | re.DOTALL
#     )

#     if extended_if_match:
#         try:
#             result = pd.Series([np.nan] * len(dataframe))
#             conditions = []
#             then_values = []

#             # First IF
#             conditions.append(extended_if_match.group(1).strip())
#             then_values.append(extended_if_match.group(2).strip())

#             # ELSE IF blocks
#             elseif_part = extended_if_match.group(3)
#             if elseif_part:
#                 elseif_matches = re.findall(r'else\s*if\s*\((.*?)\)\s*then\s*(.*?)($|\s*else)', elseif_part, re.IGNORECASE | re.DOTALL)
#                 for cond, val, _ in elseif_matches:
#                     conditions.append(cond.strip())
#                     then_values.append(val.strip())

#             # ELSE block
#             else_value = extended_if_match.group(4).strip() if extended_if_match.group(4) else None

#             # Evaluate each condition
#             for i, cond in enumerate(conditions):
#                 cond_eval = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', cond)
#                 mask = pd.eval(cond_eval)
#                 then_val_expr = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', then_values[i])
#                 try:
#                     then_val_eval = pd.eval(then_val_expr, engine='python')
#                 except:
#                     then_val_eval = then_values[i]
#                 result.loc[mask] = then_val_eval

#             # ELSE block
#             if else_value is not None:
#                 else_val_expr = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', else_value)
#                 try:
#                     else_val_eval = pd.eval(else_val_expr, engine='python')
#                 except:
#                     else_val_eval = else_value
#                 result.fillna(else_val_eval, inplace=True)

#             # Final numeric conversion
#             dataframe[columnName] = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
#             global_df = dataframe
#             return dataframe

#         except Exception as e:
#             raise ValueError(f"Error in extended IF-ELSEIF expression: {str(e)}")


    
    

#     # ========== 13. Arithmetic/Logical Expressions ==========
#     # words = re.findall(r'\[([^\]]+)\]', calculation)
#     # if not words:
#     #     raise ValueError("No valid column names found in expression.")
#     # # for col in words:
#     # #     if col not in dataframe.columns:
#     # #         raise ValueError(f"Missing column: {col}")
#     # #     calculation = calculation.replace(f"[{col}]", f'dataframe["{col}"]')
#     # for col in words:
#     #     if col not in dataframe.columns:
#     #         raise ValueError(f"Missing column: {col}")

#     #     # 🔥 FORCE numeric conversion
#     #     dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')

#     #     calculation = calculation.replace(
#     #         f"[{col}]",
#     #         f'dataframe["{col}"]'
#     #     )

#     # try:
#     #     # Evaluate expression
#     #     result = eval(calculation)

#     #     # Convert to numeric (float), coerce errors to NaN
#     #     # if isinstance(result, pd.Series):
#     #     #     result = pd.to_numeric(result, errors='coerce')  # ensures dtype is float
#     #     #     result = result.replace([np.inf, -np.inf], np.nan)
#     #     #     result = result.fillna(0)  # replace NaN with 0
#     #     #     dataframe[columnName] = result
#     #     if isinstance(result, pd.Series):
#     #         # Convert to float safely and handle NaN/inf
#     #         result = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
#     #         dataframe[columnName] = result.astype(float)
#     #     else:
#     #         # For scalar result
#     #         result = float(result)
#     #         dataframe[columnName] = result

#     #     global_df = dataframe
#     #     return dataframe
#     # ===== 13. Arithmetic/Logical Expressions (Safe) =====
#     words = re.findall(r'\[([^\]]+)\]', calculation)
#     if not words:
#         raise ValueError("No valid column names found in expression.")

#     # Step 1: Ensure all referenced columns exist and are numeric
#     for col in words:
#         if col not in dataframe.columns:
#             raise ValueError(f"Missing column: {col}")
#         dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')  # force numeric

#     # Step 2: Replace [column] with dataframe["column"] references
#     expr_python = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', calculation)

#     # Step 3: Convert Excel/PowerBI-style IF to Python np.where
#     # Example: IF([A] > 0, [B], 0) => np.where(dataframe["A"]>0, dataframe["B"], 0)
#     def convert_if(match):
#         condition, true_val, false_val = match.groups()
#         return f'np.where({condition}, {true_val}, {false_val})'

#     expr_python = re.sub(
#         r'if\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)', 
#         convert_if, 
#         expr_python, 
#         flags=re.IGNORECASE
#     )

#     # Step 4: Evaluate using pandas safely
#     try:
#         result = pd.eval(expr_python, engine='python')  # safe evaluation
#         if isinstance(result, pd.Series):
#             # Convert to float, handle NaN/inf
#             result = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
#             dataframe[columnName] = result.astype(float)
#         else:
#             dataframe[columnName] = float(result)
#         global_df = dataframe
#         return dataframe

#     # try:
#     #     result = eval(calculation)
#     #     if isinstance(result, pd.Series) and result.dtype == bool:
#     #         filtered_df = dataframe[result]
#     #         global_df = filtered_df
#     #         return filtered_df
#     #     else:
#     #         dataframe[columnName] = result
#     #         global_df = dataframe
#     #         return dataframe
        
#     except Exception as e:
#         raise ValueError(f"Failed to evaluate general expression: {str(e)}")


def perform_calculation(dataframe, columnName, calculation):
    global global_df
    calculation = calculation.strip()
    # def replace_columns(expr):
    #     columns = re.findall(r'\[([^\]]+)\]', expr)
    #     for col in columns:
    #         if col not in dataframe.columns:
    #             raise ValueError(f"Missing column: {col}")
    #         # Force numeric conversion if possible
    #         dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')
    #         expr = expr.replace(f"[{col}]", f'dataframe["{col}"]')
    #     return expr
    def replace_columns(expr):
        columns = re.findall(r'\[([^\]]+)\]', expr)
        for col in columns:
            if col not in dataframe.columns:
                raise ValueError(f"Missing column: {col}")

            # ❗ DO NOT force numeric here — it breaks text columns
            expr = expr.replace(f"[{col}]", f'dataframe["{col}"]')
        return expr



    # ========== 1. IF condition ==========
   
    pattern_then_else = (
        r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$"
    )

    # Pattern 2: if(condition, a, b)
    pattern_comma = (
        r"if\s*\(\s*(.+?)\s*,\s*['\"]?(.*?)['\"]?\s*,\s*['\"]?(.*?)['\"]?\s*\)$"
    )

    match = (
        re.match(pattern_then_else, calculation, re.IGNORECASE)
        or re.match(pattern_comma, calculation, re.IGNORECASE)
    )
    # pattern_if = r"if\s*\(\s*(.+?)\s*\)\s*then\s*['\"]?(.*?)['\"]?\s*else\s*['\"]?(.*?)['\"]?$"
    # match = re.match(pattern_if, calculation, re.IGNORECASE)
    if match:
        condition_expr, then_val, else_val = match.groups()
        print("Eval condition string:", condition_expr)

        # Replace column references like [region] with dataframe["region"]
        condition_expr = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', condition_expr)

        try:
            result = np.where(eval(condition_expr), then_val, else_val)
            dataframe[columnName] = result
            global_df = dataframe
            return dataframe
        except Exception as e:
            raise ValueError(f"Error in IF expression: {str(e)}")

    # ========== 2. SWITCH ==========
    switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calculation, re.IGNORECASE)
    if switch_match:
        col_name, rest = switch_match.groups()
        if col_name not in dataframe.columns:
            raise ValueError(f"Missing column: {col_name}")

        cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
        default_match = re.search(r'["\']?default["\']?\s*,\s*["\']?(.*?)["\']?\s*$', rest, re.IGNORECASE)
        default_value = default_match.group(1) if default_match else None

        def switch_map(val):
            for match_val, result in cases:
                if str(val) == match_val:
                    return result
            return default_value

        dataframe[columnName] = dataframe[col_name].apply(switch_map)
        global_df = dataframe
        return dataframe

    # ========== 3. IFERROR ==========
    iferror_match = re.match(r"iferror\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)$", calculation, re.IGNORECASE)
    if iferror_match:
        expr, fallback = iferror_match.groups()
        for col in re.findall(r'\[([^\]]+)\]', expr):
            if col not in dataframe.columns:
                raise ValueError(f"Missing column: {col}")
            expr = expr.replace(f"[{col}]", f'dataframe["{col}"]')
        try:
            result = eval(expr)
            dataframe[columnName] = np.where(pd.isnull(result) | np.isinf(result), fallback, result)
            global_df = dataframe
            return dataframe
        except Exception:
            dataframe[columnName] = fallback
            global_df = dataframe
            return dataframe

    # ========== 4. MAXX, MINX ==========
    # match_xx = re.match(r'(maxx|minx)\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    # if match_xx:
    #     func, col = match_xx.groups()
    #     col = col.strip()
    #     if col not in dataframe.columns:
    #         raise ValueError(f"Missing column: {col}")
    #     value = dataframe[col].max() if func.lower() == 'maxx' else dataframe[col].min()
    #     dataframe[columnName] = value
    #     global_df = dataframe
    #     return dataframe
    # ========== 4. MAXX / MINX (full expressions) ==========
    match_xx = re.match(r'(maxx|minx)\s*\(\s*(.+)\s*\)', calculation, re.IGNORECASE)
    if match_xx:
        func, expr = match_xx.groups()

        expr = replace_columns(expr)
        val = eval(expr)

        dataframe[columnName] = val.max() if func.lower() == 'maxx' else val.min()
        global_df = dataframe
        return dataframe


    # ========== 5. ABS ==========
    abs_match = re.match(r'abs\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if abs_match:
        col = abs_match.group(1)
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        dataframe[columnName] = dataframe[col].abs()
        global_df = dataframe
        return dataframe

    # ========== 6. Aggregates ==========
    agg_match = re.match(r'(sum|avg|mean|min|max|count)\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if agg_match:
        func, col = agg_match.groups()
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        func_map = {
            'sum': dataframe[col].sum,
            'avg': dataframe[col].mean,
            'mean': dataframe[col].mean,
            'min': dataframe[col].min,
            'max': dataframe[col].max,
            'count': dataframe[col].count
        }
        try:
            result = func_map[func.lower()]()
            dataframe[columnName] = result
            global_df = dataframe
            return dataframe
        except Exception as e:
            raise ValueError(f"Error in aggregate: {str(e)}")

    # ========== 7. CALCULATE ==========
    # calc_match = re.match(
    #     r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*\[([^\]]+)\]\s*=\s*[\'"](.+?)[\'"]\s*\)',
    #     calculation, re.IGNORECASE
    # )
    # if calc_match:
    #     col_to_sum, filter_col, filter_val = calc_match.groups()
    #     if col_to_sum not in dataframe.columns or filter_col not in dataframe.columns:
    #         raise ValueError("Missing column(s) for CALCULATE")
    #     filtered_df = dataframe[dataframe[filter_col] == filter_val]
    #     sum_result = filtered_df[col_to_sum].sum()
    #     dataframe[columnName] = sum_result
    #     global_df = dataframe
    #     return dataframe
    # ========== 7. CALCULATE (multiple filters) ==========
    # calc_match = re.match(
    #     r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*(.+)\)',
    #     calculation, re.IGNORECASE
    # )

    # if calc_match:
    #     col_to_sum, filters_str = calc_match.groups()

    #     if col_to_sum not in dataframe.columns:
    #         raise ValueError("Missing column for CALCULATE")

    #     filter_pairs = re.findall(
    #         r'\[([^\]]+)\]\s*=\s*[\'"](.+?)[\'"]',
    #         filters_str
    #     )

    #     filtered_df = dataframe.copy()

    #     for f_col, f_val in filter_pairs:
    #         if f_col not in dataframe.columns:
    #             raise ValueError(f"Missing column '{f_col}' for CALCULATE")
    #         filtered_df = filtered_df[filtered_df[f_col] == f_val]

    #     dataframe[columnName] = filtered_df[col_to_sum].sum()
    #     global_df = dataframe
    #     return dataframe
    calc_match = re.match(
        r'calculate\s*\(\s*sum\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*(.+)\)',
        calculation, re.IGNORECASE
    )

    if calc_match:
        col_to_sum, filters_str = calc_match.groups()

        print("🟢 CALCULATE MATCHED")
        print("   → Column to SUM:", col_to_sum)
        print("   → Raw filters:", filters_str)

        if col_to_sum not in dataframe.columns:
            raise ValueError("❌ Missing column for CALCULATE")

        filter_pairs = re.findall(
            r'\[([^\]]+)\]\s*=\s*[\'"](.+?)[\'"]',
            filters_str
        )

        print("   → Parsed filters:", filter_pairs)

        filtered_df = dataframe.copy()

        for f_col, f_val in filter_pairs:
            print(f"   → Applying filter: {f_col} == {f_val}")

            if f_col not in dataframe.columns:
                raise ValueError(f"❌ Missing column '{f_col}' for CALCULATE")

            filtered_df = filtered_df[filtered_df[f_col] == f_val]
            print("     Remaining rows:", len(filtered_df))

        result_value = filtered_df[col_to_sum].sum()
        print("🟢 CALCULATE RESULT:", result_value)

        dataframe[columnName] = result_value
        global_df = dataframe
        return dataframe



    # ========== 8. Text Functions ==========
    len_match = re.match(r'len\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if len_match:
        col = len_match.group(1)
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        dataframe[columnName] = dataframe[col].astype(str).str.len()
        global_df = dataframe
        return dataframe
   

    lower_match = re.match(r'lower\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if lower_match:
        col = lower_match.group(1)
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        dataframe[columnName] = dataframe[col].astype(str).str.lower()
        global_df = dataframe
        return dataframe

    upper_match = re.match(r'upper\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if upper_match:
        col = upper_match.group(1)
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        dataframe[columnName] = dataframe[col].astype(str).str.upper()
        global_df = dataframe
        return dataframe
    # concat_match = re.match(r'concat\s*\((.+)\)', calculation, re.IGNORECASE)
    # if concat_match:
    #     inner = concat_match.group(1)
    #     parts = [p.strip() for p in re.split(r',(?![^\[]*\])', inner)]

    #     try:
    #         concat_parts = []
    #         for p in parts:
    #             if p.startswith('[') and p.endswith(']'):
    #                 col = p[1:-1]
    #                 if col not in dataframe.columns:
    #                     raise ValueError(f"Missing column: {col}")
    #                 concat_parts.append(dataframe[col].astype(str))
    #             else:
    #                 const_str = p.strip('"').strip("'")
    #                 concat_parts.append(const_str)

    #         # Use `add` from pandas for efficient row-wise string concat
    #         result = concat_parts[0]
    #         for part in concat_parts[1:]:
    #             result = result + part

    #         dataframe[columnName] = result
    #         global_df = dataframe
    #         return dataframe
    # ========== CONCAT (supports nested UPPER/LOWER/TRIM) ==========
    # ========== CONCAT (final fixed version) ==========
    concat_match = re.match(r'concat\s*\((.+)\)', calculation, re.IGNORECASE)
    if concat_match:
        inner = concat_match.group(1)

        print("🟢 CONCAT MATCHED")
        print("   → Inner raw:", inner)

        # text functions first
        inner = re.sub(r'upper\(\[([^\]]+)\]\)',
                    r'dataframe["\1"].astype(str).str.upper()',
                    inner, flags=re.IGNORECASE)

        inner = re.sub(r'lower\(\[([^\]]+)\]\)',
                    r'dataframe["\1"].astype(str).str.lower()',
                    inner, flags=re.IGNORECASE)

        inner = re.sub(r'trim\(\[([^\]]+)\]\)',
                    r'dataframe["\1"].astype(str).str.strip()',
                    inner, flags=re.IGNORECASE)

        print("   → After text functions:", inner)

        # ❌ DO NOT call replace_columns() here anymore
        # inner = replace_columns(inner)

        parts = [p.strip() for p in re.split(r',(?![^\(\)]*\))', inner)]
        print("   → Split parts:", parts)

        python_parts = []
        for p in parts:
            if (p.startswith("'") and p.endswith("'")) or (p.startswith('"') and p.endswith('"')):
                python_parts.append(p)
            elif p.startswith("dataframe["):
                python_parts.append(p)
            else:
                python_parts.append(f'"{p}"')

        expr = " + ".join(python_parts)
        print("   → Final Python expr:", expr)

        try:
            dataframe[columnName] = eval(expr)
            global_df = dataframe
            return dataframe
        except Exception as e:
            raise ValueError(f"❌ Error in CONCAT expression: {expr} -> {str(e)}")



        # except Exception as e:
        #     raise ValueError(f"Error in CONCAT expression: {str(e)}")


    # ========== 9. Date Functions ==========
    date_match = re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if date_match:
        func, col = date_match.groups()
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        dataframe[columnName] = pd.to_datetime(dataframe[col], errors='coerce')
        if func.lower() == 'year':
            dataframe[columnName] = dataframe[columnName].dt.year
        elif func.lower() == 'month':
            dataframe[columnName] = dataframe[columnName].dt.month
        elif func.lower() == 'day':
            dataframe[columnName] = dataframe[columnName].dt.day
        global_df = dataframe
        return dataframe

    # ========== 10. ROUND ==========
    # round_match = re.match(r'round\s*\(\s*\[([^\]]+)\]\s*,\s*(\d+)\s*\)', calculation, re.IGNORECASE)
    # if round_match:
    #     col, decimals = round_match.groups()
    #     if col not in dataframe.columns:
    #         raise ValueError(f"Missing column: {col}")
    #     dataframe[columnName] = dataframe[col].round(int(decimals))
    #     global_df = dataframe
    #     return dataframe

    round_match = re.match(r'round\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)', calculation, re.IGNORECASE)
    if round_match:
        expr, decimals = round_match.groups()

        # Replace [column] with numeric dataframe references
        def replace_column(match):
            col_name = match.group(1)
            if col_name not in dataframe.columns:
                raise ValueError(f"Missing column: {col_name}")
            # Convert to numeric (errors='coerce' will turn non-numeric to NaN)
            dataframe[col_name] = pd.to_numeric(dataframe[col_name], errors='coerce')
            return f"dataframe['{col_name}']"

        expr_python = re.sub(r'\[([^\]]+)\]', replace_column, expr)

        # Evaluate expression
        dataframe[columnName] = eval(expr_python).round(int(decimals))
        global_df = dataframe
        return dataframe

    # ========== 11. ISNULL ==========
    isnull_match = re.match(r'isnull\s*\(\s*\[([^\]]+)\]\s*,\s*["\']?(.*?)["\']?\s*\)', calculation, re.IGNORECASE)
    if isnull_match:
        col, replacement = isnull_match.groups()
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        dataframe[columnName] = dataframe[col].fillna(replacement)
        global_df = dataframe
        return dataframe

    # ========== 12. IN expression ==========
    in_match = re.match(r'\[([^\]]+)\]\s+in\s+\((.*?)\)', calculation, re.IGNORECASE)
    if in_match:
        col, values = in_match.groups()
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        value_list = [v.strip().strip('"').strip("'") for v in values.split(',')]
        result = dataframe[col].astype(str).isin(value_list)
        dataframe[columnName] = result
        global_df = dataframe
        return dataframe
    # ========== 14. DATEDIFF ==========
    datediff_match = re.match(r'datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if datediff_match:
        end_col, start_col = datediff_match.groups()
        if end_col not in dataframe.columns or start_col not in dataframe.columns:
            raise ValueError("Missing column(s) for DATEDIFF")
        dataframe[columnName] = (pd.to_datetime(dataframe[end_col]) - pd.to_datetime(dataframe[start_col])).dt.days
        global_df = dataframe
        return dataframe

    # ========== 15. TODAY ==========
    today_match = re.match(r'today\(\)', calculation, re.IGNORECASE)
    if today_match:
        dataframe[columnName] = pd.to_datetime('today').normalize()
        global_df = dataframe
        return dataframe

    # ========== 16. NOW ==========
    now_match = re.match(r'now\(\)', calculation, re.IGNORECASE)
    if now_match:
        dataframe[columnName] = pd.to_datetime('now')
        global_df = dataframe
        return dataframe

    # ========== 17. DATEADD ==========
    # # ===== 17. DATEADD =====
    # dateadd_match = re.match(
    #     r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)', 
    #     calculation, 
    #     re.IGNORECASE
    # )

    # if dateadd_match:
    #     date_col, interval, unit = dateadd_match.groups()

    #     # Only fetch this column from SQL:
    #     required_columns = [date_col]

    #     if date_col not in dataframe.columns:
    #         raise ValueError(f"Missing column '{date_col}' for DATEADD")

    #     # Convert to datetime
    #     date_series = pd.to_datetime(dataframe[date_col], errors='coerce')
    #     interval = int(interval)

    #     # Add the offset based on unit
    #     if unit == 'day':
    #         result = date_series + pd.to_timedelta(interval, unit='d')
    #     elif unit == 'month':
    #         result = date_series + pd.DateOffset(months=interval)
    #     elif unit == 'year':
    #         result = date_series + pd.DateOffset(years=interval)
    #     else:
    #         raise ValueError("Invalid unit for DATEADD")

    #     # Create the new column
    #     dataframe[columnName] = result
    #     global_df = dataframe
    #     return dataframe
    # ========== 17. DATEADD (day | month | year | quarter) ==========
    dateadd_match = re.match(
        r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year|quarter)["\']\s*\)',
        calculation, re.IGNORECASE
    )

    if dateadd_match:
        date_col, interval, unit = dateadd_match.groups()

        if date_col not in dataframe.columns:
            raise ValueError(f"Missing column '{date_col}' for DATEADD")

        date_series = pd.to_datetime(dataframe[date_col], errors='coerce')
        interval = int(interval)

        if unit.lower() == 'day':
            result = date_series + pd.to_timedelta(interval, unit='d')
        elif unit.lower() == 'month':
            result = date_series + pd.DateOffset(months=interval)
        elif unit.lower() == 'year':
            result = date_series + pd.DateOffset(years=interval)
        elif unit.lower() == 'quarter':
            result = date_series + pd.DateOffset(months=interval * 3)

        dataframe[columnName] = result
        global_df = dataframe
        return dataframe



    # ===== 18. FORMATDATE =====
    # formatdate_match = re.match(
    #     r'formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*\)', 
    #     calculation, 
    #     re.IGNORECASE
    # )

    # if formatdate_match:
    #     col, fmt = formatdate_match.groups()

    #     # Only fetch this column from SQL:
    #     required_columns = [col]

    #     if col not in dataframe.columns:
    #         raise ValueError(f"Missing column '{col}' for FORMATDATE")

    #     # Replace JavaScript-style format to Python format if needed
    #     fmt = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")

    #     dataframe[columnName] = pd.to_datetime(dataframe[col], errors='coerce').dt.strftime(fmt)
    #     global_df = dataframe
    #     return dataframe
    # ========== 18. FORMATDATE (supports MMM-YYYY etc.) ==========
    formatdate_match = re.match(
        r'formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.+?)["\']\s*\)',
        calculation, re.IGNORECASE
    )

    if formatdate_match:
        col, fmt = formatdate_match.groups()

        if col not in dataframe.columns:
            raise ValueError(f"Missing column '{col}' for FORMATDATE")

        fmt = (fmt.replace("YYYY", "%Y")
                .replace("MMM", "%b")
                .replace("MM", "%m")
                .replace("DD", "%d"))

        dataframe[columnName] = pd.to_datetime(
            dataframe[col], errors='coerce'
        ).dt.strftime(fmt)

        global_df = dataframe
        return dataframe



    # ========== 19. REPLACE ==========
    replace_match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)', calculation, re.IGNORECASE)
    if replace_match:
        col, old, new = replace_match.groups()
        if col not in dataframe.columns:
            raise ValueError("Missing column for REPLACE")
        dataframe[columnName] = dataframe[col].astype(str).str.replace(old, new, regex=False)
        global_df = dataframe
        return dataframe

    # ========== 20. TRIM ==========
    trim_match = re.match(r'trim\s*\(\s*\[([^\]]+)\]\s*\)', calculation, re.IGNORECASE)
    if trim_match:
        col = trim_match.group(1)
        if col not in dataframe.columns:
            raise ValueError("Missing column for TRIM")
        dataframe[columnName] = dataframe[col].astype(str).str.strip()
        global_df = dataframe
        return dataframe
        # ========== Extended IF-ELSEIF-ELSE ==========
    extended_if_match = re.match(
        r'^if\s*\((.*?)\)\s*then\s*(.*?)((?:\s*else\s*if\s*\(.*?\)\s*then\s*.*?)*)(?:\s*else\s*(.*?))?\s*end\s*$',
        calculation,
        re.IGNORECASE | re.DOTALL
    )

    if extended_if_match:
        try:
            conditions = []
            then_values = []
            else_value = None

            # First if
            conditions.append(extended_if_match.group(1).strip())
            then_values.append(extended_if_match.group(2).strip())

            # else if blocks
            elseif_part = extended_if_match.group(3)
            if elseif_part:
                elseif_matches = re.findall(r'else\s*if\s*\((.*?)\)\s*then\s*(.*?)($|\s*else)', elseif_part, re.IGNORECASE | re.DOTALL)
                for cond, val, _ in elseif_matches:
                    conditions.append(cond.strip())
                    then_values.append(val.strip())

            # else block
            else_value = extended_if_match.group(4).strip() if extended_if_match.group(4) else None

            # Evaluate each condition
            result = None
            for i, cond in enumerate(conditions):
                # Replace [column] with dataframe["column"]
                cond_eval = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', cond)
                mask = eval(cond_eval)
                if result is None:
                    result = np.where(mask, then_values[i], None)
                else:
                    result = np.where(mask, then_values[i], result)

            # Fill remaining with else_value
            if else_value is not None:
                result = np.where(pd.isnull(result), else_value, result)

            dataframe[columnName] = result
            global_df = dataframe
            return dataframe

        except Exception as e:
            raise ValueError(f"Error in extended IF-ELSEIF expression: {str(e)}")
    # ===== CASE WHEN =====
    # ===== CASE WHEN =====
    case_pattern = r'case\s+(when.+?)\s+else\s+(.+?)\s+end'
    case_match = re.match(case_pattern, calculation, re.IGNORECASE | re.DOTALL)
    if case_match:
        when_part, else_val = case_match.groups()
        else_val = else_val.strip()
        try:
            # Initialize empty Series
            result = pd.Series([np.nan] * len(dataframe))

            # Split all WHEN ... THEN ... clauses
            when_clauses = re.findall(r'when\s+(.+?)\s+then\s+(.+?)(?=when|$)', when_part, re.IGNORECASE | re.DOTALL)

            for condition, then_val in when_clauses:
                # Replace [columns] with dataframe references
                condition_expr = replace_columns(condition.strip())
                then_val_expr = replace_columns(then_val.strip())

                # Evaluate condition
                mask = pd.eval(condition_expr, engine='python')

                # Evaluate THEN value if possible
                try:
                    then_val_eval = pd.eval(then_val_expr, engine='python')
                except:
                    then_val_eval = then_val.strip()

                # Assign values safely using pandas indexing
                result.loc[mask] = then_val_eval

            # ELSE value
            else_val_expr = replace_columns(else_val)
            try:
                else_val_eval = pd.eval(else_val_expr, engine='python')
            except:
                else_val_eval = else_val

            # Fill remaining NaNs with ELSE
            result.fillna(else_val_eval, inplace=True)

            # Force numeric if possible
            dataframe[columnName] = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
            global_df = dataframe
            return dataframe

        except Exception as e:
            raise ValueError(f"Error evaluating CASE WHEN: {str(e)}")

    # ===== Extended IF-ELSEIF-ELSE =====
    extended_if_match = re.match(
        r'^if\s*\((.*?)\)\s*then\s*(.*?)((?:\s*else\s*if\s*\(.*?\)\s*then\s*.*?)*)(?:\s*else\s*(.*?))?\s*end\s*$',
        calculation,
        re.IGNORECASE | re.DOTALL
    )

    if extended_if_match:
        try:
            result = pd.Series([np.nan] * len(dataframe))
            conditions = []
            then_values = []

            # First IF
            conditions.append(extended_if_match.group(1).strip())
            then_values.append(extended_if_match.group(2).strip())

            # ELSE IF blocks
            elseif_part = extended_if_match.group(3)
            if elseif_part:
                elseif_matches = re.findall(r'else\s*if\s*\((.*?)\)\s*then\s*(.*?)($|\s*else)', elseif_part, re.IGNORECASE | re.DOTALL)
                for cond, val, _ in elseif_matches:
                    conditions.append(cond.strip())
                    then_values.append(val.strip())

            # ELSE block
            else_value = extended_if_match.group(4).strip() if extended_if_match.group(4) else None

            # Evaluate each condition
            for i, cond in enumerate(conditions):
                cond_eval = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', cond)
                mask = pd.eval(cond_eval)
                then_val_expr = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', then_values[i])
                try:
                    then_val_eval = pd.eval(then_val_expr, engine='python')
                except:
                    then_val_eval = then_values[i]
                result.loc[mask] = then_val_eval

            # ELSE block
            if else_value is not None:
                else_val_expr = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', else_value)
                try:
                    else_val_eval = pd.eval(else_val_expr, engine='python')
                except:
                    else_val_eval = else_value
                result.fillna(else_val_eval, inplace=True)

            # Final numeric conversion
            dataframe[columnName] = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
            global_df = dataframe
            return dataframe

        except Exception as e:
            raise ValueError(f"Error in extended IF-ELSEIF expression: {str(e)}")


    
    

    # ========== 13. Arithmetic/Logical Expressions ==========
    # words = re.findall(r'\[([^\]]+)\]', calculation)
    # if not words:
    #     raise ValueError("No valid column names found in expression.")
    # # for col in words:
    # #     if col not in dataframe.columns:
    # #         raise ValueError(f"Missing column: {col}")
    # #     calculation = calculation.replace(f"[{col}]", f'dataframe["{col}"]')
    # for col in words:
    #     if col not in dataframe.columns:
    #         raise ValueError(f"Missing column: {col}")

    #     # 🔥 FORCE numeric conversion
    #     dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')

    #     calculation = calculation.replace(
    #         f"[{col}]",
    #         f'dataframe["{col}"]'
    #     )

    # try:
    #     # Evaluate expression
    #     result = eval(calculation)

    #     # Convert to numeric (float), coerce errors to NaN
    #     # if isinstance(result, pd.Series):
    #     #     result = pd.to_numeric(result, errors='coerce')  # ensures dtype is float
    #     #     result = result.replace([np.inf, -np.inf], np.nan)
    #     #     result = result.fillna(0)  # replace NaN with 0
    #     #     dataframe[columnName] = result
    #     if isinstance(result, pd.Series):
    #         # Convert to float safely and handle NaN/inf
    #         result = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
    #         dataframe[columnName] = result.astype(float)
    #     else:
    #         # For scalar result
    #         result = float(result)
    #         dataframe[columnName] = result

    #     global_df = dataframe
    #     return dataframe
    # ========== Nested IF (IF … THEN … ELSE …) ==========
    def convert_nested_if(expr):
        pattern = r'if\s*\(\s*(.*?)\s*\)\s*then\s*(.*?)\s*else\s*(.*)'
        while re.search(pattern, expr, re.IGNORECASE | re.DOTALL):
            expr = re.sub(
                pattern,
                lambda m: f'np.where({m.group(1)}, {m.group(2)}, {m.group(3)})',
                expr, flags=re.IGNORECASE | re.DOTALL
            )
        return expr

    if re.search(r'\bif\s*\(', calculation, re.IGNORECASE):
        expr = convert_nested_if(calculation)
        expr = replace_columns(expr)
        dataframe[columnName] = eval(expr)
        global_df = dataframe
        return dataframe

    # ===== 13. Arithmetic/Logical Expressions (Safe) =====
    words = re.findall(r'\[([^\]]+)\]', calculation)
    if not words:
        raise ValueError("No valid column names found in expression.")

    # Step 1: Ensure all referenced columns exist and are numeric
    for col in words:
        if col not in dataframe.columns:
            raise ValueError(f"Missing column: {col}")
        dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')  # force numeric

    # Step 2: Replace [column] with dataframe["column"] references
    expr_python = re.sub(r'\[([^\]]+)\]', r'dataframe["\1"]', calculation)

    # Step 3: Convert Excel/PowerBI-style IF to Python np.where
    # Example: IF([A] > 0, [B], 0) => np.where(dataframe["A"]>0, dataframe["B"], 0)
    def convert_if(match):
        condition, true_val, false_val = match.groups()
        return f'np.where({condition}, {true_val}, {false_val})'

    expr_python = re.sub(
        r'if\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)', 
        convert_if, 
        expr_python, 
        flags=re.IGNORECASE
    )

    # Step 4: Evaluate using pandas safely
    try:
        result = pd.eval(expr_python, engine='python')  # safe evaluation
        if isinstance(result, pd.Series):
            # Convert to float, handle NaN/inf
            result = pd.to_numeric(result, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
            dataframe[columnName] = result.astype(float)
        else:
            dataframe[columnName] = float(result)
        global_df = dataframe
        return dataframe

    # try:
    #     result = eval(calculation)
    #     if isinstance(result, pd.Series) and result.dtype == bool:
    #         filtered_df = dataframe[result]
    #         global_df = filtered_df
    #         return filtered_df
    #     else:
    #         dataframe[columnName] = result
    #         global_df = dataframe
    #         return dataframe
        
    except Exception as e:
        raise ValueError(f"Failed to evaluate general expression: {str(e)}")



# def fetchText_data(databaseName, table_Name, x_axis, aggregate_py,selectedUser,filter_options):
#     print("aggregate===========================>>>>", aggregate_py)   
#     print(table_Name)
# #     aggregate_py = {
# #     'count': 'count',
# #     'sum': 'sum',
# #     'average': 'mean',
# #     'minimum': 'min',
# #     'maximum': 'max'
# # }.get(aggregate, 'sum')  # Default to 'sum' if no match

    
#     conn = get_db_connection_or_path(selectedUser, databaseName)

        
#     cur = conn.cursor()

#     # Check the data type of the x_axis column
#     cur.execute(f"""
#         SELECT data_type 
#         FROM information_schema.columns 
#         WHERE table_name = %s AND column_name = %s
#     """, (table_Name, x_axis))
    
#     column_type = cur.fetchone()[0]
#     # print("column_type",column_type)
#     # Use DISTINCT only if the column type is character varying
#     if column_type == 'character varying':
#         query = f"""
#         SELECT COUNT(DISTINCT {x_axis}) AS total_{x_axis}
#         FROM {table_Name}
#         """
#         print("character varying")  
#     else:
#         query = f"""
#         SELECT {aggregate_py}({x_axis}) AS total_{x_axis}
#         FROM {table_Name}
#         """

#     print("Query:", query)
    
#     cur.execute(query)
#     result = cur.fetchone()  # Fetch only one row since the query returns a single value
#     print("result",result)
    
#     # Close the cursor and connection
#     cur.close()
#     conn.close()

#     # Process the result into a dictionary
#     data = {"total_x_axis": result[0]}  # result[0] contains the aggregated value

#     return data

# def fetchText_data(databaseName, table_Name, x_axis, aggregate_py, selectedUser, filter_options):
#     print("aggregate===========================>>>>", aggregate_py)   
#     print("Table:", table_Name)
#     print("Filter Options:", filter_options)

#     conn = get_db_connection_or_path(selectedUser, databaseName)
#     cur = conn.cursor()

#     # 1. Build the WHERE clause dynamically based on filter_options
#     where_clauses = []
#     query_params = []

#     if filter_options:
#         for column, values in filter_options.items():
#             # Only proceed if values list is not empty
#             if values:
#                 # Create placeholders like %s, %s, %s depending on list length
#                 placeholders = ', '.join(['%s'] * len(values))
                
#                 # Construct the clause: "region IN (%s, %s)"
#                 where_clauses.append(f"{column} IN ({placeholders})")
                
#                 # Add the actual values to the parameters list
#                 query_params.extend(values)

#     # Join multiple filters with AND (if you have more than one filter key)
#     where_sql = ""
#     if where_clauses:
#         where_sql = "WHERE " + " AND ".join(where_clauses)


#     # 2. Check Data Type
#     cur.execute(f"""
#         SELECT data_type 
#         FROM information_schema.columns 
#         WHERE table_name = %s AND column_name = %s
#     """, (table_Name, x_axis))
    
#     row = cur.fetchone()
#     if not row:
#         print(f"Error: Column {x_axis} not found in table {table_Name}")
#         cur.close()
#         conn.close()
#         return {} # Handle error gracefully

#     column_type = row[0]

#     # 3. Construct the Main Query
#     # Note: We inject the {where_sql} string into the query
#     if column_type == 'character varying':
#         query = f"""
#         SELECT COUNT(DISTINCT {x_axis}) AS total_{x_axis}
#         FROM {table_Name}
#         {where_sql}
#         """
#         print("Type: character varying")  
#     else:
#         query = f"""
#         SELECT {aggregate_py}({x_axis}) AS total_{x_axis}
#         FROM {table_Name}
#         {where_sql}
#         """

#     print("Final Query:", query)
#     print("Query Params:", query_params)
    
#     # 4. Execute with parameters (Safe against SQL injection for values)
#     cur.execute(query, tuple(query_params))
#     result = cur.fetchone()
    
#     cur.close()
#     conn.close()

#     if result:
#         data = {"total_x_axis": result[0]}
#     else:
#         data = {"total_x_axis": 0} # Handle empty result

#     return data


def fetchText_data(databaseName, table_Name, x_axis, aggregate_py, selectedUser, filter_options):
    print(f"Fetch Data Triggered: Agg={aggregate_py}, Table={table_Name}, Filters={filter_options}")

    conn = get_db_connection_or_path(selectedUser, databaseName)
    cur = conn.cursor()

    # --- 1. Dynamic WHERE Clause Construction ---
    # where_sql = ""
    # query_params = []

    # # Check if filter_options exists AND is a dictionary (handles None case)
    # if filter_options and isinstance(filter_options, dict):
    #     where_clauses = []
        
    #     for column, values in filter_options.items():
    #         # Only process if 'values' is a valid list with items
    #         if values and isinstance(values, list) and len(values) > 0:
    #             # Create placeholders: %s, %s, %s
    #             placeholders = ', '.join(['%s'] * len(values))
                
    #             # Add clause: "region IN (%s, %s)"
    #             where_clauses.append(f'"{column}" IN ({placeholders})')
                
    #             # Add actual values to params list
    #             query_params.extend(values)

    #     # If we successfully created clauses, join them
    #     if where_clauses:
    #         where_sql = "WHERE " + " AND ".join(where_clauses)
    where_sql = ""
    query_params = []

    if filter_options and isinstance(filter_options, dict):
        or_groups = []          # list of AND groups joined by OR
        current_and_group = []

        for column, data in filter_options.items():

            # Normalize input
            if isinstance(data, dict):
                values = data.get("values", [])
                operator = data.get("operator", "AND").upper()
            else:
                values = data
                operator = "AND"

            if not values:
                continue

            # Create placeholders for parametrized SQL
            placeholders = ", ".join(["%s"] * len(values))
            condition = f"\"{column}\" IN ({placeholders})"

            # Add params
            query_params.extend(values)

            # Add to current AND group
            current_and_group.append(condition)

            # Close AND group on OR
            if operator == "OR":
                or_groups.append("(" + " AND ".join(current_and_group) + ")")
                current_and_group = []

        # Add remaining AND group
        if current_and_group:
            or_groups.append("(" + " AND ".join(current_and_group) + ")")

        # Build final WHERE clause
        if or_groups:
            where_sql = "WHERE " + " OR ".join(or_groups)


    # --- 2. Check Data Type ---
    # (Checking column type to decide between COUNT or SUM/AVG)
    cur.execute("""
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s
    """, (table_Name, x_axis))
    
    row = cur.fetchone()
    
    if not row:
        print(f"Error: Column {x_axis} not found in table {table_Name}")
        cur.close()
        conn.close()
        return {"total_x_axis": 0}

    column_type = row[0]

    # 3. Construct Final Query
    # If where_sql is "", it simply acts as whitespace
    
    if aggregate_py.lower() == 'distinct count':
        query = f"""
            SELECT COUNT(DISTINCT "{x_axis}") AS total_x_axis
            FROM "{table_Name}"
            {where_sql}
        """
    elif aggregate_py.lower() == 'count':
        query = f"""
            SELECT COUNT("{x_axis}") AS total_x_axis
            FROM "{table_Name}"
            {where_sql}
        """
    elif column_type == 'character varying' or column_type == 'text':
        # For text, we usually Count Distinct if no specific aggregate matched above
        query = f"""
            SELECT COUNT(DISTINCT "{x_axis}") AS total_x_axis
            FROM "{table_Name}"
            {where_sql}
        """
    else:
        # For numbers, we use the requested aggregate (SUM, AVG, etc)
        query = f"""
            SELECT {aggregate_py}("{x_axis}") AS total_x_axis
            FROM "{table_Name}"
            {where_sql}
        """

    print("Final Query SQL:", query)
    print("Query Parameters:", query_params)

    # --- 4. Execute ---
    try:
        cur.execute(query, tuple(query_params))
        result = cur.fetchone()
        data = {"total_x_axis": result[0] if result and result[0] is not None else 0}
    except Exception as e:
        print("SQL Execution Error:", e)
        data = {"total_x_axis": 0}
    finally:
        cur.close()
        conn.close()

    return data
# def fetchText_data(databaseName, table_Name, x_axis, aggregate_py, selectedUser, filter_options):
#     print(f"Fetch Data Triggered: Agg={aggregate_py}, Table={table_Name}, Filters={filter_options}")

#     conn = get_db_connection_or_path(selectedUser, databaseName)
#     cur = conn.cursor()

#     where_sql = ""
#     query_params = []

#     # ---------------------- FILTER CLAUSE (AND / OR SUPPORT) ----------------------
#     if filter_options and isinstance(filter_options, dict):
#         where_and = []
#         where_or = []

#         for col, data in filter_options.items():
#             # Normalize input
#             if isinstance(data, dict):
#                 values = data.get("values", [])
#                 operator = data.get("operator", "AND").upper()
#             else:
#                 values = data
#                 operator = "AND"

#             if not values:
#                 continue

#             placeholders = ", ".join(["%s"] * len(values))
#             condition = f"\"{col}\" IN ({placeholders})"

#             if operator == "OR":
#                 where_or.append(condition)
#             else:
#                 where_and.append(condition)

#             query_params.extend(values)

#         # Build WHERE clause
#         if where_and and where_or:
#             where_sql = (
#                 "WHERE " + " AND ".join(where_and)
#                 + " AND (" + " OR ".join(where_or) + ")"
#             )
#         elif where_or:
#             where_sql = "WHERE " + " OR ".join(where_or)
#         elif where_and:
#             where_sql = "WHERE " + " AND ".join(where_and)

#     # ---------------------- CHECK COLUMN TYPE ----------------------
#     cur.execute("""
#         SELECT data_type 
#         FROM information_schema.columns 
#         WHERE table_name = %s AND column_name = %s
#     """, (table_Name, x_axis))

#     row = cur.fetchone()

#     if not row:
#         print(f"Error: Column {x_axis} not found in table {table_Name}")
#         cur.close()
#         conn.close()
#         return {"total_x_axis": 0}

#     column_type = row[0]

#     # ---------------------- FINAL QUERY ----------------------
#     if column_type in ('character varying', 'text'):
#         query = f"""
#             SELECT COUNT(DISTINCT "{x_axis}") AS total_x_axis
#             FROM "{table_Name}"
#             {where_sql}
#         """
#     else:
#         query = f"""
#             SELECT {aggregate_py}("{x_axis}") AS total_x_axis
#             FROM "{table_Name}"
#             {where_sql}
#         """

#     print("Final Query SQL:", query)
#     print("Query Parameters:", query_params)

#     # ---------------------- EXECUTE ----------------------
#     try:
#         cur.execute(query, tuple(query_params))
#         result = cur.fetchone()
#         data = {"total_x_axis": result[0] if result and result[0] is not None else 0}
#     except Exception as e:
#         print("SQL Execution Error:", e)
#         data = {"total_x_axis": 0}
#     finally:
#         cur.close()
#         conn.close()

#     return data


def Hierarchial_drill_down(clicked_category, x_axis_columns, y_axis_column, depth, aggregation):
    global global_df
    if global_df is None:
        print("DataFrame not initialized for drill-down.")
        return {"error": "Data not initialized for drill-down."}

    print(f"Drill-Down Logic Start: Current Depth: {depth}, Clicked Category: {clicked_category}")

    # Determine the column to filter BY at the current depth
    filter_by_column = x_axis_columns[depth]
    print(f"Filtering by column: '{filter_by_column}' for category: '{clicked_category}'")

    # Filter the DataFrame based on the clicked category
    # Ensure case-insensitivity for robustness
    # Also, ensure the column actually exists before attempting to filter
    if filter_by_column not in global_df.columns:
        return {"error": f"Hierarchy column '{filter_by_column}' not found in DataFrame."}

    filtered_df = global_df[global_df[filter_by_column].astype(str).str.lower() == str(clicked_category).lower()]

    if filtered_df.empty:
        print(f"No data found for category '{clicked_category}' in column '{filter_by_column}'.")
        # If no data is found for the clicked category, return an empty set or a specific error
        return {"categories": [], "values": [], "error": f"No deeper data for '{clicked_category}'."}

    # Determine the column for the NEXT level
    next_depth = depth + 1
    if next_depth >= len(x_axis_columns):
        # This means we've reached the deepest level defined by x_axis_columns
        # In this case, we usually want to show the final aggregated values for the clicked_category itself
        print(f"Reached last level ({depth}). Returning aggregated data for '{clicked_category}'.")

        # Aggregate the filtered_df by the current filter_by_column
        if aggregation == 'count':
            aggregated_data = filtered_df.groupby(filter_by_column).size().reset_index(name='count')
        elif aggregation == 'sum':
            aggregated_data = filtered_df.groupby(filter_by_column)[y_axis_column[0]].sum().reset_index()
        elif aggregation == 'mean':
            aggregated_data = filtered_df.groupby(filter_by_column)[y_axis_column[0]].mean().reset_index()
        else:
            return {"error": "Unsupported aggregation method."}

        return {
            "categories": aggregated_data[filter_by_column].tolist(),
            "values": aggregated_data['count'].tolist() if aggregation == 'count' else aggregated_data[y_axis_column[0]].tolist()
        }

    # If not the last level, get the next column to group by
    next_level_column = x_axis_columns[next_depth]
    print(f"Moving to next level: '{next_level_column}'")

    if next_level_column not in global_df.columns:
        return {"error": f"Next hierarchy column '{next_level_column}' not found in DataFrame."}

    # Aggregate the filtered data for the next level
    if aggregation == 'count':
        aggregated_df = filtered_df.groupby(next_level_column).size().reset_index(name='count')
    elif aggregation == 'sum':
        aggregated_df = filtered_df.groupby(next_level_column)[y_axis_column[0]].sum().reset_index()
    elif aggregation == 'mean':
        aggregated_df = filtered_df.groupby(next_level_column)[y_axis_column[0]].mean().reset_index()
    else:
        return {"error": "Unsupported aggregation method."}

    if aggregated_df.empty:
        print(f"No data available at depth {next_depth} for column {next_level_column} after filtering by '{clicked_category}'.")
        # If there's no data for the next level, you might want to return the current level's filtered data,
        # or an empty result, depending on desired UI behavior.
        # For now, returning empty to indicate no drill-down possible.
        return {"categories": [], "values": [], "error": f"No further drill-down data for '{clicked_category}' under '{next_level_column}'."}

    print(f"Aggregated data for next level ({next_level_column}):")
    print(aggregated_df.head())

    result = {
        "categories": aggregated_df[next_level_column].tolist(),
        "values": aggregated_df['count'].tolist() if aggregation == 'count' else aggregated_df[y_axis_column[0]].tolist()
        # "next_level_column": next_level_column # This might not be needed by frontend for direct rendering
    }
    return result


def fetch_hierarchical_data(table_name, db_name,selectedUser):
    global global_df

    if global_df is None:
        print("Fetching data from the database...")
        try:
           
            conn = get_db_connection_or_path(selectedUser, db_name)
            cur = conn.cursor()

            print("conn",conn)
            query = f"SELECT * FROM {table_name}"
            print("query",query)
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            
            if not data:
                print("No data returned from the query.")
                return None  # or handle this case as needed

            global_df = pd.DataFrame(data, columns=colnames)
            print("Full DataFrame loaded with rows:", len(global_df))
            print(global_df.head())
            
            # Convert the y-axis column to numeric if necessary
            y_axis_column = 'Specify your y_axis_column here'  # Update with the actual y-axis column name if necessary
            if y_axis_column in global_df.columns:
                global_df[y_axis_column] = pd.to_numeric(global_df[y_axis_column], errors='coerce')
            else:
                print(f"Warning: Column {y_axis_column} not found in DataFrame columns.")

        except psycopg2.Error as db_err:
            print("Database error:", db_err)
            return None  # Handle database connection issues
        
        except Exception as e:
            print("An unexpected error occurred:", str(e))
            return None
        
        finally:
            cur.close()
            conn.close()

    return global_df

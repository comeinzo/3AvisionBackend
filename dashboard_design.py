import psycopg2
from flask import jsonify
from config import USER_NAME, DB_NAME, PASSWORD, HOST, PORT
# import psycopg2
import re
import psycopg2
import pandas as pd
import paramiko
import socket
import threading
def get_database_table_names(db_name, username, password, host='localhost', port='5432'):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name != 'datasource'")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_name NOT IN ('employee_list', 'datasource','category','role','role_permission','user')
        """)
                           

        table_names = cursor.fetchall()
        # print("table names",table_names)
        for table in table_names:
            print(table[0])
        cursor.close()
        conn.close()
        table_names = [table[0] for table in table_names]
        return jsonify(table_names)
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database.")
        print(e)
        return jsonify([])
    



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

def get_column_names(db_name, username, password, table_name, host='localhost', port='5432'):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Get the column names without fetching data
        column_names = [desc[0] for desc in cursor.description]

        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=column_names)

        # Print all column names
        # print("All column names in the dataframe:")
        # print(df.columns.tolist())

        # Try to convert columns to numeric where possible
        for column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='ignore')

        numeric_columns = df.select_dtypes(include=[float, int]).columns.tolist()
        text_columns = df.select_dtypes(include=[object]).columns.tolist()

        # print("Numeric columns are:", numeric_columns)
        # print("=====================================")
        # print("Text columns are:", text_columns)

        numeric_columns_cleaned = {}
        text_columns_cleaned = {}

        for column_name in numeric_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            numeric_columns_cleaned[column_name] = cleaned_values
            # print("numeric column=============", list(numeric_columns_cleaned.keys()))
            num_columns=list(numeric_columns_cleaned.keys())

        for column_name in text_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            text_columns_cleaned[column_name] = cleaned_values
            # print("text column>>>>>>>>>>>>>>>>", list(text_columns_cleaned.keys()))
            txt_columns=list(text_columns_cleaned.keys())

        cursor.close()
        conn.close()

        return {
            'numeric_columns': num_columns,
            'text_columns':txt_columns
        }
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database.")
        print(e)
        return {'numeric_columns': [], 'text_columns': []}
def fetch_external_db_connection(company_name,savename):
    try:
        print("company_name",company_name)
        # Connect to local PostgreSQL to get external database connection details
        conn = psycopg2.connect(
           dbname=company_name,  # Ensure this is the correct company database
        user=USER_NAME,password=PASSWORD,host=HOST,port=PORT
        )
        print("conn",conn)
        cursor = conn.cursor()
        query = """
            SELECT * 
            FROM external_db_connections 
            WHERE savename = %s 
            ORDER BY created_at DESC 
            LIMIT 1;
        """
        print("query",query)
        cursor.execute(query, (savename,))
        connection_details = cursor.fetchone()
        conn.close()
        return connection_details
    except Exception as e:
        print(f"Error fetching connection details: {e}")
        return None

def get_db_connection_or_path(selected_user, company_name, return_path=False):
    """
    Returns either:
      - a psycopg2 connection (default)
      - or a connection path string (if return_path=True)
    Handles both LOCAL and EXTERNAL DB connections.
    """

    # ✅ 1️⃣ LOCAL CONNECTION
    # if not selected_user or selected_user.lower() in ("local", "null", "none"):
    if isinstance(selected_user, tuple):
        selected_user = selected_user[0]

    # ✅ 1️⃣ LOCAL CONNECTION
    if not selected_user or str(selected_user).lower() in ("local", "null", "none"):
        print("🟢 Using local database connection...")

        connection_path = (
            f"dbname={company_name} user={USER_NAME} password={PASSWORD} host={HOST} port={PORT}"
        )

        if return_path:
            print(f"🔗 Local Connection Path: {connection_path}")
            return connection_path

        connection = psycopg2.connect(connection_path)
        print("✅ Local PostgreSQL connection established successfully!")
        return connection

    # ✅ 2️⃣ EXTERNAL CONNECTION
    print(f"🟡 Using external database connection for user: {selected_user}")
    connection_details = fetch_external_db_connection(company_name, selected_user)

    if not connection_details:
        raise Exception(f"❌ Unable to fetch external database connection details for user '{selected_user}'")

    db_details = {
        "name": connection_details[1],
        "dbType": connection_details[2],
        "host": connection_details[3],
        "user": connection_details[4],
        "password": connection_details[5],
        "port": int(connection_details[6]),
        "database": connection_details[7],
        "use_ssh": connection_details[8],
        "ssh_host": connection_details[9],
        "ssh_port": int(connection_details[10]),
        "ssh_username": connection_details[11],
        "ssh_key_path": connection_details[12],
    }

    print(f"🔹 External DB Connection Details: {db_details}")

    ssh_client = None
    local_sock = None
    stop_event = threading.Event()

    # ✅ SSH Tunnel Setup if required
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
            timeout=10,
        )

        # Find free local port for tunnel
        local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        local_sock.bind(("127.0.0.1", 0))
        local_port = local_sock.getsockname()[1]
        local_sock.listen(1)
        print(f"✅ Local forwarder listening on 127.0.0.1:{local_port}")

        transport = ssh_client.get_transport()

        def pipe(src, dst):
            try:
                while True:
                    data = src.recv(1024)
                    if not data:
                        break
                    dst.sendall(data)
            except Exception:
                pass
            finally:
                src.close()
                dst.close()

        def forward_tunnel():
            while not stop_event.is_set():
                try:
                    client_sock, _ = local_sock.accept()
                    chan = transport.open_channel(
                        "direct-tcpip",
                        ("127.0.0.1", 5432),
                        client_sock.getsockname(),
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

        # Override host and port to tunnel
        host = "127.0.0.1"
        port = local_port
    else:
        host = db_details["host"]
        port = db_details["port"]

    # ✅ Create connection path
    connection_path = (
        f"dbname={db_details['database']} user={db_details['user']} "
        f"password={db_details['password']} host={host} port={port}"
    )

    if return_path:
        print(f"🔗 External Connection Path: {connection_path}")
        return connection_path

    print(f"🧩 Connecting to external PostgreSQL at {host}:{port} ...")

    connection = psycopg2.connect(
        dbname=db_details["database"],
        user=db_details["user"],
        password=db_details["password"],
        host=host,
        port=port,
    )

    print("✅ External PostgreSQL connection established successfully!")
    return connection
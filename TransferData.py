
from flask import request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import pytz
import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql
from psycopg2 import extras
from flask import Flask
from io import StringIO # For creating a DataFrame from database results
import numpy as np # Added for NumPy type checking
import psycopg2.extras # Needed for execute_values
app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()

# Sanitize column names
def sanitize_column_name(col_name):
    if isinstance(col_name, str):
        return re.sub(r'\W+', '_', col_name).lower()
    else:
        return col_name

# # Clean DataFrame
# def clean_data(df):
#     df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
#     return df
# Clean DataFrame
def clean_data(df):
    # Iterate through columns and apply strip only if the column actually contains string data
    for col in df.columns:
        if df[col].dtype == "object":
            # Check if all values in the column are strings before applying .str accessor
            # This is safer than just checking dtype == "object"
            if df[col].apply(lambda x: isinstance(x, str)).all():
                df[col] = df[col].str.strip()
            else:
                # If there are non-string values, you might want to convert them to string first
                # or handle them differently depending on your data cleaning requirements.
                # For example, to convert to string before stripping (be careful with non-string data):
                # df[col] = df[col].astype(str).str.strip()
                # Or, more selectively, only apply to strings within the column:
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df
# Function to determine SQL data type from pandas dtype
def determine_sql_data_type(value):
    date_pattern = r"^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$"
    if pd.api.types.is_string_dtype(value):
        if value.str.match(date_pattern).any():
            return 'DATE'
        return 'VARCHAR'
    elif pd.api.types.is_integer_dtype(value):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(value):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(value):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(value):
        return 'DATE'
    else:
        return 'VARCHAR'

# Function to fetch columns from a database table
def fetch_table_columns(db_config, table_name):
    conn = None
    columns = []
    error_message = None
    try:
        if db_config['dbType'] == 'PostgreSQL':
            conn = psycopg2.connect(dbname=db_config['dbName'], user=db_config['dbUsername'], password=db_config['dbPassword'], host=db_config['provider'] or 'localhost', port=db_config['port'] or '5432')
            cur = conn.cursor()
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = 'public'")
            columns = [row[0] for row in cur.fetchall()]
        elif db_config['dbType'] == 'MySQL':
            import mysql.connector
            conn = mysql.connector.connect(host=db_config['provider'] or 'localhost', port=db_config['port'] or '3306', database=db_config['dbName'], user=db_config['dbUsername'], password=db_config['dbPassword'])
            cursor = conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row[0] for row in cursor.fetchall()]
        elif db_config['dbType'] == 'MongoDB':
            from pymongo import MongoClient
            client = MongoClient(f"mongodb://{db_config['dbUsername']}:{db_config['dbPassword']}@{db_config['provider'] or 'localhost'}:{db_config['port'] or '27017'}/{db_config['dbName']}")
            db = client[db_config['dbName']]
            first_doc = db[table_name].find_one()
            if first_doc:
                columns = list(first_doc.keys())
            client.close()
        elif db_config['dbType'] == 'Oracle':
            import cx_Oracle
            dsn_tns = cx_Oracle.makedsn(db_config['provider'] or 'localhost', db_config['port'] or '1521', service_name=db_config['dbName'])
            conn = cx_Oracle.connect(user=db_config['dbUsername'], password=db_config['dbPassword'], dsn_tns=dsn_tns)
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name FROM user_tab_cols WHERE table_name = '{table_name.upper()}'")
            columns = [row[0] for row in cursor.fetchall()]
        elif db_config['dbType'] in ['MSSQL', 'SQLServer', 'sqlserver', 'mssql']:
            import pyodbc
            server = db_config['provider'] or 'localhost'
            sql_port = db_config['port'] or '1433'
            conn_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={server},{sql_port};"
                f"DATABASE={db_config['dbName']};"
                f"UID={db_config['dbUsername']};"
                f"PWD={db_config['dbPassword']};"
                "TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
            """)
            columns = [row[0] for row in cursor.fetchall()]

        else:
            error_message = f"Unsupported database type: {db_config['dbType']}"
    except Exception as e:
        error_message = str(e)
    finally:
        if conn and db_config['dbType'] not in ['MongoDB']:
            conn.close()
    return columns, error_message
def fetch_mysql_column_types(db_config, table_name):
    import mysql.connector

    conn = mysql.connector.connect(
        host=db_config['provider'] or 'localhost',
        port=db_config['port'] or '3306',
        database=db_config['dbName'],
        user=db_config['dbUsername'],
        password=db_config['dbPassword']
    )

    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
    """, (table_name, db_config['dbName']))

    columns = {}
    for row in cursor.fetchall():
        columns[row["COLUMN_NAME"].lower()] = {
            "type": row["DATA_TYPE"].lower(),
            "length": row["CHARACTER_MAXIMUM_LENGTH"],
            "precision": row["NUMERIC_PRECISION"],
            "scale": row["NUMERIC_SCALE"]
        }

    conn.close()
    return columns
def mysql_to_postgres_type(col_info):
    t = col_info["type"]

    if t in ["int", "integer"]:
        return "INTEGER"
    elif t == "bigint":
        return "BIGINT"
    elif t == "smallint":
        return "SMALLINT"
    elif t == "tinyint":
        return "BOOLEAN"
    elif t in ["float", "double"]:
        return "DOUBLE PRECISION"
    elif t == "decimal":
        return f"NUMERIC({col_info['precision']},{col_info['scale']})"
    elif t in ["varchar", "char"]:
        return f"VARCHAR({col_info['length']})"
    elif t in ["text", "mediumtext", "longtext"]:
        return "TEXT"
    elif t == "json":
        return "JSONB"
    elif t == "date":
        return "DATE"
    elif t in ["datetime", "timestamp"]:
        return "TIMESTAMP"
    elif t == "time":
        return "TIME"
    elif t in ["blob", "binary", "varbinary"]:
        return "BYTEA"
    else:
        return "TEXT"

def fetch_oracle_column_types(db_config, table_name):
    import cx_Oracle

    dsn = cx_Oracle.makedsn(
        db_config['provider'],
        db_config['port'],
        service_name=db_config['dbName']
    )

    conn = cx_Oracle.connect(
        user=db_config['dbUsername'],
        password=db_config['dbPassword'],
        dsn=dsn
    )

    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            DATA_LENGTH,
            DATA_PRECISION,
            DATA_SCALE
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :table
    """, table=table_name.upper())

    columns = {}
    for row in cursor.fetchall():
        columns[row[0].lower()] = {
            "type": row[1].lower(),
            "length": row[2],
            "precision": row[3],
            "scale": row[4]
        }

    conn.close()
    return columns

def oracle_to_postgres_type(col_info):
    t = col_info["type"]

    if t == "number":
        if col_info["scale"] == 0:
            return "INTEGER"
        elif col_info["precision"]:
            return f"NUMERIC({col_info['precision']},{col_info['scale']})"
        return "NUMERIC"
    elif t in ["varchar2", "nvarchar2", "char"]:
        return f"VARCHAR({col_info['length']})"
    elif t in ["clob", "nclob"]:
        return "TEXT"
    elif t == "date":
        return "TIMESTAMP"
    elif t.startswith("timestamp"):
        return "TIMESTAMP"
    elif t == "blob":
        return "BYTEA"
    elif t == "raw":
        return "BYTEA"
    else:
        return "TEXT"

def mongo_to_postgres_type(series: pd.Series):
    non_null = series.dropna()
    if non_null.empty:
        return "TEXT"

    sample = non_null.iloc[0]

    if isinstance(sample, bool):
        return "BOOLEAN"
    if isinstance(sample, int):
        return "INTEGER"
    if isinstance(sample, float):
        return "DOUBLE PRECISION"
    if isinstance(sample, dict):
        return "JSONB"
    if isinstance(sample, list):
        return "JSONB"
    if isinstance(sample, bytes):
        return "BYTEA"
    if isinstance(sample, datetime):
        return "TIMESTAMP"

    max_len = non_null.astype(str).str.len().max()
    return f"VARCHAR({max_len})" if max_len <= 255 else "TEXT"

def fetch_mssql_column_types(db_config, table_name):
    import pyodbc

    server = db_config['provider'] or 'localhost'
    sql_port = db_config['port'] or '1433'
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server},{sql_port};"
        f"DATABASE={db_config['dbName']};"
        f"UID={db_config['dbUsername']};"
        f"PWD={db_config['dbPassword']};"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
    """, table_name)

    columns = {}
    for row in cursor.fetchall():
        columns[row.COLUMN_NAME.lower()] = {
            "type": row.DATA_TYPE.lower(),
            "length": row.CHARACTER_MAXIMUM_LENGTH,
            "precision": row.NUMERIC_PRECISION,
            "scale": row.NUMERIC_SCALE
        }

    conn.close()
    return columns
def mssql_to_postgres_type(col_info):
    t = col_info["type"]

    if t in ["int"]:
        return "INTEGER"
    elif t == "bigint":
        return "BIGINT"
    elif t == "smallint":
        return "SMALLINT"
    elif t == "bit":
        return "BOOLEAN"
    elif t in ["float"]:
        return "DOUBLE PRECISION"
    elif t == "real":
        return "REAL"
    elif t in ["decimal", "numeric"]:
        return f"NUMERIC({col_info['precision']},{col_info['scale']})"
    elif t in ["money", "smallmoney"]:
        return "NUMERIC(19,4)"
    elif t in ["varchar", "nvarchar"]:
        if col_info["length"] == -1:
            return "TEXT"
        return f"VARCHAR({col_info['length']})"
    elif t in ["char", "nchar"]:
        return f"CHAR({col_info['length']})"
    elif t in ["text", "ntext"]:
        return "TEXT"
    elif t == "date":
        return "DATE"
    elif t in ["datetime", "datetime2", "smalldatetime"]:
        return "TIMESTAMP"
    elif t == "datetimeoffset":
        return "TIMESTAMPTZ"
    elif t == "time":
        return "TIME"
    elif t == "uniqueidentifier":
        return "UUID"
    elif t in ["varbinary", "binary", "image"]:
        return "BYTEA"
    else:
        return "TEXT"

def fetch_data_with_columns(db_config, table_name, selected_columns=None, chunk_size=100000):
    conn = None
    all_data_chunks = []
    offset = 0
    error_message = None

    try:
        # POSTGRESQL
        if db_config['dbType'] == 'PostgreSQL':
            conn = psycopg2.connect(
                dbname=db_config['dbName'],
                user=db_config['dbUsername'],
                password=db_config['dbPassword'],
                host=db_config['provider'] or 'localhost',
                port=db_config['port'] or '5432'
            )

            primary_keys = get_primary_key_columns(conn, table_name)
            print("primary_keys:", primary_keys)

            if not primary_keys:
                raise ValueError(f"No primary key found for table '{table_name}'")
            primary_key = primary_keys[0]  # use first key for ORDER BY

            if selected_columns and primary_key not in selected_columns:
                selected_columns.insert(0, primary_key)
            elif not selected_columns:
                selected_columns = [primary_key]

            columns_str = ", ".join([f'"{col}"' for col in selected_columns])

            while True:
                sql_query = f'SELECT {columns_str} FROM "{table_name}" ORDER BY "{primary_key}" ASC LIMIT {chunk_size} OFFSET {offset}'
                print(f"Executing query: {sql_query}")
                chunk_df = pd.read_sql_query(sql_query, conn)

                if chunk_df.empty:
                    print("No more data to fetch.")
                    break

                all_data_chunks.append(chunk_df)
                offset += chunk_size

        # MYSQL
        elif db_config['dbType'] == 'MySQL':
            import mysql.connector
            conn = mysql.connector.connect(
                host=db_config['provider'] or 'localhost',
                port=db_config['port'] or '3306',
                database=db_config['dbName'],
                user=db_config['dbUsername'],
                password=db_config['dbPassword']
            )

            if not selected_columns:
                selected_columns = ['id'] + [f"col{i}" for i in range(1, 51)]

            columns_str = ", ".join([f"`{col}`" for col in selected_columns])

            while True:
                safe_columns = []
                for col in selected_columns:
                    if col.lower() == "datetimeoffset_col":
                        safe_columns.append(f"CAST([{col}] AS DATETIME2) AS [{col}]")
                    else:
                        safe_columns.append(f"[{col}]")

                columns_str = ", ".join(safe_columns)

                sql_query = f"""
                SELECT {columns_str}
                FROM [{table_name}]
                ORDER BY [{primary_key}] ASC
                OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY
                """

                # sql_query = f"SELECT {columns_str} FROM `{table_name}` ORDER BY `id` ASC LIMIT {chunk_size} OFFSET {offset}"
                print(f"Executing query: {sql_query}")
                chunk_df = pd.read_sql_query(sql_query, conn)

                if chunk_df.empty:
                    break

                all_data_chunks.append(chunk_df)
                offset += chunk_size

        # MONGODB
        elif db_config['dbType'] == 'MongoDB':
            from pymongo import MongoClient
            client = MongoClient(f"mongodb://{db_config['dbUsername']}:{db_config['dbPassword']}@{db_config['provider'] or 'localhost'}:{db_config['port'] or '27017'}/{db_config['dbName']}")
            db = client[db_config['dbName']]
            projection = {col: 1 for col in selected_columns} if selected_columns else {}

            while True:
                cursor = db[table_name].find({}, projection).skip(offset).limit(chunk_size)
                chunk_list = list(cursor)

                if not chunk_list:
                    break

                chunk_df = pd.DataFrame(chunk_list)
                all_data_chunks.append(chunk_df)
                offset += len(chunk_list)

            client.close()

        # ORACLE
        elif db_config['dbType'] == 'Oracle':
            import cx_Oracle
            dsn = cx_Oracle.makedsn(db_config['provider'], db_config['port'], service_name=db_config['dbName'])
            conn = cx_Oracle.connect(db_config['dbUsername'], db_config['dbPassword'], dsn)

            if not selected_columns:
                selected_columns = ['ID'] + [f"COL{i}" for i in range(1, 51)]

            columns_str = ", ".join([col.upper() for col in selected_columns])

            while True:
                sql_query = f"SELECT {columns_str} FROM {table_name.upper()} ORDER BY ID ASC OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
                print(f"Executing query: {sql_query}")
                chunk_df = pd.read_sql_query(sql_query, conn)

                if chunk_df.empty:
                    break

                all_data_chunks.append(chunk_df)
                offset += chunk_size
        # MSSQL
        elif db_config['dbType'] in ['MSSQL', 'SQLServer', 'sqlserver', 'mssql']:
            print("MSSQL")
            import pyodbc

            server = db_config['provider'] or 'localhost'
            sql_port = db_config['port'] or '1433'
            conn_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={server},{sql_port};"
                f"DATABASE={db_config['dbName']};"
                f"UID={db_config['dbUsername']};"
                f"PWD={db_config['dbPassword']};"
                "TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(conn_str, timeout=10)

            # Determine primary key or fallback
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
                ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
                WHERE TC.TABLE_NAME = '{table_name}' AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """)
            primary_keys = [row[0] for row in cursor.fetchall()]
            print("primary_keys",primary_keys)
            primary_key = primary_keys[0] if primary_keys else None

            if selected_columns:
                if primary_key and primary_key not in selected_columns:
                    selected_columns.insert(0, primary_key)
            else:
                selected_columns = [primary_key] if primary_key else ['*']

            # columns_str = ", ".join([f"[{col}]" for col in selected_columns])
            safe_columns = []
            for col in selected_columns:
                if col.lower() == "datetimeoffset_col":
                    safe_columns.append(f"CAST([{col}] AS DATETIME2(7)) AS [{col}]")
                else:
                    safe_columns.append(f"[{col}]")

            columns_str = ", ".join(safe_columns)


            while True:
                # SQL Server OFFSET-FETCH pagination
                if primary_key:
                    sql_query = f"SELECT {columns_str} FROM [{table_name}] ORDER BY [{primary_key}] ASC OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
                else:
                    # fallback if no PK
                    sql_query = f"SELECT {columns_str} FROM [{table_name}]"

                print(f"Executing query: {sql_query}")
                chunk_df = pd.read_sql_query(sql_query, conn)

                if chunk_df.empty:
                    break

                all_data_chunks.append(chunk_df)
                offset += chunk_size


        else:
            error_message = f"Unsupported database type: {db_config['dbType']}"

    except Exception as e:
        error_message = f"Error fetching data from {db_config['dbType']}: {str(e)}"
    finally:
        if conn and db_config['dbType'] not in ['MongoDB']:
            conn.close()

    if all_data_chunks:
        df = pd.concat(all_data_chunks, ignore_index=True)
        df = df.replace({None: pd.NA})
    else:
        df = pd.DataFrame()

    return df, error_message

def get_destination_table_columns(db_config, table_name):
    conn = None
    columns = set()
    try:
        if db_config['dbType'] == 'PostgreSQL':
            conn = psycopg2.connect(dbname=db_config['dbName'], user=db_config['dbUsername'], password=db_config['dbPassword'], host=db_config['provider'] or 'localhost', port=db_config['port'] or '5432')
            cur = conn.cursor()
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = 'public'")
            columns = {row[0] for row in cur.fetchall()}
        # Add similar logic for other database types if needed
    except Exception as e:
        print(f"Error getting destination table columns: {e}")
    finally:
        if conn:
            conn.close()
    return columns


def get_primary_key_columns(conn, table_name):
    primary_keys = []
    try:
        cur = conn.cursor()
        sql = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisprimary;
        """
        cur.execute(sql, (table_name,))
        primary_keys = [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Error getting primary key columns: {e}")
    return primary_keys

def get_primary_key_columns_dest(db_config, table_name):
    print("db_config",db_config)
    print("table_name",table_name)
    conn = None
    primary_keys = []
    try:
        if db_config['dbType'] == 'PostgreSQL':
            conn = psycopg2.connect(
                dbname=db_config['dbName'],
                user=db_config['dbUsername'],
                password=db_config['dbPassword'],
                host=db_config['provider'] or 'localhost',
                port=db_config['port'] or '5432'
            )
            cur = conn.cursor()
            print("cur",cur)
            sql = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                AND i.indisprimary;
            """
            print("sql",sql)
            cur.execute(sql, (table_name,))
            primary_keys = [row[0] for row in cur.fetchall()]
        elif db_config['dbType'].lower() in ['mssql', 'sqlserver']:
            import pyodbc
            server = db_config['provider'] or 'localhost'
            sql_port = db_config['port'] or '1433'
            conn_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={server},{sql_port};"
                f"DATABASE={db_config['dbName']};"
                f"UID={db_config['dbUsername']};"
                f"PWD={db_config['dbPassword']};"
                "TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            sql = f"""
                SELECT k.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                WHERE t.TABLE_NAME = '{table_name}' 
                AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND t.TABLE_SCHEMA = 'dbo';
            """
            print("MSSQL primary key SQL:", sql)
            cursor.execute(sql)
            primary_keys = [row[0] for row in cursor.fetchall()]
           
            print("primary_keys",primary_keys)
    except Exception as e:
        print(f"Error getting primary key columns: {e}")
    finally:
        if conn:
            conn.close()
    return primary_keys


def _convert_to_native_types(value):
    """
    Converts a value to its Python native type if it's a NumPy scalar,
    otherwise returns the value as is. Handles pandas NaN by converting to None.
    """
    if pd.isna(value):
        return None
    elif isinstance(value, (np.integer, np.floating, np.bool_)):
        return value.item() # Convert NumPy scalar to Python scalar
    else:
        return value


def insert_dataframe_with_upsert(db_config, dest_table_name, source_df,source_config, source_table_name=None, selected_columns=None, create_view_if_exists=False):
    """
    Inserts or updates (upserts) data from a pandas DataFrame into a PostgreSQL table
    using a temporary staging table for bulk loading. Updates only if data in
    non-primary key columns is different.

    Args:
        db_config (dict): Database connection configuration for the destination.
        dest_table_name (str): The name of the destination table in PostgreSQL.
        source_df (pd.DataFrame): The DataFrame chunk containing the data to upsert.
        source_table_name (str, optional): The name of the source table (used for view creation).
                                             Required if `create_view_if_exists` is True.
        selected_columns (list, optional): List of columns from source_df to use.
                                             Defaults to all columns if None.
        create_view_if_exists (bool, optional): If True, attempts to create a view
                                                 from `source_table_name`. Defaults to False.

    Returns:
        tuple: (success (bool), error_message (str or None), view_created (bool), view_name (str or None))
    """
    conn = None
    cur = None
    success = False
    error_message = None
    view_created = False
    view_name = None

    inserted_count = 0
    updated_count = 0

    try:
        # Establish DB connection for destination
        conn = psycopg2.connect(
            dbname=db_config['dbName'],
            user=db_config['dbUsername'],
            password=db_config['dbPassword'],
            host=db_config['provider'] or 'localhost',
            port=db_config['port'] or '5432'
        )
        cur = conn.cursor()

        # Prepare DataFrame for insertion (already cleaned by the caller for chunking)
        df_to_insert = source_df[selected_columns].copy() if selected_columns else source_df.copy()
        
        # Ensure cleaning happens on this specific chunk
        df_cleaned = clean_data(df_to_insert)

        sanitized_columns = [sanitize_column_name(col) for col in df_cleaned.columns]
        column_mapping = dict(zip(df_cleaned.columns, sanitized_columns))
        df_cleaned.rename(columns=column_mapping, inplace=True)

        if df_cleaned.empty:
            print("Input DataFrame chunk is empty. No upsert operations performed.")
            return True, None, view_created, view_name

        # 1. Check if destination table exists and create if not
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s AND table_schema = 'public'
            )
        """, (dest_table_name,))
        dest_table_exists = cur.fetchone()[0]

        if not dest_table_exists:
            print(f"Table '{dest_table_name}' does not exist. Attempting to create it...")
            if not sanitized_columns:
                raise ValueError("DataFrame has no columns to create a table. Cannot create an empty table.")
            
            # if len(sanitized_columns) > 0:
            #     primary_key_for_new_table = [sanitized_columns[0]] 
            # else:
            #     raise ValueError("Cannot create table: no columns found in DataFrame.")
            source_pk_columns = []
            if source_table_name:
                print(f"Fetching primary keys from source table '{source_table_name}'...")
                try:
                    source_pk_columns = get_primary_key_columns_dest(source_config, source_table_name)
                    print(f"Source primary keys found: {source_pk_columns}")
                except Exception as pk_err:
                    print(f"Could not fetch primary keys from source table: {pk_err}")

            # If still empty, try to auto-detect based on common column names
            if not source_pk_columns:
                possible_pks = [col for col in sanitized_columns if col.lower() in ['id', 'pk', 'primary_id']]
                if possible_pks:
                    source_pk_columns = [possible_pks[0]]
                    print(f"No explicit PK found. Using heuristic primary key: {source_pk_columns[0]}")


            # columns_with_types = ', '.join(
            #     f"{col} {determine_sql_data_type(df_cleaned[col])}"
            #     for col in sanitized_columns
            # )
            # DEFAULT: existing behavior (DO NOT CHANGE)
            columns_with_types = []

            # MSSQL source → convert datatypes
            # if source_config["dbType"].lower() in ["mssql", "sqlserver"]:
            #     mssql_types = fetch_mssql_column_types(source_config, source_table_name)
            db_type = source_config["dbType"].lower()

            if db_type in ["mssql", "sqlserver"]:
                source_types = fetch_mssql_column_types(source_config, source_table_name)
                mapper = mssql_to_postgres_type

            elif db_type == "mysql":
                source_types = fetch_mysql_column_types(source_config, source_table_name)
                mapper = mysql_to_postgres_type

            elif db_type == "oracle":
                source_types = fetch_oracle_column_types(source_config, source_table_name)
                mapper = oracle_to_postgres_type
            else:
                source_types = None
                mapper = None


            for col in sanitized_columns:
                if source_types and col.lower() in source_types:
                    pg_type = mapper(source_types[col.lower()])
                elif db_type == "mongodb":
                    pg_type = mongo_to_postgres_type(df_cleaned[col])
                else:
                    pg_type = determine_sql_data_type(df_cleaned[col])

                columns_with_types.append(f"{col} {pg_type}")


            #     for col in sanitized_columns:
            #         col_info = mssql_types.get(col.lower())
            #         if col_info:
            #             pg_type = mssql_to_postgres_type(col_info)
            #         else:
            #             pg_type = determine_sql_data_type(df_cleaned[col])

            #         columns_with_types.append(f"{col} {pg_type}")

            # # All other sources → KEEP existing behavior
            # else:
            #     for col in sanitized_columns:
            #         columns_with_types.append(
            #             f"{col} {determine_sql_data_type(df_cleaned[col])}"
            #         )

            columns_with_types = ", ".join(columns_with_types)

            pk_constraint = f", PRIMARY KEY ({', '.join(source_pk_columns)})"
            create_table_query = f"CREATE TABLE {dest_table_name} ({columns_with_types}{pk_constraint})"
            cur.execute(create_table_query)
            conn.commit()
            print(f"Table '{dest_table_name}' created successfully with PK: {source_pk_columns}.")
            dest_table_exists = True

        # 2. Handle view creation if requested (does not stop upsert) - Simplified for brevity
        if create_view_if_exists and source_table_name:
            print(f"Attempting to create view from source table: {source_table_name} (implementation not shown).")
            pass # Placeholder

        # 3. Retrieve actual primary key columns from the *destination* table.
        primary_key_columns = get_primary_key_columns_dest(db_config, dest_table_name)
        
        if not primary_key_columns:
            raise ValueError(f"Table '{dest_table_name}' does not have a primary key defined. "
                             f"Cannot perform an upsert (update-if-different requires a primary key). "
                             f"Please define a PRIMARY KEY on '{dest_table_name}' in your database or create it with one.")

        if not all(col.strip('"') in [c.strip('"') for c in sanitized_columns] for col in primary_key_columns):
            raise ValueError(f"Primary key columns {primary_key_columns} fetched from the database "
                             f"are not all found in the DataFrame columns: {list(df_cleaned.columns)}. "
                             "Please ensure your DataFrame contains the necessary primary key columns for upsert.")
                             
        # --- Staging Table Approach for UPSERT ---

        # Generate a unique temporary table name for the session
        temp_table_name = f"temp_staging_{dest_table_name}_{conn.info.backend_pid}_{os.getpid()}"

        # Create a temporary staging table with the same schema as the destination table
        # INCLUDING ALL copies constraints, indexes, etc. but not data.
        create_temp_table_query = f"CREATE TEMPORARY TABLE {temp_table_name} (LIKE {dest_table_name} INCLUDING ALL);"
        cur.execute(create_temp_table_query)
        print(f"Created temporary staging table: {temp_table_name}")
       

        # Prepare data for bulk insertion into the temporary table
        data_values = []
        cols_for_insert = [col for col in sanitized_columns if col.strip('"') in [c.strip('"') for c in primary_key_columns] or col.strip('"') in [c.strip('"') for c in df_cleaned.columns]]

        # for _, row_series in df_cleaned[df_cleaned.columns.intersection([c.strip('"') for c in cols_for_insert])].iterrows():
        #     data_values.append(tuple(_convert_to_native_types(val) for val in row_series.values))
        cols_for_insert = [col for col in sanitized_columns if col in df_cleaned.columns]

        # data_values = [
        #     tuple(_convert_to_native_types(getattr(row, col)) for col in cols_for_insert)
        #     for row in df_cleaned[cols_for_insert].itertuples(index=False)
        # ]
        data_values = [
            tuple(_convert_to_native_types(val) for val in row)
            for row in df_cleaned[cols_for_insert].itertuples(index=False, name=None)
        ]


        print(f"df_cleaned columns: {df_cleaned.columns.tolist()}")
        print(f"Sanitized columns for insert: {cols_for_insert}")
        # Bulk load data into the temporary table using execute_values
        insert_temp_query = f"INSERT INTO {temp_table_name} ({', '.join(cols_for_insert)}) VALUES %s"
        extras.execute_values(cur, insert_temp_query, data_values, page_size=10000)
        print(f"Bulk loaded {len(df_cleaned)} rows into staging table '{temp_table_name}'.")

        # Perform UPSERT from staging table to destination table
        
        non_pk_columns = [col for col in sanitized_columns if col.strip('"') not in [pk.strip('"') for pk in primary_key_columns]]

        update_set_clause = []
        where_update_conditions = []
        for col in non_pk_columns:
            update_set_clause.append(f"{col} = STAGING.{col}")
            where_update_conditions.append(f"DEST.{col} IS DISTINCT FROM STAGING.{col}")
        
        pk_join_conditions = ' AND '.join([f'DEST.{pk} = STAGING.{pk}' for pk in primary_key_columns])

        # 4. Update existing rows in the destination table from the staging table
        if update_set_clause:
            update_query = f"""
                UPDATE {dest_table_name} AS DEST
                SET {', '.join(update_set_clause)}
                FROM {temp_table_name} AS STAGING
                WHERE {pk_join_conditions}
                AND ({' OR '.join(where_update_conditions)});
            """
            cur.execute(update_query)
            updated_count = cur.rowcount
            print(f"Updated {updated_count} existing rows in '{dest_table_name}'.")
        else:
            updated_count = 0
            print("No non-primary key columns to update based on schema.")

        # 5. Insert new rows from the staging table into the destination table
        # CORRECTED: Qualify column names with 'STAGING.' to avoid ambiguity
        qualified_sanitized_columns = [f"STAGING.{col}" for col in sanitized_columns]
        insert_new_query = f"""
            INSERT INTO {dest_table_name} ({', '.join(sanitized_columns)})
            SELECT {', '.join(qualified_sanitized_columns)}
            FROM {temp_table_name} AS STAGING
            LEFT JOIN {dest_table_name} AS DEST ON {pk_join_conditions}
            WHERE DEST.{primary_key_columns[0]} IS NULL;
        """
        cur.execute(insert_new_query)
        inserted_count = cur.rowcount
        print(f"Inserted {inserted_count} new rows into '{dest_table_name}'.")

        conn.commit()
        success = True

    except (psycopg2.Error, ValueError, Exception) as e:
        if conn:
            conn.rollback()
        error_message = str(e)
        print(f"Error during bulk upsert operation for chunk: {error_message}")
        success = False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        
        if 'df_cleaned' in locals():
            print(f"Chunk rows processed: {len(df_cleaned)}")
            print(f"Rows inserted: {inserted_count}")
            print(f"Rows updated: {updated_count}")
            print(f"Rows skipped (no change or already present): {len(df_cleaned) - inserted_count - updated_count}")
        else:
            print("Data cleaning failed. No rows processed.")
        print("----------------------------")
    return success, error_message, view_created, view_name,inserted_count, updated_count

def fetch_table_details(db_config, table_name):
    conn = None
    columns = []
    error_message = None
    try:
        if db_config['dbType'] == 'PostgreSQL':
            conn = psycopg2.connect(dbname=db_config['dbName'], user=db_config['dbUsername'], password=db_config['dbPassword'], host=db_config['provider'] or 'localhost', port=db_config['port'] or '5432')
            cur = conn.cursor()
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = 'public'")
            columns = [row[0] for row in cur.fetchall()]
        elif db_config['dbType'] == 'MySQL':
            import mysql.connector
            conn = mysql.connector.connect(host=db_config['provider'] or 'localhost', port=db_config['port'] or '3306', database=db_config['dbName'], user=db_config['dbUsername'], password=db_config['dbPassword'])
            cursor = conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row[0] for row in cursor.fetchall()]
        elif db_config['dbType'] == 'MongoDB':
            from pymongo import MongoClient
            client = MongoClient(f"mongodb://{db_config['dbUsername']}:{db_config['dbPassword']}@{db_config['provider'] or 'localhost'}:{db_config['port'] or '27017'}/{db_config['dbName']}")
            db = client[db_config['dbName']]
            # Get the first document to infer columns (not ideal for all cases)
            first_doc = db[table_name].find_one()
            if first_doc:
                columns = list(first_doc.keys())
            client.close()
        elif db_config['dbType'] == 'Oracle':
            import cx_Oracle
            dsn_tns = cx_Oracle.makedsn(db_config['provider'] or 'localhost', db_config['port'] or '1521', service_name=db_config['dbName'])
            conn = cx_Oracle.connect(user=db_config['dbUsername'], password=db_config['dbPassword'], dsn_tns=dsn_tns)
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name FROM user_tab_cols WHERE table_name = '{table_name.upper()}'")
            columns = [row[0] for row in cursor.fetchall()]
        elif db_config['dbType'].lower() in ['mssql', 'sqlserver']:
            print("mssql")
            import pyodbc
            server = db_config['provider'] or 'localhost'
            sql_port = db_config['port'] or '1433'
            conn_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={server},{sql_port};"
                f"DATABASE={db_config['dbName']};"
                f"UID={db_config['dbUsername']};"
                f"PWD={db_config['dbPassword']};"
                "TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            print("msconnection",conn)
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
            """)
            columns = [row[0] for row in cursor.fetchall()]
        else:
            error_message = f"Unsupported database type: {db_config['dbType']}"
    except Exception as e:
        error_message = str(e)
    finally:
        if conn and db_config['dbType'] not in ['MongoDB']:
            conn.close()
    return columns, error_message
def create_view_from_table(db_config, view_name, table_name, selected_columns=None):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbName'],
            user=db_config['dbUsername'],
            password=db_config['dbPassword'],
            host=db_config['provider'] or 'localhost',
            port=db_config['port'] or '5432'
        )
        cur = conn.cursor()

        columns_str = ', '.join(selected_columns) if selected_columns else '*'
        cur.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {columns_str} FROM {table_name}")
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        if conn:
            cur.close()
            conn.close()

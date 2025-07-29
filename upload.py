from config import PASSWORD, USER_NAME, HOST, PORT
from flask import Flask, request, jsonify
import psycopg2
def get_db_connection(dbname="datasource"):
    conn = psycopg2.connect(
        dbname=dbname,
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
        
    )
    return conn
# Function to check if a table is used in chart creation
def is_table_used_in_charts( table_name):
    conn = get_db_connection(dbname="datasource")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM table_chart_save WHERE selected_table = %s
        )
        """,
        (table_name,)
    )
    return cur.fetchone()[0]
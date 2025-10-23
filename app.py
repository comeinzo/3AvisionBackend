# Gayathri

# ==============================
# Standard Library Imports
# ==============================
import ast
import binascii
import json
import logging
import os
import re
import threading
import uuid

import whisper
import spacy
import traceback

import urllib.parse
from datetime import datetime, timedelta
from functools import lru_cache, wraps
from urllib.parse import quote_plus

# ==============================
# Third-Party Imports
# ==============================
import bcrypt
import jwt
import matplotlib
import pandas as pd
import numpy as np  # Add this if not already imported at the top
import psycopg2
import pytz
import sweetviz as sv
from functools import reduce
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request,url_for, session, send_file, current_app
from flask_cors import CORS
from flask_mail import Mail, Message
from flask_session import Session  # Flask-Session for server-side session handling
from flask_socketio import SocketIO, emit
from flask_apscheduler import APScheduler
from psycopg2 import sql
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from statsmodels.tsa.seasonal import seasonal_decompose
from werkzeug.utils import secure_filename
from urllib.parse import parse_qs
from psycopg2.extras import RealDictCursor

import smtplib
from email.mime.text import MIMEText
# ==============================
# Local Imports
# ==============================
import bar_chart as bc
from ai_charts import analyze_data
from audio import allowed_file, transcribe_audio_with_timestamps, save_file_to_db
from config import ALLOWED_EXTENSIONS, DB_NAME, USER_NAME, PASSWORD, HOST, PORT
from csv_upload import upload_csv_to_postgresql
from dashboard_design import get_database_table_names
from dashboard_save.dashboard_save import (
    create_connection,
    create_dashboard_table,
    fetch_project_names,
    get_dashboard_names,
    get_dashboard_view_chart_data,
    get_Edit_dashboard_names,
    insert_combined_chart_details,
)
from excel_upload import upload_excel_to_postgresql
from histogram_utils import generate_histogram_details, handle_column_data_types
from json_upload import upload_json_to_postgresql
from signup.signup import (
    connect_db,
    create_user_table,
    encrypt_password,
    fetch_company_login_data,
    fetch_login_data,
    fetch_usersdata,
    insert_user_data,
)
from TransferData import fetch_data_with_columns, fetch_table_details, insert_dataframe_with_upsert
from upload import is_table_used_in_charts
from user_upload import (
    get_db_connection,
    handle_file_upload_registration,
    handle_manual_registration,
)
from viewChart.viewChart import (
    fetch_ai_saved_chart_data,
    fetch_chart_data,
    filter_chart_data,
    get_db_connection_view,
)
from bar_chart import (
    calculationFetch,
    convert_calculation_to_sql,
    drill_down,
    edit_fetch_data,
    fetch_column_name,
    fetch_data,
    fetch_data_for_duel,
    fetch_data_for_duel_bar,
    fetch_data_tree,
    fetch_hierarchical_data,
    fetchText_data,
    get_column_names,
    Hierarchial_drill_down,
    perform_calculation,
)
from bar_chart import global_df,global_column_names

# ==============================
# Configurations
# ==============================
matplotlib.use('Agg')
company_name_global = None
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# ==============================
# Flask App Initialization
# ==============================
app = Flask(
    __name__,
    static_url_path='/static',
    static_folder='uploaded_logos'
)

# Secret key for sessions
app.secret_key = os.urandom(24)
app.config['SECRET_KEY'] = b'y\xd8\x9e\xa6a\xe0\x8eK\x02L\x14@\x0f\x03\xab\x8e\xae\x1d\tB\xbc\xfbL\xcc'
app.config['SESSION_TYPE'] = 'filesystem'  # Store sessions on the server

# Enable CORS
CORS(app, resources={r"/*": {"origins": "*"}})

# Enable WebSockets
socketio = SocketIO(app, cors_allowed_origins="*")

# ==============================
# Database & Upload Settings
# ==============================
db_name = DB_NAME
username = USER_NAME
password = PASSWORD
host = HOST
port = PORT

UPLOAD_FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'audio'
)
UPLOAD_ROOT = 'uploaded_logos'
BASE_DIR=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'excel')
BASE_CSV_DIR=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'csv')
os.makedirs(UPLOAD_ROOT, exist_ok=True)

company_name = None

# ==============================
# Session Handling
# ==============================
Session(app)
# ==============================
# Email Setup
# ==============================
SECRET_KEY = "454eb22721821f6b5d116b5165acf7fe6023f9439bf5ab18f401630cac4316c1"
HOTMAIL_USER = "gayathrimohan@comienzosoftware.com"
HOTMAIL_PASS = "Gaya@8sep" 
# ==============================
# Scheduler Setup
# ==============================
scheduler = BackgroundScheduler()
scheduler.start()



# JWT Configuration
JWT_SECRET_KEY = 'af65380dbf4a4d0bae8304442204678235e9bf7f5c0e482eab917f2ea9bceaeb' # Change this!
JWT_ALGORITHM = 'HS256'
JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=24)  # Access token expires in 24 hours
JWT_REFRESH_TOKEN_EXPIRES = timedelta(days=30)  # Refresh token expires in 30 days

# Load spaCy's small English model
nlp = spacy.load("en_core_web_sm")

NLP_UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads/nlp_audio')
os.makedirs(NLP_UPLOAD_FOLDER, exist_ok=True)

CHART_TYPES = {"bar", "pie", "line", "scatter", "area"}
COLUMNS_NAMES = bc.global_column_names



# Authentication Decorators
def jwt_required(f):
    """Decorator to require valid JWT token"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None
        auth_header = request.headers.get('Authorization')
        
        if auth_header:
            try:
                token = auth_header.split(' ')[1]  # Bearer <token>
            except IndexError:
                return jsonify({'message': 'Invalid token format'}), 401
        
        if not token:
            return jsonify({'message': 'Token is missing'}), 401
        
        payload = JWTManager.decode_token(token)
        
        if 'error' in payload:
            return jsonify({'message': payload['error']}), 401
        
        if payload.get('type') != 'access':
            return jsonify({'message': 'Invalid token type'}), 401
        
        # Add user info to request context
        request.current_user = payload
        return f(*args, **kwargs)
    
    return decorated_function

def employee_required(f):
    """Decorator to require employee privileges (admin or employee)"""
    @wraps(f)
    @jwt_required
    def decorated_function(*args, **kwargs):
        try:
            user_type = request.current_user.get('user_type')
            if user_type not in ['admin', 'employee']:
                return jsonify({'message': 'Employee privileges required'}), 403
            print(f"[AUTH SUCCESS] User '{request.current_user.get('username')}' "
                  f"({user_type}) passed employee access check.")
            return f(*args, **kwargs)
        except Exception as e:
            return jsonify({'message': 'Authorization check failed'}), 500
    
    return decorated_function

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=USER_NAME, password=PASSWORD, host=HOST
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None
    
def create_table():
    try:

        conn=connect_to_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS table_chart_save (
                id SERIAL PRIMARY KEY,
                User_id integer,
                company_name VARCHAR,
                chart_name VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                database_name VARCHAR,
                selected_table VARCHAR,
                x_axis VARCHAR[],
                y_axis VARCHAR[],
                aggregate VARCHAR,
                chart_type VARCHAR,
                chart_color VARCHAR,
                chart_heading VARCHAR,
                drilldown_chart_color VARCHAR,
                filter_options VARCHAR,
                ai_chart_data JSONB,
                selectedUser VARCHAR,
                xFontSize INTEGER,
                fontStyle VARCHAR,         
                categoryColor VARCHAR,      
                yFontSize INTEGER,          
                valueColor VARCHAR ,
                headingColor VARCHAR ,
                ClickedTool VARCHAR,
                Bgcolour VARCHAR,
                OptimizationData VARCHAR,
                calculationData JSONB,
                selectedFrequency VARCHAR
            )
        """)
        cur.execute("SELECT MAX(id) FROM table_chart_save")
        max_id = cur.fetchone()[0] 
        if max_id is None:
            max_id = 1
        else:
            max_id = max_id + 1
        cur.execute("SELECT setval(pg_get_serial_sequence('table_chart_save', 'id'), %s, false)", (max_id,))


       
        # cur.execute("""
        #     DO $$
        #     BEGIN
        #         IF NOT EXISTS (
        #             SELECT 1
        #             FROM information_schema.columns 
        #             WHERE table_name='table_chart_save' 
        #             AND column_name='selectedFrequency'
        #         ) THEN
        #             ALTER TABLE table_chart_save ADD COLUMN selectedFrequency VARCHAR;
        #         END IF;
        #     END$$;
        # """)
        conn.commit()
        cur.close()
        conn.close()
        print("Table created successfully")
    except Exception as e:
        print("Error in create-------------:", e)

create_table()

@app.route('/', methods=['GET'])
def index():
    return jsonify({"message": "Hello, World! tessstingsssss"})

@app.route('/test_db')
def test_db():
    try:
        # Establish connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query to fetch all table names
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        
        # Fetch all table names
        tables = cursor.fetchall()
        table_list = [table[0] for table in tables]

        # Close the connection
        cursor.close()
        conn.close()

        # Check if table list is empty
        if not table_list:
            return {"message": "No tables found in the database."}, 200

        # Return the table names
        return {"tables": table_list}, 200

    except Exception as e:
        return {"error": f"Database connection failed: {e}"}, 500

# ===================================================================
# UPLOAD SECTION START
# ===================================================================

@app.route('/uploadexcel', methods=['POST'])
@employee_required
def upload_file_excel():
    try:
        create_table()
        
        # Validate required form data
        database_name = request.form.get('company_database')
        primary_key_column = request.form.get('primaryKeyColumnName')
        
        if not database_name:
            return jsonify({'message': 'Company database name is required', 'status': False}), 400
        
        if not primary_key_column:
            return jsonify({'message': 'Primary key column name is required', 'status': False}), 400
        
        # Validate file upload
        if 'file' not in request.files:
            return jsonify({'message': 'No file uploaded', 'status': False}), 400
        
        excel_file = request.files['file']
        
        if excel_file.filename == '':
            return jsonify({'message': 'No file selected', 'status': False}), 400
        
        # Validate file extension
        allowed_extensions = {'.xlsx', '.xls'}
        file_extension = os.path.splitext(excel_file.filename)[1].lower()
        if file_extension not in allowed_extensions:
            return jsonify({'message': 'Only Excel files (.xlsx, .xls) are allowed', 'status': False}), 400
        
        # Parse selected sheets
        selected_sheets = request.form.getlist('selectedSheets')
        if not selected_sheets:
            # Try parsing as JSON string if not sent as list
            selected_sheets_json = request.form.get('selectedSheets')
            if selected_sheets_json:
                try:
                    import json
                    selected_sheets = json.loads(selected_sheets_json)
                except json.JSONDecodeError:
                    return jsonify({'message': 'Invalid selectedSheets format', 'status': False}), 400
        
        print(f"Database name: {database_name}")
        print(f"Primary key column: {primary_key_column}")
        print(f"Selected sheets: {selected_sheets}")
        print(f"User: {request.current_user.get('user_id', 'Unknown')}")
        
        # Save file temporarily
        excel_file_name = secure_filename(excel_file.filename)
        os.makedirs('tmp', exist_ok=True)
        temp_file_path = f'tmp/{excel_file_name}'
        excel_file.save(temp_file_path)
        
        try:
            # Upload to PostgreSQL
            result = upload_excel_to_postgresql(
                database_name, 
                username, 
                password, 
                temp_file_path, 
                primary_key_column, 
                host, 
                port, 
                selected_sheets
            )
            
            if result == "Upload successful":
                return jsonify({
                    'message': 'File uploaded successfully',
                    'status': True,
                    'uploaded_by': request.current_user.get('user_id'),
                    'file_name': excel_file_name
                }), 200
            else:
                return jsonify({'message': result, 'status': False}), 500
                
        finally:
            # Clean up temporary file
            try:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
            except Exception as cleanup_error:
                print(f"Warning: Could not delete temporary file {temp_file_path}: {cleanup_error}")
        
    except Exception as e:
        print(f"Upload error: {str(e)}")
        return jsonify({'message': 'Internal server error occurred', 'status': False}), 500

@app.route('/uploadcsv', methods=['POST'])
@employee_required
def upload_file_csv():
    try:
        create_table()  # Ensure table structure exists
        
        # Validate required form data
        database_name = request.form.get('company_database')
        primary_key_column = request.form.get('primaryKeyColumnName')
        update_permission = request.form.get('updatePermission')
        
        if not database_name:
            return jsonify({'message': 'Company database name is required', 'status': False}), 400
        
        if not primary_key_column:
            return jsonify({'message': 'Primary key column name is required', 'status': False}), 400
        
        if update_permission is None:
            return jsonify({'message': 'Update permission is required', 'status': False}), 400
        
        # Convert update_permission to boolean
        update_permission = update_permission.lower() in ['true', '1', 'yes']
        
        # Validate file upload
        if 'file' not in request.files:
            return jsonify({'message': 'No file uploaded', 'status': False}), 400
        
        csv_file = request.files['file']
        
        if csv_file.filename == '':
            return jsonify({'message': 'No file selected', 'status': False}), 400
        
        # Validate file extension
        allowed_extensions = {'.csv'}
        file_extension = os.path.splitext(csv_file.filename)[1].lower()
        if file_extension not in allowed_extensions:
            return jsonify({'message': 'Only CSV files (.csv) are allowed', 'status': False}), 400
        
        print(f"Database name: {database_name}")
        print(f"Primary key column: {primary_key_column}")
        print(f"Update permission: {update_permission}")
        print(f"User: {request.current_user.get('user_id', 'Unknown')}")
        
        # Save file temporarily
        csv_file_name = secure_filename(csv_file.filename)
        os.makedirs('tmp', exist_ok=True)
        temp_file_path = f'tmp/{csv_file_name}'
        csv_file.save(temp_file_path)
        
        try:
            # Upload to PostgreSQL
            result = upload_csv_to_postgresql(database_name, username, password, temp_file_path, host, port)
            
            if result == "Upload successful":
                return jsonify({
                    'message': 'CSV file uploaded successfully',
                    'status': True,
                    'uploaded_by': request.current_user.get('user_id'),
                    'file_name': csv_file_name,
                    'update_permission': update_permission
                }), 200
            else:
                return jsonify({'message': result, 'status': False}), 500
                
        finally:
            # Clean up temporary file
            try:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
            except Exception as cleanup_error:
                print(f"Warning: Could not delete temporary file {temp_file_path}: {cleanup_error}")
        
    except Exception as e:
        print(f"CSV Upload error: {str(e)}")
        return jsonify({'message': 'Internal server error occurred', 'status': False}), 500

@app.route('/upload-json', methods=['POST'])
@employee_required
def upload_file_json():
    try:
        database_name = request.form.get('company_database')
        primary_key_column = request.form.get('primaryKeyColumnName')
        
        # Check if file is present in the request
        if 'file' not in request.files:
            return jsonify({'message': 'No file part in the request'}), 400

        json_file = request.files['file']
        
        # Check if a file is selected
        if json_file.filename == '':
            return jsonify({'message': 'No file selected for uploading'}), 400

        print("primary_key_column:", primary_key_column)
        print("json_file:", json_file.filename)
        print("database_name:", database_name)
        
        # Save the file to a temporary directory
        json_file_name = secure_filename(json_file.filename)
        os.makedirs('tmp', exist_ok=True)
        temp_file_path = f'tmp/{json_file_name}'
        json_file.save(temp_file_path)

        # Call the upload_json_to_postgresql function
        result = upload_json_to_postgresql(database_name, username, password, temp_file_path, primary_key_column, host, port)
        
        if result == "Upload successful":
            return jsonify({'message': 'File uploaded successfully'}), 200
        else:
            return jsonify({'message': result}), 500

    except Exception as e:
        traceback.print_exc()
        return jsonify({'message': f"Internal Server Error: {str(e)}"}), 500

@app.route('/load-data', methods=['POST'])
def load_data():
    database_name = request.json['databaseName']
    checked_paths = request.json['checkedPaths']
    print("Database name:", database_name)
    print("Checked paths:", checked_paths)
    return jsonify({'message': 'Data loaded successfully'})

@app.route('/ai_ml_filter_chartdata', methods=['POST'])
def ai_ml_filter_chart_data():
    try:
        # Extract data from the POST request
        data = request.get_json()
        if not data or 'category' not in data or 'x_axis' not in data:
            return jsonify({"error": "category and x_axis are required"}), 400

        category = data['category']  # Value to filter by
        x_axis = data['x_axis']      # Region column name

        print("Category:", category)
        print("X-Axis:", x_axis)

        # Filter the DataFrame dynamically based on x_axis and category
        dataframe = bc.global_df  # Assuming bc.global_df is your DataFrame
        filtered_dataframe = dataframe[dataframe[x_axis] == category]

        print("Filtered DataFrame:", filtered_dataframe)
        ai_ml_charts_details = analyze_data(filtered_dataframe)
        # print("AI/ML Charts Details:", ai_ml_charts_details)

        return jsonify({"ai_ml_charts_details": ai_ml_charts_details})
    except Exception as e:
        print("Error:", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/table_names')
@employee_required
def get_table_names():
    db_name = request.args.get('databaseName')
    # print("db name",db_name)
    table_names_response = get_database_table_names(db_name, username, password, host, port)
    # print("table_names",table_names_response )
    if table_names_response is None:
        return jsonify({'message': 'Error fetching table names'}), 500
    table_names = table_names_response.get_json()  
    return jsonify(table_names)

@app.route('/column_names/<table_name>',methods=['GET'] )
def get_columns(table_name):
    db_name= request.args.get('databaseName')
    connectionType= request.args.get('connectionType')
    selectedUser=request.args.get('selectedUser')
    print(f"Request Parameters - Database: {db_name}, Connection Type: {connectionType}, User: {selectedUser}")
    print("connectionType",connectionType)
    column_names = get_column_names(db_name, username, password, table_name,selectedUser, host, port,connectionType)
    # print("column_names====================",column_names)
    return jsonify(column_names)

@app.route('/api/columns', methods=['GET'])
def get_chart_columns():
    chart_name = request.args.get('chart')
    company_name=request.args.get('company_name')
    if not chart_name:
        return jsonify({"error": "Chart name is required"}), 400
    
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Fetch x_axis and y_axis from chartdetails
        cur.execute("SELECT x_axis, y_axis FROM table_chart_save WHERE chart_name = %s AND company_name = %s ", (chart_name,company_name))
        chart = cur.fetchone()
        
        if not chart:
            return jsonify({"message": f"No chart found with the name '{chart_name}'"}), 404
        
        x_axis = chart['x_axis']
        y_axis = chart['y_axis']
        
        cur.close()
        conn.close()

        return jsonify({"x_axis": chart["x_axis"], "y_axis": chart["y_axis"]}), 200
    except Exception as e:
        print("Error while fetching chart details:", e)
        return jsonify({"error": "Failed to fetch chart details"}), 500
def get_chart_db_details(chart_name,company_name):
    try:
        conn =get_db_connection()
        cur = conn.cursor()
        print(f"Fetching DB details for chart: {chart_name}, company: {company_name}")  # Debug print
        query = "SELECT database_name, selected_table ,filter_options,calculationdata FROM table_chart_save WHERE chart_name = %s AND company_name = %s;"
        cur.execute(query, (chart_name, company_name))
        result = cur.fetchone()
        print("Query Result:", result)  # Debug print
        cur.execute(query, (chart_name,company_name))
        result = cur.fetchone()
        print(result)
        cur.close()
        conn.close()

        if result:
            return {"dbname": result[0], "table_name": result[1],"filter_options":result[2],"calculationdata":result[3]}
        else:
            return None
    except Exception as e:
        return {"error": str(e)}

@app.route('/api/column_values', methods=['GET'])
def get_column_values():
    chart_name = request.args.get('chart')
    column_name = request.args.get('column')
    company_name =request.args.get('company_name')

    if not chart_name or not column_name:
        return jsonify({"error": "Chart name and column name are required"}), 400

    # Get database and table details
    db_details = get_chart_db_details(chart_name,company_name)
    print("db_details",db_details)
    if not db_details or "error" in db_details:
        print(f"Database details not found for chart: {chart_name}")
        return jsonify({"error": "Chart not found or DB connection issue"}), 500

    # Database config for the new database
    CHART_DB_CONFIG = {
        "dbname": db_details["dbname"],  # ✅ Correct
        "user": USER_NAME,
        "password": PASSWORD,
        "host": HOST,
        "port":PORT
       
    }

    print("CHART_DB_CONFIG",CHART_DB_CONFIG)
    try:
        conn = get_company_db_connection(db_details["dbname"])
        
        
        cur = conn.cursor()
       
        # query = f'SELECT "{column_name}" FROM "{db_details["table_name"]}" LIMIT 100;'
        # calculationData = db_details.get("calculationdata", [])
        calculationData = db_details.get("calculationdata") or []

        matched_calc = next(
            (calc for calc in calculationData if calc.get("columnName") == column_name and calc.get("calculation")),
            None
        )

        if matched_calc:
            raw_formula = matched_calc["calculation"].strip()
            # You should define this function to convert formulas like "if ([gender] == 'Female') then 'F' else 'M'" to SQL
            formula_sql = convert_calculation_to_sql(raw_formula, dataframe_columns=[])  # supply your column list if needed
            alias_name = f"{column_name}_calculated"
            query = f'SELECT ({formula_sql}) AS "{alias_name}" FROM "{db_details["table_name"]}" LIMIT 100;'
        else:
            # Normal column selection
            query = f'SELECT "{column_name}" FROM "{db_details["table_name"]}" LIMIT 100;'
        print(query)
        cur.execute(query)
        values = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()
        return jsonify({
                    "column_values": values,
                    "table_name": db_details["table_name"]  # ✅ Pass table name
                })
        # return jsonify({"column_values": values})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/get_charts', methods=['GET'])
def get_charts():
    chart_name = request.args.get('chart_name')
    company_name=request.args.get('company_name')

    if not chart_name:
        return jsonify({"error": "Missing chart_name parameter"}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Step 1: Fetch chart IDs from table_dashboard
        cursor.execute("""
            SELECT chart_ids FROM table_dashboard WHERE file_name = %s
        """, (chart_name,))
        chart_ids = cursor.fetchone()

        if not chart_ids or not chart_ids[0]:
            return jsonify({"error": "No chart IDs found for the given chart name"}), 404

        # Convert PostgreSQL array string '{11,4,3,2}' to a Python list [11, 4, 3, 2]
        chart_ids_list = list(map(int, chart_ids[0].strip('{}').split(',')))

        print("Converted chart_ids:", chart_ids_list)  # Debugging

        # Step 2: Fetch chart names from new_dashboard_details using chart IDs
        cursor.execute("""
            SELECT user_id, chart_name FROM table_chart_save WHERE id = ANY(%s) AND company_name=%s
        """, (chart_ids_list,company_name))
        

        charts = cursor.fetchall()
        chart_names = [row[1] for row in charts]
        

        cursor.close()
        conn.close()

        return jsonify({"charts": chart_names})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def column_exists(cur, table_name, column_name):
    """
    Check if a column exists in a given table.
    """
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table_name, column_name)
    )
    return cur.fetchone() is not None

@app.route("/api/update_filters", methods=["POST"])
def update_filters():
    data = request.json
    chart_name = data.get("sourceChart")
    print("sourceChart", chart_name)
    selected_filters = data.get("selectedFilters")
    print("selected_filters", selected_filters)
    filename = data.get("filename")
    print("filename", filename)
    company_name = data.get("company_name")
    print("company_name", company_name)
    user_id=data.get("user_id")
    print("user_id", user_id)
    if not chart_name or not selected_filters or not filename or not company_name:
        return jsonify({"error": "Missing required fields"}), 400

    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cur = conn.cursor()

        column_name = next(iter(selected_filters)) if selected_filters else None
        if not column_name:
            return jsonify({"error": "No column selected"}), 400

        selected_values = selected_filters[column_name]
        print("selected_values", selected_values)

        cur.execute(
            "SELECT id FROM table_chart_save WHERE chart_name = %s AND company_name = %s AND  user_id =%s",
            (chart_name, company_name,user_id),
        )
        chart_id_result = cur.fetchone()
        print("chart_id_result",chart_id_result)
        if not chart_id_result:
            return jsonify({"error": f"Chart '{chart_name}' not found"}), 404

        chart_id = chart_id_result[0]
        print("chart_id", chart_id)

        cur.execute(
            "SELECT chart_ids, filterdata FROM table_dashboard WHERE file_name = %s AND company_name = %s",
            (filename, company_name),
        )
        result = cur.fetchone()
        print("result", result)
        if not result:
            return jsonify({"error": f"No data found for file '{filename}'"}), 404

        chart_ids, existing_filters = result
        print("id",chart_ids)

        chart_ids_list = [int(id.strip()) for id in chart_ids.strip("{}").split(",") if id.strip().isdigit()]
        print("chart_ids_list", chart_ids_list)

        try:
            if existing_filters:
                existing_filters_list = ast.literal_eval(existing_filters)
                filters_list = [json.loads(f) for f in existing_filters_list] if isinstance(existing_filters_list, list) else []
            else:
                filters_list = []
        except json.JSONDecodeError as e:
            print("JSON Decode Error:", str(e))
            return jsonify({"error": "Invalid JSON format in existing filters", "details": str(e)}), 500

        while len(filters_list) < len(chart_ids_list):
            filters_list.append({})

        updated_charts = []
        skipped_charts = []

        if chart_id in chart_ids_list:
            index = chart_ids_list.index(chart_id)
            existing_filter_at_position = filters_list[index]

            if column_name in existing_filter_at_position:
                filters_list[index][column_name] = selected_values
                updated_charts.append(f"Chart {chart_id} is updated.")
                print(f"Updated chart at position {index} with column {column_name}")
            else:
                skipped_charts.append(f"Chart {chart_id} is skipped because column '{column_name}' was not found.")
        else:
            return jsonify({"error": f"Chart ID {chart_id} not found in chart_ids_list"}), 404

        formatted_filters_list = [json.dumps(filter_dict) for filter_dict in filters_list]
        updated_filters_json = str(formatted_filters_list)

        print("Formatted Filters to Save:", updated_filters_json)

        cur.execute(
            "UPDATE table_dashboard SET filterdata = %s WHERE file_name = %s AND company_name = %s",
            (updated_filters_json, filename, company_name),
        )

        # Only print the update message if an update actually occurred
        if updated_charts:
            print(f"Chart '{chart_name}' is being updated with new filters: {selected_filters}")

        # cur.execute(
        #     "UPDATE dashboard_details_wu_id SET dashboard_name = %s WHERE file_name = %s AND company_name = %s",
        #     (chart_name, filename, company_name),
            
        # )
        # print("Rows affected:", cur.rowcount)
        # Only update dashboard_name if the chart was updated
        if updated_charts:
            if not column_exists(cur, "table_dashboard", "dashboard_name"):
                cur.execute("ALTER TABLE table_dashboard ADD COLUMN dashboard_name VARCHAR")
                conn.commit()
                print("Added 'dashboard_name' column to table_dashboard")
            cur.execute(
                "SELECT dashboard_name FROM table_dashboard WHERE file_name = %s AND company_name = %s",
                (filename, company_name),
            )
            existing_dashboard_name = cur.fetchone()

            # Convert existing dashboard_name into a list
            if existing_dashboard_name and existing_dashboard_name[0]:
                try:
                    dashboard_names_list = json.loads(existing_dashboard_name[0])
                    if not isinstance(dashboard_names_list, list):
                        dashboard_names_list = [dashboard_names_list]  # Convert to list if it's a single string
                except json.JSONDecodeError:
                    dashboard_names_list = [existing_dashboard_name[0]]  # Handle non-JSON formatted string
            else:
                dashboard_names_list = []

            # Add only updated charts to dashboard_name
            if chart_name not in dashboard_names_list:
                dashboard_names_list.append(chart_name)

            # Convert back to JSON string for storage
            updated_dashboard_names_json = json.dumps(dashboard_names_list)

            # Update the dashboard_name column
            cur.execute(
                "UPDATE table_dashboard SET dashboard_name = %s WHERE file_name = %s AND company_name = %s",
                (updated_dashboard_names_json, filename, company_name),
            )
            print("Dashboard names updated:", updated_dashboard_names_json)

        conn.commit()
        return jsonify({
            "message": "Filters updated successfully.",
            "updated_charts": updated_charts,
            "skipped_charts": skipped_charts,
            "chart_name": chart_name
        })

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if "cur" in locals() and cur:
            cur.close()
        if "conn" in locals() and conn:
            conn.close()

@app.route('/join-tables', methods=['POST'])
def join_tables():
    data = request.json
    print("data__________",data)
    tables = data.get('tables')  
    selected_columns = data.get('selectedColumns') 
    join_columns = data.get('joinColumns')  
    join_type = data.get('joinType', 'INNER JOIN') 
    database_name = data.get('databaseName')
    view_name = data.get('joinedTableName')  

    query = f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(selected_columns)} FROM {tables[0]}"

    for table in tables[1:]:
        join_column = join_columns.get(table)
        query += f" {join_type} {table} ON {tables[0]}.{join_column} = {table}.{join_column}"

    print("Executing query:", query)

    # Connect to the database
    connection = None
    try:
        # Use your database connection method
        connection = connect_db(database_name)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()  # Commit to save the view in the database
        print("View created successfully")
    except Exception as e:
        print("Error executing query:", e)
        return jsonify({"error": str(e)}), 500
    finally:
        if connection:
            cursor.close()
            connection.close()

    return jsonify({"message": f"View '{view_name}' created successfully"})

DB_CONFIG = {
    'user':USER_NAME,
    'password':PASSWORD,
    'host':HOST,
    'port':PORT
}

active_listeners = {}  # Store active listener threads

def create_dynamic_trigger(db_nameeee, table_name):
    """Dynamically create a trigger and function for real-time updates."""
    try:
        connection = psycopg2.connect(dbname=db_nameeee, **DB_CONFIG)
        cursor = connection.cursor()
        function_name = f"notify_chart_update_{table_name}"
        trigger_name = f"chart_update_trigger_{table_name}"

        cursor.execute(f"""
            CREATE OR REPLACE FUNCTION "{function_name}"()
            RETURNS TRIGGER AS $$
            BEGIN
                PERFORM pg_notify('chart_update', '{table_name}');
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cursor.execute(f"SELECT tgname FROM pg_trigger WHERE tgname = %s;", (trigger_name,))
        trigger_exists = cursor.fetchone()

        if not trigger_exists:
            cursor.execute(f"""
                CREATE TRIGGER "{trigger_name}"
                AFTER INSERT OR UPDATE OR DELETE ON "{table_name}"
                FOR EACH ROW EXECUTE FUNCTION "{function_name}"();
            """)

        connection.commit()
        cursor.close()
        connection.close()
        print(f"Trigger and function created for {db_nameeee}.{table_name}")

    except Exception as e:
        print(f"Error creating trigger: {e}")

@socketio.on("connect")
def handle_connect():
    """Handle client connection and start a unique listener for each session."""
    sid = request.sid
    print("sid",sid)
    # Extract parameters
    selected_table = request.args.get("selectedTable")
    x_axis = request.args.get("xAxis")
    y_axis = request.args.get("yAxis")
    aggregate = request.args.get("aggregate")
    filter_options = request.args.get("filterOptions")
    database_name = request.args.get("databaseName")
    chart_type = request.args.get("chartType")
    # calculationData =request.args.get("calculationData")
    

    calculationData_str = request.args.get("calculationData")
    try:
        calculationData = json.loads(calculationData_str) if calculationData_str else None
    except json.JSONDecodeError:
        print("❌ Failed to parse calculationData:", calculationData_str)
        calculationData = None

    if not all([selected_table, x_axis, aggregate, database_name, chart_type]):
        print("Missing required parameters.")
        return

    if sid in active_listeners:
        print(f"Listener already active for {sid}")
        return

    print(f"Starting new listener for {sid}")

    if chart_type in ["singleValueChart","meterGauge"]:
        listener = threading.Thread(
            target=listen_to_single_value_db,
            args=(selected_table, x_axis, aggregate, database_name),
            daemon=True
        )
    else:
        listener = threading.Thread(
            target=listen_to_db,
            args=(sid, selected_table, x_axis, filter_options, y_axis, aggregate, database_name, chart_type,calculationData),
            daemon=True
        )

    listener.start()
    active_listeners[sid] = listener

def listen_to_db(sid, table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_name, chart_type,calculationData):
    """Continuously listen for updates based on the session ID (sid)."""
   
    import json
    from decimal import Decimal
 
    # Custom JSON encoder to handle Decimal objects
    class DecimalEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Decimal):
                return float(obj)  # Convert Decimal to float
            return super(DecimalEncoder, self).default(obj)
   
    if not isinstance(x_axis_columns, list):
        x_axis_columns = [x_axis_columns]
    if isinstance(y_axis_columns, str):
        y_axis_columns = y_axis_columns.split(",")
 
    thread = threading.current_thread()
 
    try:
        # Establish database connection
        connection = psycopg2.connect(dbname=db_name, **DB_CONFIG)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute("LISTEN chart_update;")
        print(f"Listening for {sid} with params: {db_name}.{table_name} ({x_axis_columns}, {y_axis_columns})")
 
        while getattr(thread, "do_run", True):  # Check if the thread should stop
            connection.poll()
            while connection.notifies:
                notify = connection.notifies.pop(0)
                if notify.payload == table_name:
                    updated_data = fetch_data(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_name, None,calculationData)
                   
                    # Create data package
                    data_package = {
                        "data": updated_data,
                        "xaxis": x_axis_columns,
                        "yaxis": y_axis_columns
                    }
                   
                    # Pre-process any Decimal objects in the data to make it JSON serializable
                    def convert_decimal(obj):
                        if isinstance(obj, Decimal):
                            return float(obj)
                        elif isinstance(obj, dict):
                            return {k: convert_decimal(v) for k, v in obj.items()}
                        elif isinstance(obj, (list, tuple)):
                            return [convert_decimal(x) for x in obj]
                        return obj
                   
                    # Apply the conversion to the entire data package
                    serializable_data = convert_decimal(data_package)
                   
                    # Now emit the pre-processed data
                    socketio.emit("chart_update", serializable_data, room=sid)
 
    except Exception as e:
        print(f"Listener error for {sid}: {repr(e)}")
    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
        if sid in active_listeners:
            del active_listeners[sid]

def listen_to_single_value_db(table_Name, x_axis, aggregate_py, databaseName):
    """Continuously listen for updates with current parameters"""
    if not isinstance(x_axis, list):
        x_axis = [x_axis]
    print("Listening for updates...")
    print("X-Axis Columns:-------", x_axis)
    thread = threading.current_thread()
    try:
        connection = psycopg2.connect(dbname=databaseName, **DB_CONFIG)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute("LISTEN chart_update;")
        print(f"Listening with params: {databaseName}.{table_Name} ({x_axis}, {aggregate_py})")

        while getattr(thread, "do_run", True):  # Check if the thread should stop
            connection.poll()
            while connection.notifies:
                notify = connection.notifies.pop(0)
                if notify.payload == table_Name:
                    # if chartType == "singleValueChart":
                    aggregate_py = {
                    'count': 'count',
                    'sum': 'sum',
                    'average': 'mean',
                    'minimum': 'min',
                    'maximum': 'max'
                }.get(aggregate_py, 'sum') 
                    updated_data = fetchText_data(databaseName, table_Name, x_axis[0], aggregate_py,selectedUser="null")
                    socketio.emit("chart_update", {"data": updated_data})

    except Exception as e:
        print(table_Name, x_axis, aggregate_py, databaseName)
        print(f"Listener error: {repr(e)}")  # Better error output
    finally:
        cursor.close()
        connection.close()

def fetch_data_for_ts_decomposition(table_name, x_axis_columns, filter_options, y_axis_column, aggregation, db_name, selectedUser, calculationData):
    if not selectedUser or selectedUser.lower() == 'null':
        connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
        connection = psycopg2.connect(connection_string)
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

    # Apply calculations if any
    if calculationData and isinstance(calculationData, list):
        for calc_entry in calculationData:
            calc_formula = calc_entry.get('calculation', '').strip()
            new_col_name = calc_entry.get('columnName', '').strip()
            replace_col = calc_entry.get('replaceColumn', new_col_name)

            if not calc_formula or not new_col_name:
                continue

            def replace_column_in_formula(match):
                col_name = match.group(1)
                if col_name in temp_df.columns:
                    if pd.api.types.is_numeric_dtype(temp_df[col_name]):
                        return f"temp_df['{col_name}']"
                    else:
                        return f"temp_df['{col_name}']"
                else:
                    raise ValueError(f"Column '{col_name}' not found in DataFrame for calculation.")

            # Update x and y axis columns if they are being replaced by a calculated column
            # This is important for subsequent steps that might reference these columns
            if y_axis_column:
                y_axis_column = [new_col_name if col == replace_col else col for col in y_axis_column]

            if x_axis_columns:
                x_axis_columns = [new_col_name if col == replace_col else col for col in x_axis_columns]

            # Re-apply the calculation logic here for the temp_df
            # (Copied from your original fetch_data function's calculation block)
            # This part can be refactored into a separate function if it's identical
            # to avoid code duplication, but for clarity, it's repeated here.
            # ... (the entire calculation logic for if, switch, iferror, calculate, etc.) ...
            if calc_formula.strip().lower().startswith("if"):
                match = re.match(r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$", calc_formula.strip(), re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid if-then-else format in calculation.")

                condition_expr, then_val, else_val = match.groups()
                condition_expr_python = re.sub(r'\[(.*?)\]', replace_column_in_formula, condition_expr)

                then_val = then_val.strip('"').strip("'")
                else_val = else_val.strip('"').strip("'")

                try:
                    then_val_parsed = float(then_val)
                except ValueError:
                    then_val_parsed = then_val

                try:
                    else_val_parsed = float(else_val)
                except ValueError:
                    else_val_parsed = else_val

                temp_df[new_col_name] = np.where(eval(condition_expr_python), then_val_parsed, else_val_parsed)

            elif calc_formula.lower().startswith("switch"):
                switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calc_formula, re.IGNORECASE)
                if not switch_match:
                    raise ValueError("Invalid SWITCH syntax")

                col_name_switch, rest = switch_match.groups()
                if col_name_switch not in temp_df.columns:
                    raise ValueError(f"Column '{col_name_switch}' not found in DataFrame")

                cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
                default_match = re.search(r'["\']?default["\']?\s*,\s*["\']?(.*?)["\']?\s*$', rest, re.IGNORECASE)
                default_value = default_match.group(1).strip('"').strip("'") if default_match else None

                mapping = {}
                for case_val, result_val in cases:
                    mapping[case_val.strip('"').strip("'")] = result_val.strip('"').strip("'")

                temp_df[new_col_name] = temp_df[col_name_switch].astype(str).map(mapping).fillna(default_value)

            elif calc_formula.lower().startswith("iferror"):
                match = re.match(r"iferror\s*\((.+?)\s*,\s*(.+?)\)", calc_formula.strip(), re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid IFERROR format")

                expr, fallback = match.groups()
                expr_python = re.sub(r'\[(.*?)\]', replace_column_in_formula, expr)
                fallback = fallback.strip().strip('"').strip("'")

                try:
                    temp_df[new_col_name] = eval(expr_python)
                    if pd.api.types.is_numeric_dtype(temp_df[new_col_name]):
                        temp_df[new_col_name] = temp_df[new_col_name].fillna(pd.to_numeric(fallback, errors='coerce'))
                    else:
                        temp_df[new_col_name] = temp_df[new_col_name].fillna(fallback)
                except Exception as e:
                    print(f"Error evaluating IFERROR expression '{expr_python}': {e}. Filling with fallback.")
                    temp_df[new_col_name] = fallback

            elif calc_formula.lower().startswith("calculate"):
                match = re.match(r"calculate\s*\(\s*(sum|avg|count|max|min)\s*\(\s*\[([^\]]+)\]\s*\)\s*,\s*\[([^\]]+)\]\s*=\s*['\"](.*?)['\"]\s*\)", calc_formula.strip(), re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid CALCULATE format")

                agg_func, value_col, filter_col, filter_val = match.groups()

                df_filtered = temp_df[temp_df[filter_col].astype(str) == filter_val]
                if agg_func == "sum":
                    result_val = df_filtered[value_col].astype(float).sum()
                elif agg_func == "avg":
                    result_val = df_filtered[value_col].astype(float).mean()
                elif agg_func == "count":
                    result_val = df_filtered[value_col].count()
                elif agg_func == "max":
                    result_val = df_filtered[value_col].astype(float).max()
                elif agg_func == "min":
                    result_val = df_filtered[value_col].astype(float).min()
                else:
                    raise ValueError("Unsupported aggregate in CALCULATE")

                temp_df[new_col_name] = result_val
            
            elif calc_formula.lower().startswith("maxx") or calc_formula.lower().startswith("minx"):
                match = re.match(r'(maxx|minx)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid MAXX/MINX syntax.")
                func, col = match.groups()
                if col not in temp_df.columns:
                    raise ValueError(f"Column '{col}' not found.")
                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')
                result_val = temp_df[col].max() if func.lower() == "maxx" else temp_df[col].min()
                temp_df[new_col_name] = result_val

            elif calc_formula.lower().startswith("abs"):
                match = re.match(r'abs\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid ABS syntax.")
                col = match.group(1)
                if col not in temp_df.columns:
                    raise ValueError(f"Column '{col}' not found.")
                temp_df[new_col_name] = pd.to_numeric(temp_df[col], errors='coerce').abs()

            elif calc_formula.lower().startswith("len"):
                match = re.match(r'len\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*\)', calc_formula, re.IGNORECASE)
                col = match.group(1) or match.group(2)
                if col not in temp_df.columns:
                    raise ValueError(f"Column '{col}' not found.")
                temp_df[new_col_name] = temp_df[col].astype(str).str.len()

            elif calc_formula.lower().startswith("lower"):
                match = re.match(r'lower\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                col = match.group(1)
                temp_df[new_col_name] = temp_df[col].astype(str).str.lower()

            elif calc_formula.lower().startswith("upper"):
                match = re.match(r'upper\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                col = match.group(1)
                temp_df[new_col_name] = temp_df[col].astype(str).str.upper()

            elif calc_formula.lower().startswith("concat"):
                match = re.match(r'concat\s*\((.+)\)', calc_formula, re.IGNORECASE)
                if match:
                    parts = [p.strip() for p in re.split(r',(?![^\[]*\])', match.group(1))]
                    concat_parts = []
                    for part in parts:
                        if part.startswith('[') and part.endswith(']'):
                            col = part[1:-1]
                            if col not in temp_df.columns:
                                raise ValueError(f"Column '{col}' not found.")
                            concat_parts.append(temp_df[col].astype(str))
                        else:
                            concat_parts.append(part.strip('"').strip("'"))
                    from functools import reduce
                    temp_df[new_col_name] = reduce(lambda x, y: x + y, [p if isinstance(p, pd.Series) else pd.Series([p]*len(temp_df)) for p in concat_parts])

            elif re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE):
                match = re.match(r'(year|month|day)\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                func, col = match.groups()
                if col not in temp_df.columns:
                    raise ValueError(f"Column '{col}' not found.")
                temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
                if func.lower() == "year":
                    temp_df[new_col_name] = temp_df[col].dt.year
                elif func.lower() == "month":
                    temp_df[new_col_name] = temp_df[col].dt.month
                elif func.lower() == "day":
                    temp_df[new_col_name] = temp_df[col].dt.day

            elif calc_formula.lower().startswith("isnull"):
                match = re.match(r'isnull\s*\(\s*\[([^\]]+)\]\s*,\s*["\']?(.*?)["\']?\s*\)', calc_formula, re.IGNORECASE)
                if match:
                    col, fallback = match.groups()
                    if col not in temp_df.columns:
                        raise ValueError(f"Column '{col}' not found.")
                    temp_df[new_col_name] = temp_df[col].fillna(fallback.strip('"').strip("'"))

            elif re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', calc_formula, re.IGNORECASE):
                match = re.match(r'(?:\[([^\]]+)\]|"([^"]+)")\s+in\s*\((.*?)\)', calc_formula, re.IGNORECASE)
                col = match.group(1) or match.group(2)
                raw_values = match.group(3)

                cleaned_values = []
                for v in raw_values.split(','):
                    v = v.strip().strip('"').strip("'")
                    cleaned_values.append(v)

                if col not in temp_df.columns:
                    raise ValueError(f"Column '{col}' not found in DataFrame.")

                temp_df[new_col_name] = temp_df[col].isin(cleaned_values)

            elif calc_formula.lower().startswith("datediff"):
                match = re.match(r'datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid DATEDIFF format.")
                end_col, start_col = match.groups()
                temp_df[end_col] = pd.to_datetime(temp_df[end_col], errors='coerce')
                temp_df[start_col] = pd.to_datetime(temp_df[start_col], errors='coerce')
                temp_df[new_col_name] = (temp_df[end_col] - temp_df[start_col]).dt.days

            elif calc_formula.lower().startswith("today()"):
                temp_df[new_col_name] = pd.to_datetime(pd.Timestamp.today().normalize())

            elif calc_formula.lower().startswith("now()"):
                temp_df[new_col_name] = pd.Timestamp.now()

            elif calc_formula.lower().startswith("dateadd"):
                match = re.match(
                    r'dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*["\'](day|month|year)["\']\s*\)',
                    calc_formula,
                    re.IGNORECASE
                )
                if not match:
                    raise ValueError("Invalid DATEADD format. Use: dateadd([column], number, 'unit')")

                col, interval, unit = match.groups()
                interval = int(interval)

                if col not in temp_df.columns:
                    raise ValueError(f"DATEADD error: Column '{col}' not found in dataframe")

                temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')

                if unit == "day":
                    temp_df[new_col_name] = temp_df[col] + pd.to_timedelta(interval, unit='d')
                elif unit == "month":
                    temp_df[new_col_name] = temp_df[col] + pd.DateOffset(months=interval)
                elif unit == "year":
                    temp_df[new_col_name] = temp_df[col] + pd.DateOffset(years=interval)
                else:
                    raise ValueError("DATEADD error: Unsupported time unit. Use 'day', 'month', or 'year'")
                temp_df[new_col_name] = temp_df[new_col_name].dt.normalize()

            elif calc_formula.lower().startswith("formatdate"):
                match = re.match(r'formatdate\s*\(\s*(?:\[([^\]]+)\]|"([^"]+)")\s*,\s*["\'](.+?)["\']\s*\)', calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid FORMATDATE format.")
                
                col = match.group(1) or match.group(2)
                fmt = match.group(3)

                temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
                temp_df[new_col_name] = temp_df[col].dt.strftime(fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d").replace("HH", "%H").replace("mm", "%M").replace("ss", "%S"))

            elif calc_formula.lower().startswith("replace"):
                match = re.match(r'replace\s*\(\s*\[([^\]]+)\]\s*,\s*["\'](.*?)["\']\s*,\s*["\'](.*?)["\']\s*\)', calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid REPLACE format.")
                col, old, new = match.groups()
                temp_df[new_col_name] = temp_df[col].astype(str).str.replace(old, new, regex=False)

            elif calc_formula.lower().startswith("trim"):
                match = re.match(r'trim\s*\(\s*\[([^\]]+)\]\s*\)', calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid TRIM format.")
                col = match.group(1)
                temp_df[new_col_name] = temp_df[col].astype(str).str.strip()
            else:
                calc_formula_python = re.sub(r'\[(.*?)\]', replace_column_in_formula, calc_formula)
                try:
                    temp_df[new_col_name] = eval(calc_formula_python)
                except Exception as e:
                    raise ValueError(f"Error evaluating math formula '{calc_formula}': {e}") from e

    # Apply filters
    if isinstance(filter_options, str):
        filter_options = json.loads(filter_options)

    for col, filters in filter_options.items():
        if col in temp_df.columns:
            try:
                if pd.api.types.is_numeric_dtype(temp_df[col]):
                    filters_converted = [pd.to_numeric(f, errors='coerce') for f in filters]
                    filters_converted = [f for f in filters_converted if pd.notna(f)]
                    temp_df = temp_df[temp_df[col].isin(filters_converted)]
                else:
                    temp_df[col] = temp_df[col].astype(str)
                    filters = list(map(str, filters))
                    temp_df = temp_df[temp_df[col].isin(filters)]
            except Exception as e:
                print(f"Warning: Could not apply filter on column '{col}' during TS fetch due to type mismatch or error: {e}. Falling back to string comparison.")
                temp_df[col] = temp_df[col].astype(str)
                filters = list(map(str, filters))
                temp_df = temp_df[temp_df[col].isin(filters)]

    return temp_df[[x_axis_columns[0], y_axis_column[0]]] if x_axis_columns and y_axis_column else temp_df

@app.route('/plot_chart', methods=['POST', 'GET'])
def get_bar_chart_route(): 
    
    
    # Safely access the global DataFrame
    df = bc.global_df
    data = request.json

    # print("data", data)
    
    try:
        x_axis_columns = [col.strip() for col in data['xAxis'].split(',')]
    except AttributeError:
        x_axis_columns = []
        # print("xAxis is not a string, check the request data")
        return jsonify({"error": "xAxis must be a string"})  # Return an error response

    table_name = data['selectedTable']
    y_axis_columns = data['yAxis']  # Assuming yAxis can be multiple columns as well
    aggregation = data['aggregate']
    filter_options = data['filterOptions']
    checked_option = data['filterOptions']
    db_nameeee = data['databaseName']
    selectedUser = data['selectedUser']
    chart_data = data['chartType']
    print("chart_data",data)
    
    # NEW: Get data limiting options and consent tracking
    data_limit_type = data.get('dataLimitType', None)  # 'top10', 'bottom10', 'both5'
    data_limit_column = data.get('dataLimitColumn', None)  # Column to sort by for limiting
    user_consent_given = data.get('userConsentGiven', False)  # Track if user already gave consent
    current_x_axis_key = ','.join(sorted(x_axis_columns))  # Create a key for current xAxis combination
    calculationData = data.get('calculationData')
    selectedFrequency=data.get('selectedFrequency')
    if not selectedUser or selectedUser.lower() == 'null':
        print("Using default database connection...")
        connection_path = f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}"
    else:
        print(f"Using connection for user: {selectedUser}")
        connection_string = fetch_external_db_connection(db_nameeee, selectedUser)
        if not connection_string:
            raise Exception("Unable to fetch external database connection details.")

        db_details = {
            "host": connection_string[3],
            "database": connection_string[7],
            "user": connection_string[4],
            "password": connection_string[5],
            "port": int(connection_string[6])
        }

        connection_path = f"dbname={db_details['database']} user={db_details['user']} password={db_details['password']} host={db_details['host']} port={db_details['port']}"
    
    database_con = psycopg2.connect(connection_path)
    # print("database_con", connection_path)
    
    # Fetch chart data
    new_df = fetch_chart_data(database_con, table_name)

    # Check if fetched data is valid
    if new_df is None:
        print("Error: fetch_chart_data returned None")
        return jsonify({"error": "Failed to fetch data"}), 500
    
    # Update global DataFrame
    if df is not None and df.equals(new_df):
        print("Both DataFrames are equal")
    else:
        print("DataFrames are not equal")
        bc.global_df = new_df
    
    # Safely create JSON from DataFrame
    try:
        # Make sure we're using the latest DataFrame
        df_to_convert = bc.global_df
        
        # Check again to be safe
        if df_to_convert is None:
            return jsonify({"error": "No data available"}), 500
            
        # Handle NaN and infinity values properly
        df_to_convert = df_to_convert.replace([np.nan, np.inf, -np.inf], None)
        
        # Convert to JSON
        df_json = df_to_convert.to_json(orient='split')
    except Exception as e:
        print(f"Error converting DataFrame to JSON: {e}")
        return jsonify({"error": f"Data conversion error: {str(e)}"}), 500

    # Continue with the rest of the function
    if not db_nameeee or not table_name:
        return jsonify({"error": "Database name and table name required"}), 400
    
    create_dynamic_trigger(db_nameeee, table_name)
    
    # Apply filters
    for column, filters in filter_options.items():
        if column in new_df.columns:
            new_df = new_df[new_df[column].isin(filters)]
    
    # NEW: Enhanced function to apply data limiting with caching logic
    def apply_data_limiting(df, limit_type, sort_column, x_cols, y_cols, agg):
        """Apply data limiting based on user selection"""
        if limit_type is None or sort_column is None:
            return df
        
        try:
            # If we need to aggregate first to get meaningful sorting
            if agg and y_cols and len(y_cols) > 0:
                # Group by x-axis columns and aggregate y-axis
                if agg == "sum":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].sum()
                elif agg == "count":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].count()
                elif agg == "average" or agg == "mean":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].mean()
                elif agg == "maximum" or agg == "max":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].max()
                elif agg == "minimum" or agg == "min":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].min()
                else:
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].sum()
                
                sort_by_column = y_cols[0]  # Sort by the aggregated y-axis column
                df_to_limit = grouped_df
            else:
                sort_by_column = sort_column if sort_column in df.columns else (y_cols[0] if y_cols else x_cols[0])
                df_to_limit = df
            
            # Apply the limiting based on type
            if limit_type == "top10":
                limited_df = df_to_limit.nlargest(10, sort_by_column)
            elif limit_type == "bottom10":
                limited_df = df_to_limit.nsmallest(10, sort_by_column)
            elif limit_type == "both5":
                top5 = df_to_limit.nlargest(5, sort_by_column)
                bottom5 = df_to_limit.nsmallest(5, sort_by_column)
                limited_df = pd.concat([top5, bottom5]).drop_duplicates()
            else:
                limited_df = df_to_limit
            
            print(f"Applied {limit_type} limiting: {len(df_to_limit)} -> {len(limited_df)} rows")
            return limited_df
            
        except Exception as e:
            print(f"Error applying data limiting: {e}")
            return df

    # NEW: Enhanced data points check with consent logic
    def check_data_points_limit(data_length, has_consent=False, limit_type=None):
        """Check if data points exceed limit and return appropriate response"""
        if data_length > 30:
            if has_consent and limit_type:
                # User already gave consent and we have limiting - this shouldn't happen
                # But if it does, just log it and continue
                print(f"⚠️ Data still exceeds limit ({data_length}) even with consent and limiting ({limit_type})")
                return None
            elif has_consent and not limit_type:
                # User gave consent but no limiting applied - ask for limiting options
                return {
                    "error": "Data points exceed maximum limit", 
                    "message": "Please select a data limiting option to reduce the data size.",
                    "dataPointCount": data_length,
                    "maxDataPoints": 30,
                    "canApplyLimiting": True,
                    "needsLimitingSelection": True
                }
            else:
                # No consent given - show normal error with consent request
                return {
                    "error": "Data points exceed maximum limit", 
                    "message": "Your categories and values contain more than the maximum 30 data points. Please apply filters or data limiting options to reduce the data size.",
                    "dataPointCount": data_length,
                    "maxDataPoints": 30,
                    "canApplyLimiting": True
                }
        return None

    # Word Cloud chart
    if len(y_axis_columns) == 0 and chart_data == "wordCloud":
        all_text = ""
        for index, row in new_df.iterrows():
            for col in filter_options.keys():
                if col in new_df.columns:
                    all_text += str(row[col]) + " "

        query = f"""
            SELECT word, COUNT(*) AS word_count
            FROM (
                SELECT regexp_split_to_table('{all_text}', '\\s+') AS word
            ) AS words
            GROUP BY word
            ORDER BY word_count DESC;
        """
        
        try:
            if not selectedUser or selectedUser.lower() == 'null':
                connection_string = f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}"
                connection = psycopg2.connect(connection_string)
            else:  # External connection
                savename = data['selectedUser']
                connection_details = fetch_external_db_connection(db_nameeee, savename)
                if connection_details:
                    db_details = {
                        "host": connection_details[3],
                        "database": connection_details[7],
                        "user": connection_details[4],
                        "password": connection_details[5],
                        "port": int(connection_details[6])
                    }
                if not connection_details:
                    raise Exception("Unable to fetch external database connection details.")
                
                connection = psycopg2.connect(
                    dbname=db_details['database'],
                    user=db_details['user'],
                    password=db_details['password'],
                    host=db_details['host'],
                    port=db_details['port'],
                )
            cursor = connection.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            
            categories = [row[0] for row in data]  # Words
            values = [row[1] for row in data]     # Counts
            data = {
                "categories": [row[0] for row in data],  # Words
                "values": [row[1] for row in data],     #
                "dataframe": df_json
            }
        
            return jsonify(data)
        except Exception as e:
            print("Error executing WordCloud query:", e)
            return jsonify({"error": str(e)})
    
    # Single Value Chart
    if chart_data in ["singleValueChart","meterGauge"]:
        create_dynamic_trigger(db_nameeee, table_name)
        aggregate_py = {
            'count': 'count',
            'sum': 'sum',
            'average': 'mean',
            'minimum': 'min',
            'maximum': 'max'
        }.get(aggregation, 'sum') 

        try:
            fetched_data = fetchText_data(db_nameeee, table_name, x_axis_columns[0], aggregate_py, selectedUser)
            return jsonify({
                "data": fetched_data,
                "message": "Data received successfully!"
            })
        except Exception as e:
            print(f"Error in fetchText_data: {e}")
            return jsonify({"error": f"Error fetching single value data: {e}"}), 500
    
    # # Dual Bar Chart
    if len(x_axis_columns) == 2 and chart_data in ["duealbarChart", "stackedbar"] :
        data = fetch_data_for_duel_bar(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee, selectedUser,calculationData)
        # print("data",data)
        # Apply data limiting if specified
        if data_limit_type:
            # Convert data to DataFrame for limiting
            # temp_df = pd.DataFrame(data, columns=['category', 'series1', 'series2'])
            # Convert only series2 (index 2) to float
            converted_data = [
                [row[0], row[1], float(row[2])] for row in data
            ]
            temp_df = pd.DataFrame(converted_data, columns=['category', 'series1', 'series2'])
            
            temp_df['series2'] = pd.to_numeric(temp_df['series2'], errors='coerce')

            # print("temp_df",temp_df)


            # limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'series1', ['category'], ['series1'], aggregation)
            limited_df = apply_data_limiting(
                temp_df,
                data_limit_type,
                data_limit_column or 'series2',      # Limiting on numeric column
                ['category', 'series1'],             # Grouping columns (X-axis)
                ['series2'],                         # Aggregated column
                aggregation
            )
            data = limited_df.values.tolist()
        
        # Check data points limit with consent logic
        limit_check = check_data_points_limit(len(data), user_consent_given, data_limit_type)
        if limit_check:
            return jsonify(limit_check), 400
        # print("Final limited data (for dual bar):", data)
        for i, row in enumerate(data):
            print(f"Row {i}: {row}, length: {len(row)}")

        # Return categories and series for dual X-axis chart
        return jsonify({
            "categories": [row[0] for row in data],  # First X-axis category
            "series1": [row[1] for row in data],  # Series 1 data
            "series2": [row[2] for row in data],  # Series 2 data
            "dataframe": df_json
        })


    # Handle time conversion if needed
    try:
        if y_axis_columns and len(y_axis_columns) > 0:
            # Check if safe to access df and y_axis_columns exists in df
            if df is not None and y_axis_columns[0] in df.columns:
                try:
                    df[y_axis_columns[0]] = pd.to_datetime(df[y_axis_columns[0]], format='%H:%M:%S', errors='raise')
                    # If successful, convert time format to minutes
                    df[y_axis_columns[0]] = df[y_axis_columns[0]].apply(lambda x: x.hour * 60 + x.minute)
                    print(f"{y_axis_columns[0]} converted to minutes.")
                except ValueError:
                    # If conversion fails, it is not in time format
                    print(f"{y_axis_columns[0]} is not in time format. No conversion applied.")
    except Exception as e:
        print(f"Error during time conversion: {e}")
        # Continue execution, don't return error
    
    # Tree Hierarchy chart
    # if chart_data == "treeHierarchy" and len(x_axis_columns) > 0:
    if chart_data in ["treeHierarchy", "tablechart"] and len(x_axis_columns) > 0:
        print("Single treeHierarchy chart")

        try:
            
            # Check if we need aggregation
            if aggregation and y_axis_columns:
                if aggregation == "sum":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].sum()
                elif aggregation == "count":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].count()
                elif aggregation == "mean":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].mean()
                else:
                    return jsonify({"error": f"Unsupported aggregation type: {aggregation}"})
            
            # Apply data limiting if specified
            if data_limit_type:
                new_df = apply_data_limiting(new_df, data_limit_type, data_limit_column or y_axis_columns[0], x_axis_columns, y_axis_columns, aggregation)
            
            print("Limit Check Result:", data_limit_type)
            
            # Check data points limit with consent logic
            # limit_check = check_data_points_limit(len(new_df), user_consent_given, data_limit_type)
            limit_check = check_data_points_limit(len(new_df), user_consent_given, data_limit_type)
            print("Limit Check Result:", limit_check)

            if limit_check:
                return jsonify(limit_check), 400
            
            # Prepare data for tree hierarchy
            categories = []
            values = []

            # for index, row in new_df.iterrows():
            #     category = {col: row[col] for col in x_axis_columns}  # Hierarchy levels
            #     categories.append(category)
            #     values.append(row[y_axis_columns[0]] if y_axis_columns else 1)  # Use aggregated value
            for index, row in new_df.iterrows():
                category = [row[col] for col in x_axis_columns]  # Ordered list of hierarchy values
                categories.append(category)
                values.append(row[y_axis_columns[0]] if y_axis_columns else 1)

            print("x_axis_columns",x_axis_columns)
            print("categories",categories)
            # return jsonify({
            #     "categories": categories,
            #     "values": values,
            #     "chartType": "treeHierarchy",
            # })
            return jsonify({
                "categories": categories,
                "values": values,
                "x_axis_columns": x_axis_columns,
                "chartType": "treeHierarchy",
            })


        except Exception as e:
            print("Error preparing Tree Hierarchy data:", e)
            return jsonify({"error": str(e)})

    # Single Y-axis chart
    if len(y_axis_columns) == 1 and chart_data not in ["treeHierarchy", "tablechart","timeSeriesDecomposition"]:
        print("Single Y-axis chart")
        data = fetch_data(table_name, x_axis_columns, filter_options, y_axis_columns, aggregation, db_nameeee, selectedUser,calculationData)
        print(data)
        if aggregation == "count":
            array1 = [item[0] for item in data]
            array2 = [item[1] for item in data]
            
            # Apply data limiting if specified
            if data_limit_type:
                temp_df = pd.DataFrame({'categories': array1, 'values': array2})
                limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
                array1 = limited_df['categories'].tolist()
                array2 = limited_df['values'].tolist()
            
            # Check data points limit with consent logic
            limit_check = check_data_points_limit(len(array1), user_consent_given, data_limit_type)
            if limit_check:
                return jsonify(limit_check), 400
            
            # Return the JSON response for count aggregation
            return jsonify({"categories": array1, "values": array2, "aggregation": aggregation, "dataframe": df_json})
            
        elif aggregation == "average":
            array1 = [item[0] for item in data]
            array2 = [item[1] for item in data]
            
            # Apply data limiting if specified
            if data_limit_type:
                temp_df = pd.DataFrame({'categories': array1, 'values': array2})
                limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
                array1 = limited_df['categories'].tolist()
                array2 = limited_df['values'].tolist()
            
            # Check data points limit with consent logic
            limit_check = check_data_points_limit(len(array1), user_consent_given, data_limit_type)
            if limit_check:
                return jsonify(limit_check), 400
            
            return jsonify({"categories": array1, "values": array2, "aggregation": aggregation, "dataframe": df_json})
            
        elif aggregation == "variance":
            array1 = [item[0] for item in data]
            array2 = [item[1] for item in data]
            
            # Apply data limiting if specified
            if data_limit_type:
                temp_df = pd.DataFrame({'categories': array1, 'values': array2})
                limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
                array1 = limited_df['categories'].tolist()
                array2 = limited_df['values'].tolist()
            
            # Check data points limit with consent logic
            limit_check = check_data_points_limit(len(array1), user_consent_given, data_limit_type)
            if limit_check:
                return jsonify(limit_check), 400
            
            return jsonify({"categories": array1, "values": array2, "aggregation": aggregation, "dataframe": df_json})
        
        # For other aggregation types
        categories = {}
        for row in data:
            category = tuple(row[:-1])
            y_axis_value = row[-1]
            if category not in categories:
                categories[category] = initial_value(aggregation)
            update_category(categories, category, y_axis_value, aggregation)

        labels = [', '.join(category) for category in categories.keys()]
        values = list(categories.values())
        
        # Apply data limiting if specified
        if data_limit_type:
            temp_df = pd.DataFrame({'categories': labels, 'values': values})
            limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
            labels = limited_df['categories'].tolist()
            values = limited_df['values'].tolist()
        
        # Check data points limit with consent logic
        limit_check = check_data_points_limit(len(labels), user_consent_given, data_limit_type)
        if limit_check:
            return jsonify(limit_check), 400
        
        return jsonify({"categories": labels, "values": values, "aggregation": aggregation, "dataframe": df_json})

    # # # Dual Y-axis chart
    # elif len(y_axis_columns) == 2 and chart_data != "duealbarChart":
    #     data = fetch_data_for_duel(table_name, x_axis_columns, filter_options, y_axis_columns, aggregation, db_nameeee, selectedUser)
        
    #     # Apply data limiting if specified
    #     if data_limit_type:
    #         temp_df = pd.DataFrame(data, columns=['category', 'series1', 'series2'])
    #         limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'series1', ['category'], ['series1'], aggregation)
    #         data = limited_df.values.tolist()
        
    #     # Check data points limit with consent logic
    #     limit_check = check_data_points_limit(len(data), user_consent_given, data_limit_type)
    #     if limit_check:
    #         return jsonify(limit_check), 400
        
    #     return jsonify({
    #         "categories": [row[0] for row in data],
    #         "series1": [row[1] for row in data],
    #         "series2": [row[2] for row in data],
    #         "dataframe": df_json
    #     })
    #  # Dual Y-axis chart - FIXED
    
    # elif chart_data == "timeSeriesDecomposition":
    #         print("Time Series Decomposition Chart")
    #         # For time series decomposition, we need one time column (x_axis_columns[0]) and one value column (y_axis_columns[0])
    #         if not x_axis_columns or not y_axis_columns:
    #             return jsonify({"error": "Time series decomposition requires a time column and a value column."}), 400

    #         time_column = x_axis_columns[0]
    #         value_column = y_axis_columns[0]
    #         print("time_column", time_column)
    #         print("value_column", value_column)
        
    #         temp_df_for_ts = fetch_data_for_ts_decomposition(table_name, [time_column], filter_options, [value_column], None, db_nameeee, selectedUser, calculationData)
    #         print("temp_df_for_ts", temp_df_for_ts)
    #         if temp_df_for_ts.empty:
    #             return jsonify({"error": "No data available for time series decomposition after filtering."}), 400
    #         temp_df_for_ts.dropna(subset=[time_column], inplace=True)
    #         # Ensure time column is datetime and set as index
    #         temp_df_for_ts[time_column] = pd.to_datetime(temp_df_for_ts[time_column], errors='coerce')
    #         temp_df_for_ts.set_index(time_column, inplace=True)

    #         # Ensure value column is numeric
    #         temp_df_for_ts[value_column] = pd.to_numeric(temp_df_for_ts[value_column], errors='coerce')
    #         temp_df_for_ts.dropna(subset=[value_column], inplace=True) # Drop rows with missing values for decomposition
            
    #         if temp_df_for_ts.empty:
    #             return jsonify({"error": "No valid numeric data for time series decomposition after conversion."}), 400

    #         time_series_frequency = data.get('timeSeriesFrequency', 'MS') # Default to Month Start if not provided
    #         print("time_series_frequency", time_series_frequency)
    #         try:
    #             # If there are multiple entries for the same timestamp after filtering, aggregate them
    #             # For example, sum sales for a given day
    #             ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).sum().ffill().bfill() # Sum and fill NaNs
    #             print("ts_data", ts_data)
    #             # period = 12 # Default to monthly
    #             # if 'D' in time_series_frequency: # If daily, maybe a weekly seasonality
    #             #     period = 7
    #             # elif 'W' in time_series_frequency: # If weekly, maybe a yearly seasonality (52 weeks)
    #             #     period = 52
    #             # elif 'Q' in time_series_frequency: # Quarterly
    #             #     period = 4

    #             if 'D' in time_series_frequency:
    #                 period = 7
    #             elif 'W' in time_series_frequency:
    #                 period = 52
    #             elif 'Q' in time_series_frequency:
    #                 period = 4
    #             else:  # Default for MS (monthly)
    #                 period = min(4, len(ts_data) // 2)  # Safe fallback for small datasets

    #             if len(ts_data) < 2 * period:
    #                 return jsonify({"error": f"Not enough data points for decomposition with period {period}. Need at least {2 * period} data points, but got {len(ts_data)}."}), 400

    #             # IMPORTANT: ts_data must have a DatetimeIndex
    #             # seasonal_decompose requires non-null data and enough observations for the period.
    #             decomposition = seasonal_decompose(ts_data, model='additive', period=period) # 'additive' or 'multiplicative'

    #             # Extract components
    #             # .dropna().tolist() is used to convert Series to list, dropping any NaN values that might occur
    #             # at the start/end of trend/seasonal components due to decomposition process.
    #             trend = decomposition.trend.dropna().tolist()
    #             seasonal = decomposition.seasonal.dropna().tolist()
    #             residual = decomposition.resid.dropna().tolist()
    #             observed = decomposition.observed.dropna().tolist()
                
    #             # Align indices for plotting if needed (e.g., convert datetime index to string)
    #             dates = decomposition.observed.dropna().index.strftime('%Y-%m-%d').tolist()

    #             response = {
    #                 "dates": dates,
    #                 "observed": observed,
    #                 "trend": trend,
    #                 "seasonal": seasonal,
    #                 "residual": residual,
    #                 "model": "additive" # Or 'multiplicative' - consider making this configurable
    #             }
    #             return jsonify(response)

    #         except ValueError as ve:
    #             return jsonify({"error": f"Error during time series decomposition: {ve}. This often happens with insufficient data for the chosen period or frequency. Ensure your time column has enough granular data."}), 400
    #         except Exception as e:
    #             return jsonify({"error": f"An unexpected error occurred during time series decomposition: {e}"}), 500

    elif chart_data == "timeSeriesDecomposition":
        print("Time Series Decomposition Chart")
        if not x_axis_columns or not y_axis_columns:
            return jsonify({"error": "Time series decomposition requires a time column and a value column."}), 400
        
        time_column = x_axis_columns[0]
        value_column = y_axis_columns[0]
        print("time_column", time_column)
        print("value_column", value_column)
        print("selectedFrequency",selectedFrequency )


        temp_df_for_ts = fetch_data_for_ts_decomposition(
            table_name, [time_column], filter_options, [value_column], None, db_nameeee, selectedUser, calculationData
        )
        print("temp_df_for_ts", temp_df_for_ts)

        if temp_df_for_ts.empty:
            return jsonify({"error": "No data available for time series decomposition after filtering."}), 400

        temp_df_for_ts.dropna(subset=[time_column], inplace=True)
        temp_df_for_ts[time_column] = pd.to_datetime(temp_df_for_ts[time_column], errors='coerce')
        temp_df_for_ts.set_index(time_column, inplace=True)

        temp_df_for_ts[value_column] = pd.to_numeric(temp_df_for_ts[value_column], errors='coerce')
        temp_df_for_ts.dropna(subset=[value_column], inplace=True)

        if temp_df_for_ts.empty:
            return jsonify({"error": "No valid numeric data for time series decomposition after conversion."}), 400

        # time_series_frequency = data.get('timeSeriesFrequency', 'MS')
        # Set frequency dynamically based on selectedFrequency
        if selectedFrequency == "daily":
            time_series_frequency = 'D'
        elif selectedFrequency == "monthly":
            time_series_frequency = 'MS'  # Month Start
        elif selectedFrequency == "yearly":
            time_series_frequency = 'YS'  # Year Start
        else:
            time_series_frequency = 'MS'  # Default fallback

        print("time_series_frequency", time_series_frequency)

        try:
            # # Decide the resampling aggregation method dynamically
            # if aggregation == "sum":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).sum()
            # elif aggregation == "average":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).mean()
            # elif aggregation == "count":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).count()
            # elif aggregation == "min":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).min()
            # elif aggregation == "max":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).max()
            # elif aggregation == "median":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).median()
            # else:
            #     return jsonify({"error": f"Unsupported aggregation type for time series decomposition: {aggregation}"}), 400
            if aggregation == "sum":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).sum()
            elif aggregation == "average":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).mean()
            elif aggregation == "count":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).count()
            elif aggregation ==  "minimum" or aggregation == "min":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).min()
            elif aggregation == "max" or aggregation == "maximum":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).max()
            elif aggregation == "median":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).median()
            elif aggregation == "variance":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).agg('var')
            else:
                return jsonify({"error": f"Unsupported aggregation type for time series decomposition: {aggregation}"}), 400


            # Fill missing values
            ts_data = ts_data.ffill().bfill()
            print("ts_data", ts_data)

            # Determine decomposition period
            if 'D' in time_series_frequency:
                period = 7
            elif 'W' in time_series_frequency:
                period = 52
            elif 'Q' in time_series_frequency:
                period = 4
            else:
                period = min(4, len(ts_data) // 2)

            if len(ts_data) < 2 * period:
                return jsonify({"error": f"Not enough data points for decomposition with period {period}. Need at least {2 * period} data points, but got {len(ts_data)}."}), 400

            decomposition = seasonal_decompose(ts_data, model='additive', period=period)

            trend = decomposition.trend.dropna().tolist()
            seasonal = decomposition.seasonal.dropna().tolist()
            residual = decomposition.resid.dropna().tolist()
            observed = decomposition.observed.dropna().tolist()
            dates = decomposition.observed.dropna().index.strftime('%Y-%m-%d').tolist()

            return jsonify({
                "dates": dates,
                "observed": observed,
                "trend": trend,
                "seasonal": seasonal,
                "residual": residual,
                "aggregation": aggregation,
                "model": "additive"
            })

        except ValueError as ve:
            return jsonify({
                "error": f"Error during time series decomposition: {ve}. This often happens with insufficient data for the chosen period or frequency. Ensure your time column has enough granular data."
            }), 400
        except Exception as e:
            return jsonify({"error": f"An unexpected error occurred during time series decomposition: {e}"}), 500

    elif len(y_axis_columns) == 2 and chart_data != "duealbarChart":
        try:

            print("calculationData",calculationData)
            data = fetch_data_for_duel(table_name, x_axis_columns, filter_options, y_axis_columns, aggregation, db_nameeee, selectedUser,calculationData= data.get('calculationData'))
            
            # Debug: Print the structure of fetched data
            print(f"🔍 Dual Y-axis Chart - Original data length: {len(data)}")
            if len(data) > 0:
                print(f"🔍 Sample data row: {data[0]}")
            
            # Apply data limiting if specified
            if data_limit_type and len(data) > 0:
                try:
                    # Convert data to DataFrame for limiting - handle different data structures
                    if isinstance(data[0], (list, tuple)) and len(data[0]) >= 3:
                        # Standard format: [category, series1, series2]
                        # temp_df = pd.DataFrame(data, columns=['category', 'series1', 'series2'])
                        # Convert Decimal to float explicitly
                        converted_data = [
                            [row[0], float(row[1]), float(row[2])] for row in data
                        ]
                        temp_df = pd.DataFrame(converted_data, columns=['category', 'series1', 'series2'])

                        sort_column = data_limit_column or 'series1'
                    else:
                        # Fallback: convert to simple format
                        temp_df = pd.DataFrame(data)
                        sort_column = data_limit_column or temp_df.columns[1] if len(temp_df.columns) > 1 else temp_df.columns[0]
                    
                    # Apply limiting
                    if data_limit_type == "top10":
                        limited_df = temp_df.nlargest(10, sort_column)
                    elif data_limit_type == "bottom10":
                        limited_df = temp_df.nsmallest(10, sort_column)
                    elif data_limit_type == "both5":
                        top5 = temp_df.nlargest(5, sort_column)
                        bottom5 = temp_df.nsmallest(5, sort_column)
                        limited_df = pd.concat([top5, bottom5]).drop_duplicates()
                    else:
                        limited_df = temp_df
                    
                    # Convert back to list format
                    data = limited_df.values.tolist()
                    print(f"✅ Applied {data_limit_type} limiting: {len(temp_df)} -> {len(limited_df)} rows")
                    
                except Exception as e:
                    print(f"⚠️ Error applying data limiting to dual y-axis chart: {e}")
                    # Continue without limiting if there's an error
            
            # Check data points limit with consent logic
            limit_check = check_data_points_limit(len(data), user_consent_given, data_limit_type)
            if limit_check:
                return jsonify(limit_check), 400
            
            # Return categories and series for dual Y-axis chart
            if len(data) > 0 and isinstance(data[0], (list, tuple)) and len(data[0]) >= 3:
                return jsonify({
                    "categories": [row[0] for row in data],
                    "series1": [row[1] for row in data],
                    "series2": [row[2] for row in data],
                    "dataframe": df_json
                })
            else:
                # Fallback for unexpected data structure
                return jsonify({
                    "categories": [],
                    "series1": [],
                    "series2": [],
                    "dataframe": df_json,
                    "warning": "Unexpected data structure in dual y-axis chart"
                })
                
        except Exception as e:
            print(f"❌ Error in dual y-axis chart processing: {e}")
            return jsonify({"error": f"Error processing dual y-axis chart: {str(e)}"}), 500
    # Default response
    return jsonify({"message": "Chart data processed successfully."})

def initial_value(aggregation):
    if aggregation in ['sum', 'average']:
        return 0
    elif aggregation == 'minimum':
        return float('inf')
    elif aggregation == 'maximum':
        return float('-inf')
    elif aggregation == 'count':
        return 0

def update_category(categories, category_key, y_axis_value, aggregation):
    if aggregation == 'sum':
        categories[category_key] += float(y_axis_value)
    elif aggregation == 'minimum':
        categories[category_key] = min(categories[category_key], float(y_axis_value))
    elif aggregation == 'maximum':
        categories[category_key] = max(categories[category_key], float(y_axis_value))
    elif aggregation == 'average':
        if isinstance(categories[category_key], list):
            categories[category_key][0] += float(y_axis_value)
            categories[category_key][1] += 1
        else:
            categories[category_key] = [float(y_axis_value), 1]
    elif aggregation == 'count':
        categories[category_key] += 1
    elif aggregation == 'variance':
        if isinstance(categories[category_key], list):
            categories[category_key][0] += float(y_axis_value)  # Sum of values
            categories[category_key][1] += float(y_axis_value) ** 2  # Sum of squares
            categories[category_key][2] += 1  # Count of values
        else:
            categories[category_key] = [float(y_axis_value), float(y_axis_value) ** 2, 1]  # Initialize list for sum, sum of squares, and count

@app.route('/edit_plot_chart', methods=['POST', 'GET'])
def get_edit_chart_route():
    df = bc.global_df
    data = request.json
    print("data",data)
    table_name = data['selectedTable']
    x_axis_columns = data['xAxis'].split(', ')  # Split multiple columns into a list
    y_axis_columns = data['yAxis'] # Assuming yAxis can be multiple columns as well
    aggregation = data['aggregate']
    checked_option = data['filterOptions'] 
    db_nameeee = data['databaseName']
    chartType = data['chartType']
    selectedUser=data['selectedUser']
    calculationData=data['calculation']
    data_limit_type=data['data_limit_type']
    data_limit_column =  None  # Column to sort by for limiting
    user_consent_given =  False  # Track if user already gave consent
    current_x_axis_key = None  # Create a key for current xAxis combination
    selectedFrequency=data['selectedFrequency']
    if not selectedUser or selectedUser.lower() == 'null':
        print("Using default database connection...")
        connection_path = f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}"
    else:
        print(f"Using connection for user: {selectedUser}")
        connection_string = fetch_external_db_connection(db_nameeee, selectedUser)
        if not connection_string:
            raise Exception("Unable to fetch external database connection details.")

        db_details = {
            "host": connection_string[3],
            "database": connection_string[7],
            "user": connection_string[4],
            "password": connection_string[5],
            "port": int(connection_string[6])
        }

        connection_path = f"dbname={db_details['database']} user={db_details['user']} password={db_details['password']} host={db_details['host']} port={db_details['port']}"
    
    database_con = psycopg2.connect(connection_path)
    print("database_con", connection_path)
    # Fetch chart data
    new_df = fetch_chart_data(database_con, table_name)

    # Check if fetched data is valid
    if new_df is None:
        print("Error: fetch_chart_data returned None")
        return jsonify({"error": "Failed to fetch data"}), 500
    
    # Update global DataFrame
    if df is not None and df.equals(new_df):
        print("Both DataFrames are equal")
    else:
        print("DataFrames are not equal")
        bc.global_df = new_df
    print(".......................................",data)
    print(".......................................db_nameeee",db_nameeee)
    print(".......................................checked_option",checked_option)
    print("......................................xAxis.",x_axis_columns)
    print(".......................................yAxis",y_axis_columns)
    print(".......................................table_name",table_name)
    print('......................................selectedUser',selectedUser)

    # NEW: Enhanced function to apply data limiting with caching logic
    def apply_data_limiting(df, limit_type, sort_column, x_cols, y_cols, agg):
        """Apply data limiting based on user selection"""
        if limit_type is None or sort_column is None:
            return df
        
        try:
            # If we need to aggregate first to get meaningful sorting
            if agg and y_cols and len(y_cols) > 0:
                # Group by x-axis columns and aggregate y-axis
                if agg == "sum":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].sum()
                elif agg == "count":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].count()
                elif agg == "average" or agg == "mean":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].mean()
                elif agg == "maximum" or agg == "max":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].max()
                elif agg == "minimum" or agg == "min":
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].min()
                else:
                    grouped_df = df.groupby(x_cols, as_index=False)[y_cols[0]].sum()
                
                sort_by_column = y_cols[0]  # Sort by the aggregated y-axis column
                df_to_limit = grouped_df
            else:
                sort_by_column = sort_column if sort_column in df.columns else (y_cols[0] if y_cols else x_cols[0])
                df_to_limit = df
            
            # Apply the limiting based on type
            if limit_type == "top10":
                limited_df = df_to_limit.nlargest(10, sort_by_column)
            elif limit_type == "bottom10":
                limited_df = df_to_limit.nsmallest(10, sort_by_column)
            elif limit_type == "both5":
                top5 = df_to_limit.nlargest(5, sort_by_column)
                bottom5 = df_to_limit.nsmallest(5, sort_by_column)
                limited_df = pd.concat([top5, bottom5]).drop_duplicates()
            else:
                limited_df = df_to_limit
            
            print(f"Applied {limit_type} limiting: {len(df_to_limit)} -> {len(limited_df)} rows")
            return limited_df
            
        except Exception as e:
            print(f"Error applying data limiting: {e}")
            return df

    # NEW: Enhanced data points check with consent logic
    def check_data_points_limit(data_length, has_consent=False, limit_type=None):
        """Check if data points exceed limit and return appropriate response"""
        if data_length > 30:
            if has_consent and limit_type:
                # User already gave consent and we have limiting - this shouldn't happen
                # But if it does, just log it and continue
                print(f"⚠️ Data still exceeds limit ({data_length}) even with consent and limiting ({limit_type})")
                return None
            elif has_consent and not limit_type:
                # User gave consent but no limiting applied - ask for limiting options
                return {
                    "error": "Data points exceed maximum limit", 
                    "message": "Please select a data limiting option to reduce the data size.",
                    "dataPointCount": data_length,
                    "maxDataPoints": 30,
                    "canApplyLimiting": True,
                    "needsLimitingSelection": True
                }
            else:
                # No consent given - show normal error with consent request
                return {
                    "error": "Data points exceed maximum limit", 
                    "message": "Your categories and values contain more than the maximum 30 data points. Please apply filters or data limiting options to reduce the data size.",
                    "dataPointCount": data_length,
                    "maxDataPoints": 30,
                    "canApplyLimiting": True
                }
        return None
    if chartType in ["singleValueChart","meterGauge"]:
        print("++++++++singleValueChart+++++++_________________singleValueChart_____________________singleValueChart_____________________singleValueChart_____________________________+++++++++++singleValueChart+++++++++++++++++++++++++++")
        create_dynamic_trigger(db_nameeee, table_name
                           )
        aggregate_py = {
                        'count': 'count',
                        'sum': 'sum',
                        'average': 'mean',
                        'minimum': 'min',
                        'maximum': 'max'
                    }.get(aggregation, 'sum') 

        fetched_data = fetchText_data(db_nameeee, table_name, x_axis_columns[0],aggregate_py,selectedUser)
        print("fetched_data=========",fetched_data)
        return jsonify({"data": fetched_data,
                    # "chart_id": chart_id,
                     "message": "Data received successfully!"})
    
    # if chart_data == "singleValueChart":
    elif chartType in ["duealbarChart", "stackedbar"]:
        datass = fetch_data_for_duel_bar(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee,selectedUser,calculationData)
        data = {
             "categories": [row[0] for row in datass],
            "series1": [row[1] for row in datass],
            "series2": [row[2] for row in datass],
            "aggregation": aggregation,
            "x_axis_columns":x_axis_columns,
            "y_axis_columns":y_axis_columns
        }
        print("data====================", data)
        
        return jsonify(data)
    
    elif chartType  == "tablechart"  and len(x_axis_columns) > 0: 
    # elif chartType == "treeHierarchy" and len(x_axis_columns) > 0:  # Tree Hierarchy logic
        print("treeHierarchy-----------------------------")
        try:
            conn = get_company_db_connection(db_nameeee)

            new_df = fetch_chart_data(conn, table_name)
            # new_df = fetch_tree_data(db_nameeee, table_name, x_axis_columns, y_axis_columns,checked_option, selectedUser)

            # Check if we need aggregation
            if aggregation and y_axis_columns:
                if aggregation == "sum":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].sum()
                elif aggregation == "count":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].count()
                elif aggregation == "mean":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].mean()
                else:
                    return jsonify({"error": f"Unsupported aggregation type: {aggregation}"})
            
            # Prepare data for tree hierarchy
            categories = []
            values = []

            for index, row in new_df.iterrows():
                category = {col: row[col] for col in x_axis_columns}  # Hierarchy levels
                categories.append(category)
                values.append(row[y_axis_columns[0]] if y_axis_columns else 1)  # Use aggregated value
            print("aggregation",aggregation)
            return jsonify({
                "categories": categories,
                "values": values,
                "chartType": "treeHierarchy",
            })
           

        except Exception as e:
            print("Error preparing Tree Hierarchy data:", e)
            return jsonify({"error": str(e)})
    elif chartType == "timeSeriesDecomposition":
        print("Time Series Decomposition Chart")
        if not x_axis_columns or not y_axis_columns:
            return jsonify({"error": "Time series decomposition requires a time column and a value column."}), 400
        
        time_column = x_axis_columns[0]
        value_column = y_axis_columns[0]
        print("time_column", time_column)
        print("value_column", value_column)
        print("selectedFrequency",selectedFrequency )


        temp_df_for_ts = fetch_data_for_ts_decomposition(
            table_name, [time_column], checked_option, [value_column], None, db_nameeee, selectedUser, calculationData
        )
        print("temp_df_for_ts", temp_df_for_ts)

        if temp_df_for_ts.empty:
            return jsonify({"error": "No data available for time series decomposition after filtering."}), 400

        temp_df_for_ts.dropna(subset=[time_column], inplace=True)
        temp_df_for_ts[time_column] = pd.to_datetime(temp_df_for_ts[time_column], errors='coerce')
        temp_df_for_ts.set_index(time_column, inplace=True)

        temp_df_for_ts[value_column] = pd.to_numeric(temp_df_for_ts[value_column], errors='coerce')
        temp_df_for_ts.dropna(subset=[value_column], inplace=True)

        if temp_df_for_ts.empty:
            return jsonify({"error": "No valid numeric data for time series decomposition after conversion."}), 400

        # time_series_frequency = data.get('timeSeriesFrequency', 'MS')
        # Set frequency dynamically based on selectedFrequency
        if selectedFrequency == "daily":
            time_series_frequency = 'D'
        elif selectedFrequency == "monthly":
            time_series_frequency = 'MS'  # Month Start
        elif selectedFrequency == "yearly":
            time_series_frequency = 'YS'  # Year Start
        else:
            time_series_frequency = 'MS'  # Default fallback

        print("time_series_frequency", time_series_frequency)

        try:
            # # Decide the resampling aggregation method dynamically
            # if aggregation == "sum":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).sum()
            # elif aggregation == "average":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).mean()
            # elif aggregation == "count":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).count()
            # elif aggregation == "min":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).min()
            # elif aggregation == "max":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).max()
            # elif aggregation == "median":
            #     ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).median()
            # else:
            #     return jsonify({"error": f"Unsupported aggregation type for time series decomposition: {aggregation}"}), 400
            if aggregation == "sum":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).sum()
            elif aggregation == "average":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).mean()
            elif aggregation == "count":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).count()
            elif aggregation ==  "minimum" or aggregation == "min":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).min()
            elif aggregation == "max" or aggregation == "maximum":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).max()
            elif aggregation == "median":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).median()
            elif aggregation == "variance":
                ts_data = temp_df_for_ts[value_column].resample(time_series_frequency).agg('var')
            else:
                return jsonify({"error": f"Unsupported aggregation type for time series decomposition: {aggregation}"}), 400


            # Fill missing values
            ts_data = ts_data.ffill().bfill()
            print("ts_data", ts_data)

            # Determine decomposition period
            if 'D' in time_series_frequency:
                period = 7
            elif 'W' in time_series_frequency:
                period = 52
            elif 'Q' in time_series_frequency:
                period = 4
            else:
                period = min(4, len(ts_data) // 2)

            if len(ts_data) < 2 * period:
                return jsonify({"error": f"Not enough data points for decomposition with period {period}. Need at least {2 * period} data points, but got {len(ts_data)}."}), 400

            decomposition = seasonal_decompose(ts_data, model='additive', period=period)

            trend = decomposition.trend.dropna().tolist()
            seasonal = decomposition.seasonal.dropna().tolist()
            residual = decomposition.resid.dropna().tolist()
            observed = decomposition.observed.dropna().tolist()
            dates = decomposition.observed.dropna().index.strftime('%Y-%m-%d').tolist()

            return jsonify({
                "dates": dates,
                "observed": observed,
                "trend": trend,
                "seasonal": seasonal,
                "residual": residual,
                "aggregation": aggregation,
                "model": "additive"
            })

        except ValueError as ve:
            return jsonify({
                "error": f"Error during time series decomposition: {ve}. This often happens with insufficient data for the chosen period or frequency. Ensure your time column has enough granular data."
            }), 400
        except Exception as e:
            return jsonify({"error": f"An unexpected error occurred during time series decomposition: {e}"}), 500
        
    elif chartType == "treeHierarchy" and len(x_axis_columns) > 0:  # Tree Hierarchy logic
        print("treeHierarchy-----------------------------")
        try:
            # new_df = fetch_tree_data(db_nameeee, table_name, x_axis_columns, y_axis_columns,checked_option, selectedUser)

            # Check if we need aggregation
            if aggregation and y_axis_columns:
                if aggregation == "sum":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].sum()
                elif aggregation == "count":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].count()
                elif aggregation == "mean":
                    new_df = new_df.groupby(x_axis_columns, as_index=False)[y_axis_columns[0]].mean()
                else:
                    return jsonify({"error": f"Unsupported aggregation type: {aggregation}"})
            
            # Prepare data for tree hierarchy
            categories = []
            values = []

            for index, row in new_df.iterrows():
                category = {col: row[col] for col in x_axis_columns}  # Hierarchy levels
                categories.append(category)
                values.append(row[y_axis_columns[0]] if y_axis_columns else 1)  # Use aggregated value
            print("aggregation",aggregation)
            return jsonify({
                "categories": categories,
                "values": values,
                "chartType": "treeHierarchy",
            })
           

        except Exception as e:
            print("Error preparing Tree Hierarchy data:", e)
            return jsonify({"error": str(e)})

    elif len(y_axis_columns) == 1 and chartType != "duealbarChart" and chartType !="stackedbar" and chartType != "timeSeriesDecomposition":
        data = fetch_data(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee,selectedUser,calculationData)
        # print("data====================", data)     
        # categories = {}  
        # for row in data:
        #     category = tuple(row[:-1])
        #     y_axis_value = row[-1]
        #     if category not in categories:
        #         categories[category] = initial_value(aggregation)
        #     update_category(categories, category, y_axis_value, aggregation)      
        # labels = [', '.join(category) for category in categories.keys()]  
        # values = list(categories.values())
        # print("labels====================", labels)
        # print("values====================", values)
        # return jsonify({"categories": labels, "values": values, "aggregation": aggregation})
    
        if aggregation == "count":
            array1 = [item[0] for item in data]
            array2 = [item[1] for item in data]
            
            # Apply data limiting if specified
            if data_limit_type:
                temp_df = pd.DataFrame({'categories': array1, 'values': array2})
                limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
                array1 = limited_df['categories'].tolist()
                array2 = limited_df['values'].tolist()
            
            # Check data points limit with consent logic
            limit_check = check_data_points_limit(len(array1), user_consent_given, data_limit_type)
            if limit_check:
                return jsonify(limit_check), 400
            
            # Return the JSON response for count aggregation
            return jsonify({"categories": array1, "values": array2, "aggregation": aggregation})
            
        elif aggregation == "average":
            array1 = [item[0] for item in data]
            array2 = [item[1] for item in data]
            
            # Apply data limiting if specified
            if data_limit_type:
                temp_df = pd.DataFrame({'categories': array1, 'values': array2})
                limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
                array1 = limited_df['categories'].tolist()
                array2 = limited_df['values'].tolist()
            
            # Check data points limit with consent logic
            limit_check = check_data_points_limit(len(array1), user_consent_given, data_limit_type)
            if limit_check:
                return jsonify(limit_check), 400
            
            return jsonify({"categories": array1, "values": array2, "aggregation": aggregation})
            
        elif aggregation == "variance":
            array1 = [item[0] for item in data]
            array2 = [item[1] for item in data]
            
            # Apply data limiting if specified
            if data_limit_type:
                temp_df = pd.DataFrame({'categories': array1, 'values': array2})
                limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
                array1 = limited_df['categories'].tolist()
                array2 = limited_df['values'].tolist()
            
            # Check data points limit with consent logic
            limit_check = check_data_points_limit(len(array1), user_consent_given, data_limit_type)
            if limit_check:
                return jsonify(limit_check), 400
            
            return jsonify({"categories": array1, "values": array2, "aggregation": aggregation})
        
        # For other aggregation types
        categories = {}
        for row in data:
            category = tuple(row[:-1])
            y_axis_value = row[-1]
            if category not in categories:
                categories[category] = initial_value(aggregation)
            update_category(categories, category, y_axis_value, aggregation)

        labels = [', '.join(category) for category in categories.keys()]
        values = list(categories.values())
        
        # Apply data limiting if specified
        if data_limit_type:
            temp_df = pd.DataFrame({'categories': labels, 'values': values})
            limited_df = apply_data_limiting(temp_df, data_limit_type, data_limit_column or 'values', ['categories'], ['values'], aggregation)
            labels = limited_df['categories'].tolist()
            values = limited_df['values'].tolist()
        
        # Check data points limit with consent logic
        limit_check = check_data_points_limit(len(labels), user_consent_given, data_limit_type)
        if limit_check:
            return jsonify(limit_check), 400
        
        return jsonify({"categories": labels, "values": values, "aggregation": aggregation})

    elif len(y_axis_columns) == 0 and chartType == "wordCloud":
            query = f"""
                SELECT word, COUNT(*) AS word_count
                FROM (
                    SELECT regexp_split_to_table('{checked_option}', '\\s+') AS word
                    FROM {table_name}
                ) AS words
                GROUP BY word
                ORDER BY word_count DESC;
            """
            print("WordCloud SQL Query:", query)

            try:
                # Database connection
                if not selectedUser or selectedUser.lower() == 'null':
                    print("Using default database connection...")
                    connection_string = f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}"
                    connection = psycopg2.connect(connection_string)
                else:
                    print("Using external database connection for user:", selectedUser)
                    savename = data['selectedUser']
                    connection_details = fetch_external_db_connection(db_nameeee, savename)
                    if connection_details:
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
                    else:
                        raise Exception("Unable to fetch external database connection details.")
                
                cursor = connection.cursor()
                cursor.execute(query)
                data = cursor.fetchall()

                categories = [row[0] for row in data]
                values = [row[1] for row in data]
                print("WordCloud Data:", {"categories": categories, "values": values})

                return jsonify({
                    "categories": categories,
                    "values": values,
                    "chartType": "wordCloud"
                })

            except Exception as e:
                print("Error executing WordCloud query:", e)
                return jsonify({"error": str(e)}), 500
        
    elif len(y_axis_columns) == 2:
        datass = fetch_data_for_duel(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee,selectedUser,calculationData)
        data = {
             "categories": [row[0] for row in datass],
            "series1": [row[1] for row in datass],
            "series2": [row[2] for row in datass],
            "aggregation": aggregation
        }
        print("data====================", data)
        
        return jsonify(data)

def fetch_tree_data(db_name, table_name, x_axis_columns, y_axis_columns, checked_option, selectedUser):
    try:
        # Handle internal vs external DB connections
        if not selectedUser or selectedUser.lower() == 'null':
            print("Using default database connection...")
            connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
            conn = psycopg2.connect(connection_string)
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

        cur = conn.cursor()

        # Prepare SQL
        columns = ', '.join(x_axis_columns + y_axis_columns)

        # If filtering options are present
        filter_clause = ""
        if checked_option:
            filter_conditions = []
            for col, values in checked_option.items():
                if values:  # only add filter if there are values for this column
                    value_list = "', '".join(values)
                    filter_conditions.append(f"{col} IN ('{value_list}')")
            if filter_conditions:
                filter_clause = "WHERE " + " AND ".join(filter_conditions)

        query = f"SELECT {columns} FROM {table_name} {filter_clause};"
        print("Executing query:", query)

        # Execute query
        cur.execute(query)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=col_names)

        cur.close()
        conn.close()
        return df

    except Exception as e:
        print("Error fetching tree hierarchy data:", e)
        raise

def edit_initial_value(aggregation):
    if aggregation in ['sum', 'average']:
        return 0
    elif aggregation == 'minimum':
        return float('inf')
    elif aggregation == 'maximum':
        return float('-inf')
    elif aggregation == 'count':
        return 0

def edit_update_category(categories, category_key, y_axis_value, aggregation):
    if aggregation == 'sum':
        categories[category_key] += float(y_axis_value)
    elif aggregation == 'minimum':
        categories[category_key] = min(categories[category_key], float(y_axis_value))
    elif aggregation == 'maximum':
        categories[category_key] = max(categories[category_key], float(y_axis_value))
    elif aggregation == 'average':
        if isinstance(categories[category_key], list):
            categories[category_key][0] += float(y_axis_value)
            categories[category_key][1] += 1
        else:
            categories[category_key] = [float(y_axis_value), 1]
    elif aggregation == 'count':
        categories[category_key] += 1

@app.route('/your-backend-endpoint', methods=['POST','GET'])
def handle_bar_click():
    # conn = psycopg2.connect("dbname=datasource user=postgres password=Gayu@123 host=localhost")
    conn = connect_to_db()
    cur = conn.cursor()
    data = request.json
    clicked_category = data.get('category')
    x_axis_columns = data.get('xAxis')
    y_axis_column = data.get('yAxis')
    table_name = data.get('tableName')
    aggregation = data.get('aggregation')
    print("x_axis_columns====================",x_axis_columns)
    print("aggregate====================",aggregation)
    print("clicked_category====================",clicked_category)
    if aggregation == "sum":
        aggregation = "SUM"
    elif aggregation == "average":
        aggregation = "AVG"
    elif aggregation == "count":
        aggregation = "COUNT"
    elif aggregation == "maximum":
        aggregation = "MAX"
    elif aggregation == "minimum":
        aggregation = "MIN"

    data=drill_down(clicked_category, x_axis_columns, y_axis_column, aggregation)
    # data=drill_down(clicked_category, x_axis_columns, y_axis_column, table_name, aggregation)
    categories={}
    y_axis_values = []
    for row in data:
        category = tuple(row[:-1])
        y_axis_value = int(row[-1])
        y_axis_values.append(y_axis_value)

        if category not in categories:
            categories[category] = initial_value(aggregation)
       
    labels = [', '.join(category) for category in categories.keys()]
    values = list(categories.values())

    return jsonify({"categories": labels, "values": y_axis_values, "aggregation": aggregation})

@app.route('/plot_chart/<selectedTable>/<columnName>', methods=['POST', 'GET'])
def get_filter_options(selectedTable, columnName):
    table_name = selectedTable
    column_name = columnName
    db_name = request.args.get('databaseName')
    selectedUser = request.args.get('selectedUser','null')  # Default to 'local' if not specified
    raw_query = request.query_string.decode()
    parsed_query = parse_qs(raw_query)

    calculation_expr = None
    calc_column = None

    for i in range(10):  # Max 10 calculationData entries
        col_key = f"calculationData[{i}][columnName]"
        expr_key = f"calculationData[{i}][calculation]"
        if parsed_query.get(col_key, [None])[0] == column_name:
            calc_column = parsed_query.get(col_key, [None])[0]
            calculation_expr = parsed_query.get(expr_key, [None])[0]
            break

    # print("table_name====================", table_name)
    # print("db_name====================", db_name)
    # print("column_name====================", column_name)
    # print("selectedUser====================", selectedUser)
    # print("calculationData.calculation====================", calculation_expr)
    # print("calculationData.columnName====================", calc_column)
    # print("calculationData.dbTableName====================", db_table)
    # Fetch data from cache or database
    column_data = fetch_column_name_with_cache(table_name, column_name, db_name, selectedUser,calculation_expr,calc_column)
    
    # print("column_data====================", column_data)
    return jsonify(column_data)

@lru_cache(maxsize=128)
def fetch_column_name_with_cache(table_name, column_name, db_name, selectedUser,calculation_expr,calc_column):
    print("Fetching from database...")
    return fetch_column_name(table_name, column_name, db_name,calculation_expr,calc_column,selectedUser)

@app.route('/clear_cache', methods=['POST'])
def clear_cache():
    fetch_column_name_with_cache.cache_clear()
    return jsonify({"message": "Cache cleared!"})

@app.route('/save_data', methods=['POST'])
def save_data():
    data = request.json
    user_id=data['user_id']
    save_name= data['saveName']
    company_name=data['company_name']   

    print("data====================", data) 
    
    print("company_name====================",company_name)  
    print("user_id====================",user_id)
    print("save_name====================", save_name)
    print("connectionType====================", data)
   
    
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        # chart_heading_json = json.dumps(data.get('chart_heading'))
        raw_heading = data.get('chart_heading')
        clean_heading = None if str(raw_heading).strip().lower() in ["", "undefined", "null"] else raw_heading
        chart_heading_json = json.dumps(clean_heading)

        filter_options_json = json.dumps(data.get('filterOptions')) 
        chart_color_json = json.dumps(data.get('chartColor'))  # ✅ FIX: Convert to JSON
        calculation_data_json = json.dumps(data.get('calculationData'))

        cur.execute("SELECT MAX(id) FROM table_chart_save")
        last_chart_id = cur.fetchone()[0] or 0
        new_chart_id = last_chart_id + 1

        # new_chart_ids.append(new_chart_id)
        print("new_chart_ids",new_chart_id)
        print("calculation_data_json",calculation_data_json)
        cur.execute("""
            INSERT INTO table_chart_save (
                id,
                User_id,
                company_name,                
                chart_name,
                database_name,
                selected_table,
                x_axis,
                y_axis,
                aggregate,
                chart_type,
                chart_color,
                chart_heading,
                drilldown_chart_color,
                filter_options,
                ai_chart_data,
                selectedUser,
                xFontSize,          
                fontStyle,          
                categoryColor,      
                yFontSize,          
                valueColor,headingColor,ClickedTool,Bgcolour,OptimizationData,calculationData,selectedFrequency
            )VALUES (
                %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s
            )
        """, (
            new_chart_id,
            data.get('user_id'),
            data.get('company_name'),       
            data.get('saveName'),  
            data.get('databaseName'),
            data.get('selectedTable'),
            data.get('xAxis'),
            data.get('yAxis'),
            data.get('aggregate'),
            data.get('chartType'),
            # data.get('chartColor'),
            chart_color_json,
            chart_heading_json,
            data.get('drillDownChartColor'),
            filter_options_json,
            json.dumps(data.get('ai_chart_data')),  # Serialize ai_chart_data as JSON
            data.get('selectedUser'),
            data.get('xFontSize'),
            data.get('fontStyle'),
            data.get('categoryColor'),
            data.get('yFontSize'),
            data.get('valueColor'),
            data.get('headingColor'),
            data.get('ClickedTool'),
            data.get('areaColor'),
            data.get('optimizationOption'),
            calculation_data_json,
            data.get('selectedFrequency')

        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'message': 'Data inserted successfully'})
    except Exception as e:
        print("Error:iiiiiiiiiiiii", e)
        return jsonify({'error': str(e)})
    
@app.route('/update_data', methods=['POST'])
def update_data():
    data = request.json
    print("Received data for update:", data)
    def clean_int(value):
        try:
            if value is None or value == "" or str(value).lower() == "null":
                return None
            return int(value)
        except Exception:
            return None

    
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        chart_heading_json = json.dumps(data.get('chart_heading'))
        chart_color_json = json.dumps(data.get('chartColor'))  # ✅ FIX: Convert to JSON
        filter_options_json = json.dumps(data.get('filterOptions')) 
        
        
        # Update data in the table
        cur.execute("""
            UPDATE table_chart_save
            SET

                selected_table = %s,
                x_axis = %s,
                y_axis = %s,
                aggregate = %s,
                chart_type = %s,
                chart_color = %s,
                chart_heading = %s,
                drilldown_chart_color = %s,
                filter_options = %s,
                xFontSize= %s,         
                fontStyle= %s,          
                categoryColor= %s,   
                yFontSize= %s,          
                valueColor= %s,
                headingColor=%s,
                ClickedTool=%s,Bgcolour=%s
            WHERE
                id = %s
        """, (

            data.get('selectedTable'),
            data.get('xAxis'),
            data.get('yAxis'),
            data.get('aggregate'),
            data.get('chartType'),
            chart_color_json,
            chart_heading_json,
            data.get('drillDownChartColor'),
            # data.get('filterOptions'),
            filter_options_json,

            # Assuming there's an 'id' field in the data
            # data.get('xFontSize'),
            clean_int(data.get('xFontSize')),
            data.get('fontStyle'),
            data.get('categoryColor'),
            # data.get('yFontSize'),
            clean_int(data.get('yFontSize')),
            data.get('valueColor'),
            data.get('headingColor'),
            data.get('ClickedTool') ,
            data.get('areaColor'),
            data.get('chartId'),
            
           
        ))
        

        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'message': 'Data updated successfully'})
    except Exception as e:
        print("Error:", e)
        return jsonify({'error': str(e)})

def get_chart_names(user_id, database_name):
    # Step 1: Get employees reporting to the given user_id from the company database.
    conn_company = get_company_db_connection(database_name)
    print("company",database_name)
    reporting_employees = []

    if conn_company:
        try:
            with conn_company.cursor() as cursor:
                # Check if reporting_id column exists dynamically (skip errors if missing).
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name='employee_list' AND column_name='reporting_id'
                """)
                column_exists = cursor.fetchone()

                if column_exists:
                    # Fetch employees who report to the given user_id (including NULL reporting_id if not assigned).
                    # cursor.execute("""
                    #     SELECT employee_id FROM employee_list WHERE reporting_id = %s 
                    # """, (user_id,))
                    # reporting_employees = [row[0] for row in cursor.fetchall()]
                    cursor.execute("""
                        WITH RECURSIVE subordinates AS (
                            SELECT employee_id, reporting_id
                            FROM employee_list
                            WHERE reporting_id = %s

                            UNION

                            SELECT e.employee_id, e.reporting_id
                            FROM employee_list e
                            INNER JOIN subordinates s ON e.reporting_id = s.employee_id
                        )
                        SELECT employee_id FROM subordinates
                        UNION
                        SELECT %s;
                    """, (user_id,user_id))
                    reporting_employees = [row[0] for row in cursor.fetchall()]
                    print("reporting_employees",reporting_employees)
        except psycopg2.Error as e:
            print(f"Error fetching reporting employees: {e}")
        finally:
            conn_company.close()

    # Include the user's own employee_id for fetching their charts.
    # Convert all IDs to integers for consistent data type handling.
    all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]
    print("all_employee_ids",all_employee_ids)
    # Step 2: Fetch dashboard names for these employees from the datasource database.
    conn_datasource = get_db_connection("datasource")
    dashboard_structure = []


    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                # Create placeholders for the IN clause
                placeholders = ', '.join(['%s'] * len(all_employee_ids))
                print("placeholders",placeholders)
                # query = f"""
                #     SELECT user_id, chart_name FROM table_chart_save
                #     WHERE user_id IN ({placeholders}) and company_name = %s
                # """
                query = f"""
                    SELECT user_id, chart_name 
                    FROM table_chart_save
                    WHERE user_id IN ({placeholders}) AND company_name = %s
                    ORDER BY timestamp ASC
                """

                cursor.execute(query, tuple(all_employee_ids)+ (database_name,))
                print("query",query)
                charts = cursor.fetchall()
                print("charts",charts)
                
                # Organize charts by user_id
                # for uid, chart_name in charts:
                #     if uid not in dashboard_structure:
                #         dashboard_structure[uid] = []
                #     dashboard_structure[uid].append(chart_name)
                for uid, chart_name in charts:
                    dashboard_structure.append((uid, chart_name))
        except psycopg2.Error as e:
            print(f"Error fetching dashboard details: {e}")
        finally:
            conn_datasource.close()

    return dashboard_structure

def get_chart_names_Edit(user_id, database_name):
     
    if not isinstance(user_id,list):
        user_id=[user_id]
    # Step 2: Fetch dashboard names for these employees from the datasource database.
    conn_datasource = get_db_connection("datasource")
    dashboard_structure = []
   

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                # Create placeholders for the IN clause
                placeholders = ', '.join(['%s'] * len(user_id))
                print("placeholders",placeholders)
                #
                query = f"""
                    SELECT user_id, chart_name 
                    FROM table_chart_save
                    WHERE user_id IN ({placeholders}) AND company_name = %s
                    ORDER BY timestamp ASC
                """

                cursor.execute(query, tuple(user_id)+ (database_name,))
                print("query",query)
                charts = cursor.fetchall()
                print("charts",charts)
                
                
                for uid, chart_name in charts:
                    dashboard_structure.append((uid, chart_name))
        except psycopg2.Error as e:
            print(f"Error fetching dashboard details: {e}")
        finally:
            conn_datasource.close()

    return dashboard_structure

@app.route('/total_rows_Edit', methods=['GET'])
def chart_names_Edit():
    user_id = request.args.get('user_id')
    database_name = request.args.get('company')  # Getting the database_name

    print("user_id====================", user_id)
    print("database_name====================", database_name)

    # Validate the user_id
    try:
        user_id = int(user_id)  # Convert to integer
    except ValueError:
        return jsonify({'error': 'Invalid user_id. Must be an integer.'})

    # Check if the database_name is valid (you can extend this validation if needed)
    if not database_name:
        return jsonify({'error': 'Invalid or missing database_name.'})

    # Pass the user_id and database_name to the get_chart_names function
    names = get_chart_names_Edit(user_id, database_name)

    print("names====================", names)

    if names is not None:
        return jsonify({'chart_names': names})
    else:
        return jsonify({'error': 'Failed to fetch chart names'})
    
@app.route('/total_rows', methods=['GET'])
@employee_required
def chart_names():
    user_id = request.args.get('user_id')
    database_name = request.args.get('company')  # Getting the database_name

    print("user_id====================", user_id)
    print("database_name====================", database_name)

    # Validate the user_id
    try:
        user_id = int(user_id)  # Convert to integer
    except ValueError:
        return jsonify({'error': 'Invalid user_id. Must be an integer.'})

    # Check if the database_name is valid (you can extend this validation if needed)
    if not database_name:
        return jsonify({'error': 'Invalid or missing database_name.'})

    # Pass the user_id and database_name to the get_chart_names function
    names = get_chart_names(user_id, database_name)

    print("names====================", names)

    if names is not None:
        return jsonify({'chart_names': names})
    else:
        return jsonify({'error': 'Failed to fetch chart names'})

def get_chart_data(chart_name,company_name,user_id):
    print("chart_id====================......................................................",chart_name,company_name,user_id)
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, selected_table, x_axis, y_axis, aggregate, chart_type, chart_color, chart_heading, drilldown_chart_color, filter_options, database_name ,selecteduser, xFontSize,fontStyle,categoryColor, yFontSize,valueColor,headingColor,clickedtool,Bgcolour,calculationData,optimizationdata,selectedfrequency FROM table_chart_save WHERE chart_name = %s AND company_name = %s AND user_id =%s " , (chart_name, company_name,user_id ))
            data = cursor.fetchone()
            print("data",data)
            if data is None:
                print(f"No data found for Chart ID: {chart_name}")
                return None
            
            filterdata = list(data[10])
            cursor.close()
            conn.close()
            return data
        except psycopg2.Error as e:
            print("Error fetching data for Chart", chart_name, ":", e)
            conn.close()
            return None
    else:
        return None

@app.route('/chart_data/<chart_name>/<company_name>', methods=['GET'])
def chart_data(chart_name,company_name):
    # user_id = request.args.get('user_id')
    user_id, chart_name = chart_name.split(",", 1)  # Split only once
    data = get_chart_data(chart_name,company_name,user_id)
    
    print("chart datas------------------------------------------------------------------------------------------------------------------",data)
    if data is not None:
        return jsonify(data)
    else:
        return jsonify({'error': 'Failed to fetch data for Chart {}'.format(chart_name)})

@app.route('/list-excel-files', methods=['GET'])
def list_files():
    directory_structure = {}
    for root, dirs, files in os.walk(BASE_DIR):
        dir_path = os.path.relpath(root, BASE_DIR)
        if dir_path == '.':
            dir_path = ''
        directory_structure[dir_path] = {'dirs': dirs, 'files': files}
    return jsonify(directory_structure)

@app.route('/list-csv-files', methods=['GET'])
def list_csv_files():
    directory_structure = {}

    for root, dirs, files in os.walk(BASE_CSV_DIR):
        dir_path = os.path.relpath(root, BASE_CSV_DIR)
        if dir_path == '.':
            dir_path = ''
        directory_structure[dir_path] = {'dirs': dirs, 'files': files}
    
    return jsonify(directory_structure)

@app.route('/save_all_chart_details', methods=['POST'])
@employee_required
def save_all_chart_details():
    data = request.get_json()
    print("data====================", data)
    user_id=data['user_id']
    charts = data['charts']
    heading=data['heading']
    droppableBgColor=data['droppableBgColor']
    image_positions = data.get('imagePositions', [])
    print("charts====================", charts)
    print("user_id====================", user_id)
    dashboardfilterXaxis = data['dashboardfilterXaxis']
    dashboardClickedCategory = data['selectedCategory']
    project_name = data.get('projectName') 
    print("company_name====================", dashboardClickedCategory)
    file_name = data['fileName']
    company_name=data['company_name']
    print("company_name====================", company_name)
    print("file_name====================", file_name)
    conn = create_connection()
    if conn is None:
        return jsonify({'message': 'Database connection failed'}), 500
    create_dashboard_table(conn)

    image_ids = []
    for img in image_positions:
        image_id = img['id']
        image_ids.append(image_id)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO image_positions (image_id, src, x, y, width, height, zIndex, disableDragging)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (image_id) DO UPDATE SET
                    src = EXCLUDED.src,
                    x = EXCLUDED.x,
                    y = EXCLUDED.y,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height,
                    zIndex = EXCLUDED.zIndex,
                    disableDragging = EXCLUDED.disableDragging
            """, (
                image_id, img['src'], img['x'], img['y'],
                img['width'], img['height'], img['zIndex'],
                img['disableDragging']
            ))
            conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error inserting image: {e}")

  
    # Initialize lists to combine chart details
    combined_chart_details = {
        'user_id': user_id, 
        'file_name': file_name,
        'company_name': company_name,
        'project_name': project_name,
        'chart_ids': [],
        'positions': [],
        'sizes':[],
        'chart_types': [],
        'chart_Xaxes': [],
        'chart_Yaxes': [],
        'chart_aggregates': [],
        'filterdata': [],  # Assuming this remains the same for all charts
        'clicked_category': dashboardClickedCategory , # Assuming this remains the same for all charts
        'heading':heading,
        'chartcolor': [] ,
        'droppableBgColor':droppableBgColor,
        'opacities': [],
        'image_ids': image_ids,
    }
    # chartcolor = data.get('position', [{}])[0].get('areaColor')
   

    for chart in charts:
        chart_id_key = list(chart.keys())[0]
        chart_type = list(chart.keys())[5]
        chart_Xaxis = list(chart.keys())[2]
        chart_Yaxis = list(chart.keys())[3]
        chart_aggregate = list(chart.keys())[4]
        # chartcolor = chart.get('areaColor', '#0000')  # fallback if key missing
        chartcolor = chart.get('areaColor', chart.get('19', '#0000'))
        opacity = chart.get('opacity', chart.get('24', 1))
        processed_chartcolor = chartcolor  # Initialize
        print(f"opacity for chart : {opacity}")
        print("area_color====================", chartcolor)
        # Ensure it's a string for regex check
        if not isinstance(chartcolor, str):
            chartcolor = str(chartcolor) if chartcolor is not None else ''


        # Check for the specific '{#color1,#color2,...}' format
        match = re.match(r'^\{(#[0-9a-fA-F]{6}(?:,#[0-9a-fA-F]{6})*)\}$', chartcolor)
        if match:
            color_string = match.group(1)
            processed_chartcolor = [c.strip() for c in color_string.split(',')]
        elif isinstance(chartcolor, str) and ',' in chartcolor and all(re.match(r'^#[0-9a-fA-F]{6}$', c.strip()) for c in chartcolor.split(',')):
            # Handle comma-separated without curly braces (as in previous logic)
            processed_chartcolor = [c.strip() for c in chartcolor.split(',')]
        else:
            processed_chartcolor = chartcolor  # Keep the original if no specific format is matched


        combined_chart_details['chart_ids'].append(chart.get(chart_id_key))
        combined_chart_details['positions'].append(chart.get('position'))
        combined_chart_details['sizes'].append(chart.get('size'))
        combined_chart_details['chart_types'].append(chart.get(chart_type))
        combined_chart_details['chart_Xaxes'].append(chart.get(chart_Xaxis))
        combined_chart_details['chart_Yaxes'].append(chart.get(chart_Yaxis))
        combined_chart_details['chart_aggregates'].append(chart.get(chart_aggregate))
        combined_chart_details['chartcolor'].append(processed_chartcolor) # Append the single areaColor to all charts
        combined_chart_details['opacities'].append(opacity) # ADDED: Append opacity


    filter_data = fetch_filter_data(conn, combined_chart_details['chart_ids'])
    print("filter_data",filter_data)
    combined_chart_details['filterdata'] = filter_data
    insert_combined_chart_details(conn, combined_chart_details)
    
    conn.close()
    
    return jsonify({
        'message': 'Chart details saved successfully',
        'chart_details': combined_chart_details
    })

def fetch_filter_data(conn, chart_ids):
    """Fetch filter options for the given chart_ids from table_chart_save"""
    cursor = conn.cursor()
    filter_data = []
    
    for chart_id in chart_ids:
        cursor.execute("SELECT filter_options FROM table_chart_save WHERE id = %s", (chart_id,))
        result = cursor.fetchone()
        filter_data.append(result[0] if result else None)  # Maintain order
    
    cursor.close()
    return filter_data

@app.route('/plot_dual_axis_chart', methods=['POST','GET'])
def get_duel_chart_route():
    data = request.json
    table_name = data['selectedTable']
    x_axis_columns = data['xAxis'].split(', ')  # Split multiple columns into a list
    y_axis_column = data['yAxis']
    aggregation = data['aggregate']
    checked_option=data['filterOptions']
    db_nameeee=data['databaseName']
    print("db_nameeee====================",db_nameeee)
    print("checked_option====================",checked_option)
    print("aggregate====================",aggregation) 
    print("x_axis_columns====================",x_axis_columns)
    print("y_axis_column====================",y_axis_column) 
    categories = {}
    for row in data:
        category = tuple(row[:-1])
        y_axis_value = row[-1]
        if category not in categories:
            categories[category] = initial_value(aggregation)
        update_category(categories, category, y_axis_value, aggregation)
    labels = [', '.join(category) for category in categories.keys()]  
    values = list(categories.values())
    print("labels====================",labels)
    print("values====================",values)
    return jsonify({"categories": labels, "values": values, "aggregation": aggregation})

def initial_value(aggregation):
    if aggregation in ['sum', 'average']:
        return 0
    elif aggregation == 'minimum':
        return float('inf')
    elif aggregation == 'maximum':
        return float('-inf')
    elif aggregation == 'count':
        return 0

def update_category(categories, category_key, y_axis_value, aggregation):
    if aggregation == 'sum':
        categories[category_key] += float(y_axis_value)
    elif aggregation == 'minimum':
        categories[category_key] = min(categories[category_key], float(y_axis_value))
    elif aggregation == 'maximum':
        categories[category_key] = max(categories[category_key], float(y_axis_value))
    elif aggregation == 'average':
        categories[category_key][0] += float(y_axis_value)
        categories[category_key][1] += 1
    elif aggregation == 'count':
        categories[category_key] += 1

@app.route('/upload_audio_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if file and allowed_file(file.filename, ALLOWED_EXTENSIONS):
        filename = secure_filename(file.filename)
        file.save(os.path.join(UPLOAD_FOLDER, filename))
        transcript=transcribe_audio_with_timestamps(os.path.join(UPLOAD_FOLDER, filename))
        postgres_dp=save_file_to_db(filename, transcript)
        print("postgres_dp is=======================> ",postgres_dp)
        return jsonify({'message': 'File successfully uploaded','transcription': transcript}), 200
    else:
        return jsonify({'error': 'Invalid file type'}), 400

@app.route('/api/calculation', methods=['POST'])
def handle_calculation():
    data = request.json
    columnName = data.get('columnName')
    calculation = data.get('calculation')
    db_name= data.get('databaseName')
    dbTableName= data.get('dbTableName')
    selectedUser= data.get('selectedUser')
    print("calculation",calculation)
    print("data",data)
    
    dataframe = calculationFetch(db_name,dbTableName,selectedUser)
    print("========================================================before calculation========================================================")
    print(dataframe.head(5))
    print("========================================================")
    if dataframe is None:
        return jsonify({'error': 'Failed to fetch data from the database'}), 500
    
    try:
        new_dataframe = perform_calculation(dataframe.copy(), columnName, calculation)
        print("========================================================after calculation========================================================")
        print(new_dataframe.head(5))
        print("========================================================")

        dataframe_json = new_dataframe.to_json(orient='split')
        return jsonify({'dataframe': dataframe_json})
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        print(f"Error processing dataframe: {e}")
        return jsonify({'error': 'Error processing dataframe'}), 500
    
@app.route('/api/signup', methods=['POST'])
def signup():
    # data = request.json
    # print("data",data)
    userName = request.form.get('userName')
    password = request.form.get('password')
    retypePassword = request.form.get('retypePassword')
    organizationName = request.form.get('organizationName')
    email = request.form.get('email')
    logo = request.files.get('logo')  # ✅ File comes from request.files

    if password != retypePassword:
        return jsonify({'error': 'Passwords do not match'}), 400
    logo_filename = None
    if logo:
        # Ensure safe directory and file names
        org_folder = secure_filename(organizationName.lower().replace(" ", "_"))
        org_folder_path = os.path.join(UPLOAD_ROOT, org_folder)
        os.makedirs(org_folder_path, exist_ok=True)

        filename = secure_filename(logo.filename)
        logo_path = os.path.join(org_folder_path, filename)
        logo.save(logo_path)
        logo_filename = os.path.join(org_folder, filename)  # Store relative path

    try:
        insert_user_data(organizationName, email, userName, password,logo_filename)
        return jsonify({'message': 'User created successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/signUP_username', methods=['GET'])
def get_userdata():
    usersdata = fetch_usersdata()
    return jsonify(usersdata)

class JWTManager:
    @staticmethod
    def generate_tokens(user_data, user_type='user'):
        """Generate both access and refresh tokens"""
        now = datetime.utcnow()
        
        # Access Token Payload
        access_payload = {
            'user_id': user_data.get('user_id') or user_data.get('employee_id'),
            'user_type': user_type,  # 'admin', 'user', 'employee'
            'email': user_data.get('email'),
            'permissions': user_data.get('permissions'),
            'company': user_data.get('company'),
            'role_id': user_data.get('role_id'),
            'exp': now + JWT_ACCESS_TOKEN_EXPIRES,
            'iat': now,
            'type': 'access'
        }
        
        # Refresh Token Payload
        refresh_payload = {
            'user_id': user_data.get('user_id') or user_data.get('employee_id'),
            'user_type': user_type,
            'exp': now + JWT_REFRESH_TOKEN_EXPIRES,
            'iat': now,
            'type': 'refresh',
            'jti': str(uuid.uuid4())  # Unique token ID for revocation
        }
        
        access_token = jwt.encode(access_payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        refresh_token = jwt.encode(refresh_payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_in': JWT_ACCESS_TOKEN_EXPIRES.total_seconds()
        }
    
    @staticmethod
    def decode_token(token):
        """Decode and validate JWT token"""
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            return payload
        except jwt.ExpiredSignatureError:
            return {'error': 'Token has expired'}
        except jwt.InvalidTokenError:
            return {'error': 'Invalid token'}
    
    @staticmethod
    def refresh_access_token(refresh_token):
        """Generate new access token using refresh token"""
        payload = JWTManager.decode_token(refresh_token)
        
        if 'error' in payload:
            return None
        
        if payload.get('type') != 'refresh':
            return None
        
        # Here you would typically check if refresh token is revoked in database
        # For now, we'll assume it's valid
        
        # Get user data again (you might want to fetch fresh data from database)
        user_data = {
            'user_id': payload.get('user_id'),
            'email': payload.get('email'),
            'user_type': payload.get('user_type')
        }
        
        return JWTManager.generate_tokens(user_data, payload.get('user_type'))

def admin_required(f):
    """Decorator to require admin privileges"""
    @wraps(f)
    @jwt_required
    def decorated_function(*args, **kwargs):
        if request.current_user.get('user_type') != 'admin':
            return jsonify({'message': 'Admin privileges required'}), 403
        return f(*args, **kwargs)
    
    return decorated_function

def permission_required(required_permissions):
    """Decorator to check specific permissions"""
    def decorator(f):
        @wraps(f)
        @jwt_required
        def decorated_function(*args, **kwargs):
            user_permissions = request.current_user.get('permissions', [])
            
            # Convert permissions to list if it's a string
            if isinstance(user_permissions, str):
                import json
                try:
                    user_permissions = json.loads(user_permissions)
                except:
                    user_permissions = []
            
            # Check if user has required permissions
            if isinstance(required_permissions, str):
                required_perms = [required_permissions]
            else:
                required_perms = required_permissions
            
            if not any(perm in user_permissions for perm in required_perms):
                return jsonify({'message': 'Insufficient permissions'}), 403
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# Updated Login Route
@app.route('/api/login', methods=['POST'])
def login():
    global company_name_global
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')
    company = data.get('company')
    
    if not email or not password:
        return jsonify({'message': 'Email and password are required'}), 400
    
    if company is not None:
        company_name_global = company

    print("company_name====================", company)
    print("email====================", email)

    # Super Admin Login
    if email == 'superadmin@gmail.com' and password == 'superAdmin':
        user_data = {
            'user_id': 'superadmin',
            'email': email,
            'user_type': 'admin',
            'permissions': ['all'],
            'company': None,
            'role_id': None
        }
        
        tokens = JWTManager.generate_tokens(user_data, 'admin')
        print("Admin Token------",tokens)
        
        return jsonify({
            'message': 'Login successful',
            'user_type': 'admin',
            'access_token': tokens['access_token'],
            'refresh_token': tokens['refresh_token'],
            'expires_in': tokens['expires_in'],
            'user_data': {
                'email': email,
                'user_type': 'admin'
            }
        }), 200
    
    # Regular User/Employee Login
    usersdata = None
    employeedata = None
    
    if company is None:
        usersdata = fetch_login_data(email, password)
        print("usersdata", usersdata)
        
        if usersdata:
            user_data = {
                'user_id': usersdata.get('user_id') or usersdata.get('id'),
                'email': email,
                'user_type': 'user',
                'permissions': usersdata.get('permissions', []),
                'company': None,
                'role_id': usersdata.get('role_id')
            }
            
            tokens = JWTManager.generate_tokens(user_data, 'user')
            print("Cleent user Token------",tokens)
            
            return jsonify({
                'message': 'Login successful',
                'user_type': 'user',
                'access_token': tokens['access_token'],
                'refresh_token': tokens['refresh_token'],
                'expires_in': tokens['expires_in'],
                'user_data': usersdata
            }), 200
    else:
        employeedata = fetch_company_login_data(email, password, company)
        print("employeedata====================", employeedata)
        
        if employeedata:
            user_info = employeedata['user']
            user_data = {
                'employee_id': user_info[0],  # employee_id
                'user_id': user_info[0],
                'email': user_info[3],  # email
                'user_type': 'employee',
                'permissions': employeedata.get('permissions', []),
                'company': company,
                'role_id': user_info[2]  # role_id
            }
            
            tokens = JWTManager.generate_tokens(user_data, 'employee')
            print("Employee Token------------------",tokens)
            
            return jsonify({
                'message': 'Login successful',
                'user_type': 'employee',
                'access_token': tokens['access_token'],
                'refresh_token': tokens['refresh_token'],
                'expires_in': tokens['expires_in'],
                'user_data': employeedata
            }), 200
    
    return jsonify({'message': 'Invalid credentials'}), 401

# Token Refresh Route
@app.route('/api/refresh', methods=['POST'])
def refresh_token():
    data = request.get_json()
    refresh_token = data.get('refresh_token')
    
    if not refresh_token:
        return jsonify({'message': 'Refresh token is required'}), 400
    
    new_tokens = JWTManager.refresh_access_token(refresh_token)
    
    if not new_tokens:
        return jsonify({'message': 'Invalid refresh token'}), 401
    
    return jsonify({
        'message': 'Token refreshed successfully',
        'access_token': new_tokens['access_token'],
        'refresh_token': new_tokens['refresh_token'],
        'expires_in': new_tokens['expires_in']
    }), 200

# Logout Route (Token Revocation)
@app.route('/api/logout', methods=['POST'])
@jwt_required
def logout():
    # In a production environment, you would add the token to a blacklist
    # stored in Redis or database with expiration time
    # For now, we'll just return success
    
    return jsonify({'message': 'Logged out successfully'}), 200

# Protected Route Examples
@app.route('/api/profile', methods=['GET'])
@jwt_required
def get_profile():
    user = request.current_user
    return jsonify({
        'user_id': user.get('user_id'),
        'email': user.get('email'),
        'user_type': user.get('user_type'),
        'permissions': user.get('permissions'),
        'company': user.get('company')
    }), 200

@app.route('/api/admin/users', methods=['GET'])
@admin_required
def get_all_userss():
    # Admin only route
    return jsonify({'message': 'Admin access granted', 'users': []}), 200

@app.route('/api/employee/users', methods=['GET'])
@employee_required
def get_empployees():
    try:
        conn = get_db_connection()  # Your DB connection function
        cursor = conn.cursor()

        # Fetch all employees with all columns
        cursor.execute("SELECT * FROM employees")
        rows = cursor.fetchall()

        # Get column names from cursor.description
        col_names = [desc[0] for desc in cursor.description]

        # Convert to list of dicts
        employees = [dict(zip(col_names, row)) for row in rows]

        cursor.close()
        conn.close()

        return jsonify({
            'message': 'Employee access granted',
            'users': employees
        }), 200

    except Exception as e:
        return jsonify({
            'message': 'Error fetching employees',
            'error': str(e)
        }), 500

@app.route('/api/data/sensitive', methods=['GET'])
@permission_required(['read_sensitive_data'])
def get_sensitive_data():
    # Route that requires specific permission
    return jsonify({'message': 'Sensitive data access granted'}), 200

@app.route('/api/singlevalue_text_chart', methods=['POST'])
def receive_single_value_chart_data():
    data = request.get_json()
    print("data====================",data)
    chart_id=data.get('chart_id')
    # x_axis = data.get('text_y_xis')[0]
    x_axis_list = data.get('text_y_xis') or []

    if not x_axis_list:
        return jsonify({"error": "x-axis field is required and cannot be empty"}), 400

    x_axis = x_axis_list[0]
    databaseName = data.get('text_y_database')
    table_Name = data.get('text_y_table')
    selectedUser=data.get("selectedUser")
    print("table_Name====================",table_Name)
    aggregate=data.get('text_y_aggregate')
    print("x_axis====================",x_axis)  
    print("databaseName====================",databaseName)  
    print("table_Name====================",table_Name)
    print("aggregate====================",aggregate)
    if not databaseName or not table_Name:
        return jsonify({"error": "Database name and table name required"}), 400
    create_dynamic_trigger(databaseName, table_Name)

    aggregate_py = {
                    'count': 'count',
                    'sum': 'sum',
                    'average': 'avg',
                    'minimum': 'min',
                    'maximum': 'max'
                }.get(aggregate, 'sum') 
    fetched_data = fetchText_data(databaseName, table_Name, x_axis,aggregate_py,selectedUser)
    print("Fetched Data:", fetched_data)
    print(f"Received x_axis: {x_axis}")
    print(f"Received databaseName: {databaseName}")
    print(f"Received table_Name: {table_Name}")
    print(f"aggregate====================",{aggregate})
    return jsonify({"data": fetched_data,
                    "chart_id": chart_id,
                     "message": "Data received successfully!"})

@app.route('/api/text_chart', methods=['POST'])
def receive_chart_data():
    data = request.get_json()
    print("data====================",data)
    chart_id=data.get('chart_id')
    x_axis = data.get('text_y_xis')[0]
    databaseName = data.get('text_y_database')
    table_Name = data.get('text_y_table')[0]
    print("table_Name====================", table_Name)
    aggregate=data.get('text_y_aggregate')
    selectedUser=data.get("selectedUser")
    print("x_axis====================",x_axis)  
    print("databaseName====================",databaseName)  
    print("table_Name====================",table_Name)
    print("aggregate====================",aggregate)
    print("selectedUser====================",selectedUser)
    
    fetched_data = fetchText_data(databaseName, table_Name, x_axis,aggregate,selectedUser)
    print("Fetched Data:", fetched_data)
    print(f"Received x_axis: {x_axis}")
    print(f"Received databaseName: {databaseName}")
    print(f"Received table_Name: {table_Name}")
    print(f"aggregate====================",{aggregate})

    return jsonify({"data": fetched_data,
                    "chart_id": chart_id,
                     "message": "Data received successfully!"})

@app.route('/api/text_chart_view', methods=['POST'])
def receive_view_chart_data():
    data = request.get_json()
    print("data====================",data)
    chart_id=data.get('chart_id')
    x_axis = data.get('text_y_xis')[0]
    databaseName = data.get('text_y_database')
    table_Name = data.get('text_y_table')
    aggregate=data.get('text_y_aggregate')
    
    print("x_axis====================",x_axis)  
    print("databaseName====================",databaseName)  
    print("table_Name====================",table_Name)
    print("aggregate====================",aggregate)
    connection =get_db_connection()
        
        # Fetch selectedUser from the database based on chart_id
    query = "SELECT selectedUser FROM table_chart_save WHERE id = %s"
    cursor = connection.cursor()
    cursor.execute(query, (chart_id,))  # Ensure chart_id is passed as a tuple
    selectedUser = cursor.fetchone()
    print("selectedUser",selectedUser)
    if not selectedUser:
            print("No selectedUser found for chart_id:", chart_id)
            return {"error": "No user found for the given chart ID"}
        
    selectedUser = selectedUser[0]  # Extract value from tuple
    print("Fetched selectedUser:", selectedUser)
    fetched_data = fetchText_data(databaseName, table_Name, x_axis,aggregate,selectedUser)
    print("Fetched Data:", fetched_data)
    print(f"Received x_axis: {x_axis}")
    print(f"Received databaseName: {databaseName}")
    print(f"Received table_Name: {table_Name}")
    print(f"aggregate====================",{aggregate})

    return jsonify({"data": fetched_data,
                    "chart_id": chart_id,
                     "message": "Data received successfully!"})


@app.route('/api/handle-clicked-category',methods=['POST'])
def handle_clicked_category():
    data=request.json
    category = data.get('category')
    charts= data.get('charts')  
    clicked_catagory_Xaxis=data.get('x_axis')
    # print("data", data)
    print("Category clicked:", category)
    database_name = data.get('databaseName')
    print("clicked_catagory_Xaxis====================",clicked_catagory_Xaxis)
    # print("charts===================",charts)
    if isinstance(clicked_catagory_Xaxis, list):
            clicked_catagory_Xaxis = ', '.join(clicked_catagory_Xaxis)
    
    # print("Charts:", charts)    
    chart_details = []
    print("chart_details :", chart_details)
    chart_data_list = []
    print("chart_data_list :", chart_data_list)
    if charts:
        charts_count = len(charts)  
        print("Charts count:", charts_count)
        for chart in charts:   
            chart_id = chart.get('chart_id')
            table_name = chart.get('table_name')
            x_axis = chart.get('x_axis')
            y_axis = chart.get('y_axis')
            aggregate = chart.get('aggregate')
            chart_type = chart.get('chart_type')
            filter_options = chart.get('filter_options')
            calculationData=chart.get("calculationData")
            # database_name = chart.get('databaseName')
            
            if isinstance(x_axis, list):
                x_axis = ', '.join(x_axis)
            
            if isinstance(y_axis, list):
                y_axis = ', '.join(y_axis)
            chart_details.append([{
                'chart_id': chart_id,
                'table_name': table_name,
                'chart_type':chart_type,
                'x_axis': x_axis,
                'y_axis': y_axis,
                'aggregate': aggregate,
                'filter_options': filter_options,
                'database_name': database_name,
                'calculationData':calculationData
            }])
            
            print("x_axis====================",x_axis)
            print("chart_type====================",chart_type)
            chart_data = filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate,clicked_catagory_Xaxis,category,chart_id,calculationData,chart_type)
            chart_data_list.append({
                "chart_id": chart_id,
                "data": chart_data
            })
        print(chart_data_list)
    return jsonify({"message": "Category clicked successfully!",
                    "chart_data_list": chart_data_list})

def apply_calculation_to_df(df, calculation_data_list, x_axis=None, y_axis=None):
    if not calculation_data_list:
        return df, x_axis, y_axis

    for calculation_data in calculation_data_list:
        try:
            if not (calculation_data.get('calculation') and calculation_data.get('columnName')):
                continue

            calc_formula = calculation_data['calculation']
            new_col_name = calculation_data['columnName']
            replace_col_name = calculation_data.get('replaceColumn', new_col_name)

            if new_col_name not in (x_axis or []) and new_col_name not in (y_axis or []):
                continue  # Skip if not used in chart

            def replace_column(match):
                col_name = match.group(1)
                if col_name in df.columns:
                    return f"df['{col_name}']"
                else:
                    raise ValueError(f"Column {col_name} not found in DataFrame.")

            formula_lower = calc_formula.strip().lower()

            if formula_lower.startswith("if"):
                match = re.match(r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$", calc_formula.strip(), re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid IF format")
                condition_expr, then_val, else_val = match.groups()
                condition_expr_python = re.sub(r'\[(.*?)\]', replace_column, condition_expr)
                df[new_col_name] = np.where(eval(condition_expr_python), then_val.strip("'\""), else_val.strip("'\""))

            elif formula_lower.startswith("switch"):
                switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calc_formula, re.IGNORECASE)
                if not switch_match:
                    raise ValueError("Invalid SWITCH format")
                col_name, rest = switch_match.groups()
                if col_name not in df.columns:
                    raise ValueError(f"Column '{col_name}' not found")
                cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
                default_match = re.search(r'default\s*,\s*["\']?(.*?)["\']?$', rest, re.IGNORECASE)
                default_val = default_match.group(1) if default_match else None
                df[new_col_name] = df[col_name].map(dict(cases)).fillna(default_val)

            elif formula_lower.startswith("iferror"):
                match = re.match(r"iferror\s*\((.+?)\s*,\s*(.+?)\)", calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid IFERROR format")
                expr, fallback = match.groups()
                expr_python = re.sub(r'\[(.*?)\]', replace_column, expr)
                try:
                    df[new_col_name] = eval(expr_python)
                    df[new_col_name] = df[new_col_name].fillna(fallback)
                except:
                    df[new_col_name] = fallback

            elif formula_lower.startswith("calculate"):
                match = re.match(r"calculate\s*\(\s*(sum|avg|count|max|min)\s*\(\s*\[([^\]]+)\]\)\s*,\s*\[([^\]]+)\]\s*=\s*['\"](.*?)['\"]\s*\)", calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid CALCULATE format")
                agg, value_col, filter_col, filter_val = match.groups()
                df_filtered = df[df[filter_col] == filter_val]
                result_val = {
                    "sum": df_filtered[value_col].astype(float).sum(),
                    "avg": df_filtered[value_col].astype(float).mean(),
                    "count": df_filtered[value_col].count(),
                    "max": df_filtered[value_col].astype(float).max(),
                    "min": df_filtered[value_col].astype(float).min(),
                }[agg]
                df[new_col_name] = result_val

            elif formula_lower.startswith(("maxx", "minx")):
                match = re.match(r"(maxx|minx)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE)
                func, col = match.groups()
                df[new_col_name] = df[col].max() if func.lower() == "maxx" else df[col].min()

            elif formula_lower.startswith("abs"):
                col = re.match(r"abs\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                df[new_col_name] = df[col].abs()

            elif formula_lower.startswith("len"):
                col = re.match(r"len\s*\(\s*(?:\[([^\]]+)\]|\"([^\"]+)\")\s*\)", calc_formula, re.IGNORECASE).groups()
                df[new_col_name] = df[col[0] or col[1]].astype(str).str.len()

            elif formula_lower.startswith("lower"):
                col = re.match(r"lower\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                df[new_col_name] = df[col].astype(str).str.lower()

            elif formula_lower.startswith("upper"):
                col = re.match(r"upper\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                df[new_col_name] = df[col].astype(str).str.upper()

            elif formula_lower.startswith("concat"):
                parts = re.split(r",(?![^\[]*\])", re.match(r"concat\s*\((.+)\)", calc_formula, re.IGNORECASE).group(1))
                concat_parts = []
                for part in parts:
                    part = part.strip()
                    if part.startswith("[") and part.endswith("]"):
                        col = part[1:-1]
                        concat_parts.append(df[col].astype(str))
                    else:
                        concat_parts.append(part.strip('"').strip("'"))
                df[new_col_name] = reduce(lambda x, y: x + y, [p if isinstance(p, pd.Series) else pd.Series([p]*len(df)) for p in concat_parts])

            elif re.match(r"(year|month|day)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE):
                func, col = re.match(r"(year|month|day)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).groups()
                df[col] = pd.to_datetime(df[col], errors="coerce")
                df[new_col_name] = getattr(df[col].dt, func.lower())

            elif formula_lower.startswith("isnull"):
                col, fallback = re.match(r"isnull\s*\(\s*\[([^\]]+)\]\s*,\s*['\"]?(.*?)['\"]?\s*\)", calc_formula, re.IGNORECASE).groups()
                df[new_col_name] = df[col].fillna(fallback)

            elif re.match(r"(?:\[([^\]]+)\]|\"([^\"]+)\")\s+in\s*\((.*?)\)", calc_formula, re.IGNORECASE):
                match = re.match(r"(?:\[([^\]]+)\]|\"([^\"]+)\")\s+in\s*\((.*?)\)", calc_formula, re.IGNORECASE)
                col = match.group(1) or match.group(2)
                values = [v.strip().strip('"').strip("'") for v in match.group(3).split(",")]
                df[new_col_name] = df[col].isin(values)

            elif formula_lower.startswith("datediff"):
                end_col, start_col = re.match(r"datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).groups()
                df[end_col] = pd.to_datetime(df[end_col], errors="coerce")
                df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
                df[new_col_name] = (df[end_col] - df[start_col]).dt.days

            elif formula_lower.startswith("today()"):
                df[new_col_name] = pd.Timestamp.today().normalize()

            elif formula_lower.startswith("now()"):
                df[new_col_name] = pd.Timestamp.now()

            elif formula_lower.startswith("dateadd"):
                col, interval, unit = re.match(r"dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*['\"](day|month|year)['\"]\)", calc_formula, re.IGNORECASE).groups()
                interval = int(interval)
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if unit == "day":
                    df[new_col_name] = df[col] + pd.to_timedelta(interval, unit="d")
                elif unit == "month":
                    df[new_col_name] = df[col] + pd.DateOffset(months=interval)
                elif unit == "year":
                    df[new_col_name] = df[col] + pd.DateOffset(years=interval)

            elif formula_lower.startswith("formatdate"):
                col, fmt = re.match(r"formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*['\"](.+?)['\"]\)", calc_formula, re.IGNORECASE).groups()
                df[col] = pd.to_datetime(df[col], errors="coerce")
                fmt_mapped = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
                df[new_col_name] = df[col].dt.strftime(fmt_mapped)

            elif formula_lower.startswith("replace"):
                col, old, new = re.match(r"replace\s*\(\s*\[([^\]]+)\]\s*,\s*['\"](.*?)['\"]\s*,\s*['\"](.*?)['\"]\)", calc_formula, re.IGNORECASE).groups()
                df[new_col_name] = df[col].astype(str).str.replace(old, new, regex=False)

            elif formula_lower.startswith("trim"):
                col = re.match(r"trim\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                df[new_col_name] = df[col].astype(str).str.strip()

            else:
                calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
                df[new_col_name] = eval(calc_formula_python)

            print(f"✅ New column '{new_col_name}' created.")

            # Replace in axes
            if y_axis:
                y_axis = [new_col_name if col == replace_col_name else col for col in y_axis]
            if x_axis:
                x_axis = [new_col_name if col == replace_col_name else col for col in x_axis]

        except Exception as e:
            print(f"❌ Error applying calculation for column '{calculation_data.get('columnName')}': {e}")

    return df, x_axis, y_axis

@app.route('/api/send-chart-details', methods=['POST'])
def receive_chart_details():  
    data = request.get_json()
    print("Received data:", data)
    print("Keys:", data.keys())
    calculation_data = data.get('calculationData')
    print("calculationData:", calculation_data)
    chart_id = data.get('chart_id')
    tableName = data.get('tableName')
    x_axis = data.get('x_axis')  # Assuming this is a list of columns to group by
    y_axis = data.get('y_axis')  # Assuming this is a list of columns to aggregate
    aggregate = data.get('aggregate')  # Aggregation method, e.g., 'sum', 'mean', etc.
    chart_type = data.get('chart_type')
    chart_heading = data.get('chart_heading')
    optimizeData= data.get('optimizeData')
    try:
        filter_options_str = data.get('filter_options').replace('null', 'null') #this line is not needed.
        filter_options = json.loads(data.get('filter_options')) #use json.loads instead of ast.literal_eval.
        # print("filter_options====================", filter_options)
    except (ValueError, SyntaxError, json.JSONDecodeError) as e:
        print(f"Error parsing filter_options: {e}")
        return jsonify({"message": "Invalid filter_options format", "error": str(e)}), 400


    # print("filter_options====================", filter_options)
    databaseName = data.get('databaseName')
    
    print("optimizeData====================", optimizeData)

    # Define aggregate function based on request
    aggregate_py = {
        'count': 'count',
        'sum': 'sum',
        'average': 'mean',
        'minimum': 'min',
        'maximum': 'max'
    }.get(aggregate, 'sum')  # Default to 'sum' if no match

    try:
        connection =get_db_connection()
        
        # Fetch selectedUser from the database based on chart_id
        query = f"SELECT selectedUser FROM table_chart_save WHERE id = %s"
        cursor = connection.cursor()
        cursor.execute(query, (chart_id,))
        selectedUser = cursor.fetchone()
        print("selectedUser",selectedUser)
        # connection = get_db_connection_view(databaseName)
        # df = fetch_chart_data(connection, tableName)
        if selectedUser is None:
            print("No selectedUser found for this chart_id.")
            connection = get_db_connection_view(databaseName)
        else:
            selectedUser = selectedUser[0]  # Extract the actual value from the tuple
            print("Fetched selectedUser:", selectedUser)


            if selectedUser:
                connection = fetch_external_db_connection(databaseName, selectedUser)
                host = connection[3]
                dbname = connection[7]
                user = connection[4]
                password = connection[5]

                # Create a new psycopg2 connection using the details from the tuple
                connection = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host
                )
                print('External Connection established:', connection)
            else:
                print("No valid selectedUser found.")
                connection = get_db_connection_view(databaseName)
        masterdatabasecon=get_db_connection()
        df = fetch_chart_data(connection, tableName)
        print(df.head())
        
        
        # Logic for 'singleValueChart'
        if chart_type in ["singleValueChart","meterGauge"]:
            try:
                df[y_axis[0]] = pd.to_numeric(df[y_axis[0]], errors='coerce')
                single_value = df[y_axis[0]].agg(aggregate_py)
                print(f"Single value computed: {single_value}")

                connection.close()

                # Return the single value as part of the response
                return jsonify({
                    "message": "Single value chart details received successfully!",
                    "single_value": float(single_value),  # Convert Decimal to float
                    "chart_type": chart_type,
                    "chart_heading": chart_heading,
                }), 200

            except Exception as e:
                print(f"Error processing singleValueChart: {e}")
                connection.close()
                return jsonify({"message": "Error processing single value chart", "error": str(e)}), 500
        if chart_type == 'timeSeriesDecomposition':
            try:
                selectedFrequency = data.get("selectedFrequency",)
                print("selectedFrequency",selectedFrequency)
                time_column = x_axis[0]
                value_column = y_axis[0]

                df = fetch_data_for_ts_decomposition(
                    tableName, [time_column], filter_options, [value_column], None, databaseName, selectedUser, calculation_data
                )

                if df.empty:
                    return jsonify({"error": "No data available for time series decomposition after filtering."}), 400

                df.dropna(subset=[time_column], inplace=True)
                df[time_column] = pd.to_datetime(df[time_column], errors='coerce')
                df.set_index(time_column, inplace=True)
                df[value_column] = pd.to_numeric(df[value_column], errors='coerce')
                df.dropna(subset=[value_column], inplace=True)

                if selectedFrequency == "daily":
                    time_series_frequency = 'D'
                elif selectedFrequency == "monthly":
                    time_series_frequency = 'MS'
                elif selectedFrequency == "yearly":
                    time_series_frequency = 'YS'
                else:
                    time_series_frequency = 'MS'

                print("Resampling frequency:", time_series_frequency)

                # Apply aggregation
                if aggregate == "sum":
                    ts_data = df[value_column].resample(time_series_frequency).sum()
                elif aggregate == "average":
                    ts_data = df[value_column].resample(time_series_frequency).mean()
                elif aggregate == "count":
                    ts_data = df[value_column].resample(time_series_frequency).count()
                elif aggregate in ["minimum", "min"]:
                    ts_data = df[value_column].resample(time_series_frequency).min()
                elif aggregate in ["maximum", "max"]:
                    ts_data = df[value_column].resample(time_series_frequency).max()
                elif aggregate == "median":
                    ts_data = df[value_column].resample(time_series_frequency).median()
                elif aggregate == "variance":
                    ts_data = df[value_column].resample(time_series_frequency).var()
                else:
                    return jsonify({"error": f"Unsupported aggregation type: {aggregate}"}), 400

                ts_data = ts_data.ffill().bfill()

                # Choose decomposition period
                if 'D' in time_series_frequency:
                    period = 7
                elif 'W' in time_series_frequency:
                    period = 52
                elif 'Q' in time_series_frequency:
                    period = 4
                else:
                    period = min(4, len(ts_data) // 2)

                if len(ts_data) < 2 * period:
                    return jsonify({
                        "error": f"Not enough data for decomposition. At least {2 * period} points needed."
                    }), 400

                decomposition = seasonal_decompose(ts_data, model='additive', period=period)

               
                observed = decomposition.observed.dropna().tolist()
                dates = decomposition.observed.dropna().index.strftime('%Y-%m-%d').tolist()

                return jsonify({
                    "chart_type": "timeSeriesDecomposition",
                    "categories": dates,
                    "values": observed,
                    "aggregation": aggregate,
                    "selectedFrequency": selectedFrequency,
                    "chart_heading": chart_heading,
                    
                }), 200

            except Exception as e:
                print(f"Error in timeSeriesDecomposition: {e}")
                return jsonify({"error": f"Failed to process time series decomposition: {e}"}), 500

        if chart_type == 'AiCharts':
            try:
                
                df = fetch_ai_saved_chart_data(masterdatabasecon, tableName="table_chart_save",chart_id=chart_id)
                print("AI/ML chart details:", df)
                return jsonify({
                    "histogram_details": df,
                }), 200
            except Exception as e:
                print("Error while processing chart:", e)
                return jsonify({"error": "An error occurred while generating the chart."}), 500
        if chart_type == 'sampleAitestChart':
            try:
                df = fetch_chart_data(connection, tableName)
                df, numeric_columns, text_columns = handle_column_data_types(df)
                histogram_details = generate_histogram_details(df)
                connection.close()
                return jsonify({
                    "histogram_details": histogram_details,
                }), 200
            except Exception as e:
                print("Error while processing chart:", e)
                return jsonify({"error": "An error occurred while generating the chart."}), 500
        # if chart_type != 'treeHierarchy':
        if chart_type != 'treeHierarchy' and chart_type != 'tablechart':

            if chart_type in ["duealbarChart", "stackedbar"]:
                    datass = fetch_data_for_duel_bar(tableName, x_axis, filter_options, y_axis, aggregate, databaseName,selectedUser,calculation_data)
                    print("Duel/Stacked bar")
                    return jsonify({
                                "message": "Chart details received successfully!",
                                "categories": [row[0] for row in datass],
                                "series1":[row[1] for row in datass],
                                "series2": [row[2] for row in datass],
                                "chart_type": chart_type,
                                "chart_heading": chart_heading,
                                "x_axis": x_axis,
                                "y_axis": y_axis,
                                "optimizeData": optimizeData,   
                            }), 200
            
            if chart_type == "Butterfly"  :
                    print("Dual y-axis chart detected")
                    # print("filter_options====================", filter_options)
                    # print("filtertype====================", type(filter_options))
                    data = fetch_data_for_duel(tableName, x_axis, filter_options, y_axis, aggregate, databaseName, selectedUser,calculation_data)
                    # print("Data from fetch_data_for_duel:")  # Print before returning
                    # print("Categories:", [row[0] for row in data])
                    # print("Series1:", [row[1] for row in data])
                    # print("Series2:", [row[2] for row in data])
                    return jsonify({
                        "categories": [row[0] for row in data],
                        "series1": [row[1] for row in data],
                        "series2": [row[2] for row in data],
                        # "dataframe": df_json
                         "optimizeData": optimizeData, 
                    })
            if chart_type == "duealChart"  :
                    print("Dual y-axis chart detected")
                    # print("filter_options====================", filter_options)
                    # print("filtertype====================", type(filter_options))
                    data = fetch_data_for_duel(tableName, x_axis, filter_options, y_axis, aggregate, databaseName, selectedUser,calculation_data)
                    # print("Data from fetch_data_for_duel:")  # Print before returning
                    # print("Categories:", [row[0] for row in data])
                    # print("Series1:", [row[1] for row in data])
                    # print("Series2:", [row[2] for row in data])
                    
                    return jsonify({
                        "categories": [row[0] for row in data],
                        "series1": [row[1] for row in data],
                        "series2": [row[2] for row in data],
                        # "dataframe": df_json
                         "optimizeData": optimizeData, 
                    })
            
            # if aggregate == 'count':
              
            #     df, x_axis, y_axis = apply_calculation_to_df(df, calculation_data, x_axis, y_axis)

            #     grouped_df = df.groupby(x_axis[0]).size().reset_index(name="count")
            #     print("Grouped DataFrame with all rows:", grouped_df)
            #     filtered_df_valid = df[df[y_axis[0]].notnull()]
            #     grouped_df_valid = filtered_df_valid.groupby(x_axis[0])[y_axis[0]].count().reset_index(name="count")
            #     print("Grouped DataFrame with valid rows:", grouped_df_valid)
            #     chosen_grouped_df = grouped_df_valid
            #     categories = chosen_grouped_df[x_axis[0]].tolist()
            #     values = chosen_grouped_df["count"].tolist()
            #     # print("categories====================", categories)
            #     # print("values====================", values)
            #     allowed_categories = filter_options.get(x_axis[0], [])  # Get the list of valid categories
            #     filtered_categories = []
            #     filtered_values = []

            #     for category, value in zip(categories, values):
            #         # if category.strip() in allowed_categories:  # Ensure category matches the filter
            #         if str(category).strip() in allowed_categories:
            #             filtered_categories.append(category)
            #             filtered_values.append(value)
            #         else:
            #             print(f"Category '{category}' not in filter_options[{x_axis[0]}]")


            #     # print("filtered_categories====================3333", filtered_categories)
            #     # print("filtered_values====================33333", filtered_values)

            #     connection.close()

            #     return jsonify({
            #         "message": "Chart details received successfully!",
            #         "categories": filtered_categories,
            #         "values": filtered_values,
            #         "chart_type": chart_type,
            #         "chart_heading": chart_heading,
            #         "x_axis": x_axis,
            #         "y_axis": y_axis,
            #          "optimizeData": optimizeData, 
            #     }), 200

            if aggregate == 'count':
                print("Count aggregation detected")
                
                df, x_axis, y_axis = apply_calculation_to_df(df, calculation_data, x_axis, y_axis)
                
                # Parse the filter options
                allowed_categories = json.loads(data.get('filter_options'))
                print("allowed_categories====================", allowed_categories)
                
                # Apply filters to the dataframe BEFORE grouping
                filtered_df = df.copy()
                
                # Apply each filter condition
                for column, valid_values in allowed_categories.items():
                    if column in filtered_df.columns:
                        # Filter the dataframe to only include rows where the column value is in valid_values
                        filtered_df = filtered_df[filtered_df[column].isin(valid_values)]
                        print(f"After filtering {column}: {len(filtered_df)} rows remaining")
                    else:
                        print(f"Warning: Column '{column}' not found in dataframe")
                
                print(f"Original dataframe rows: {len(df)}")
                print(f"Filtered dataframe rows: {len(filtered_df)}")
                
                # Now group the filtered dataframe
                grouped_df = filtered_df.groupby(x_axis[0]).size().reset_index(name="count")
                print("Grouped DataFrame with all rows:", grouped_df)
                
                # For valid rows (non-null y_axis values)
                filtered_df_valid = filtered_df[filtered_df[y_axis[0]].notnull()]
                grouped_df_valid = filtered_df_valid.groupby(x_axis[0])[y_axis[0]].count().reset_index(name="count")
                print("Grouped DataFrame with valid rows:", grouped_df_valid)
                
                chosen_grouped_df = grouped_df_valid
                categories = chosen_grouped_df[x_axis[0]].tolist()
                values = chosen_grouped_df["count"].tolist()
                category_value_pairs = list(zip(categories, values))
                optimized_categories = []
                optimized_values = []

                if optimizeData == "top10":
                            # Sort by values in descending order and take top 10
                    sorted_pairs = sorted(category_value_pairs, key=lambda x: x[1], reverse=True)
                    optimized_pairs = sorted_pairs[:10]
                            
                elif optimizeData == "bottom10":
                            # Sort by values in ascending order and take bottom 10
                    sorted_pairs = sorted(category_value_pairs, key=lambda x: x[1])
                    optimized_pairs = sorted_pairs[:10]
                            
                elif optimizeData == "both10":
                            # Get bottom 5
                    sorted_asc = sorted(category_value_pairs, key=lambda x: x[1])
                    bottom5_pairs = sorted_asc[:5]
                            
                            # Get top 5
                    sorted_desc = sorted(category_value_pairs, key=lambda x: x[1], reverse=True)
                    top5_pairs = sorted_desc[:5]
                            
                            # Combine bottom 5 and top 5
                    optimized_pairs = bottom5_pairs + top5_pairs
                            
                else:
                            # Default: return all filtered data
                    optimized_pairs = category_value_pairs

                        # Separate back into categories and values
                optimized_categories = [pair[0] for pair in optimized_pairs]
                optimized_values = [pair[1] for pair in optimized_pairs]
                
                print("Final categories====================", categories)
                print("Final values====================", values)
                
                connection.close()
                return jsonify({
                    "message": "Chart details received successfully!",
                    "categories": optimized_categories,
                    "values": optimized_values,
                    "chart_type": chart_type,
                    "chart_heading": chart_heading,
                    "x_axis": x_axis,
                    "y_axis": y_axis,
                    "optimizeData": optimizeData, 
                }), 200


            else:
                # if len(y_axis) == 2:
                if chart_type == "duealChart":
                    print("Dual y-axis chart detected") 
                    if 'OptimizationData' in locals() or 'OptimizationData' in globals():
                        df = pd.DataFrame(data, columns=[x_axis[0], 'series1', 'series2'])
                            
                        if optimizeData == 'top10':
                            df = df.sort_values(by='series1', ascending=False).head(10)
                        elif optimizeData == 'bottom10':
                            df = df.sort_values(by='series1', ascending=True).head(10)
                        elif optimizeData == 'both5':
                            top_df = df.sort_values(by='series1', ascending=False).head(5)
                            bottom_df = df.sort_values(by='series1', ascending=True).head(5)
                            df = pd.concat([top_df, bottom_df])

                    df, x_axis, y_axis = apply_calculation_to_df(df, calculation_data, x_axis, y_axis)
                    for axis in y_axis:
                        try:
                            df[axis] = pd.to_datetime(df[axis], errors='raise', format='%H:%M:%S')
                            df[axis] = df[axis].apply(lambda x: x.hour * 60 + x.minute)
                        except (ValueError, TypeError):
                            df[axis] = pd.to_numeric(df[axis], errors='coerce')

                    grouped_df = df.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
                    print("Grouped DataFrame (dual y-axis): ", grouped_df.head())

                    # categories = grouped_df[x_axis[0]].tolist()
                    # categories = [category.strftime('%Y-%m-%d') for category in grouped_df[x_axis[0]]]
                    categories = grouped_df[x_axis[0]].tolist()

                    # Check if the elements are datetime objects before formatting
                    if isinstance(categories[0], pd.Timestamp):  # Assumes at least one value is present
                        categories = [category.strftime('%Y-%m-%d') for category in categories]
                    else:
                        categories = [str(category) for category in categories]  


                    
                    values1 = [float(value) for value in grouped_df[y_axis[0]]]  # Convert Decimal to float
                    values2 = [float(value) for value in grouped_df[y_axis[1]]]  # Convert Decimal to float
                    print("duel axis categories====================", categories)

                    # Filter categories and values based on filter_options
                    filtered_categories = []
                    filtered_values1 = []
                    filtered_values2 = []

                    # Extract the filter values dynamically (e.g., filter_options['region'])
                    filter_values = list(filter_options.values())[0]  # Extract the first filter list dynamically

                    for category, value1, value2 in zip(categories, values1, values2):
                        if category in filter_values:  # Dynamically check category
                            filtered_categories.append(category)
                            filtered_values1.append(value1)
                            filtered_values2.append(value2)

                    print("filtered_categories====================", filtered_categories)
                    print("filtered_values1====================", filtered_values1)
                    print("filtered_values2====================", filtered_values2)


                    connection.close()

                    # Return the filtered data for both series
                    return jsonify({
                        "message": "Chart details received successfully!",
                        "categories": filtered_categories,
                        "series1": filtered_values1,
                        "series2": filtered_values2,
                        "chart_type": chart_type,
                        "chart_heading": chart_heading,
                        "x_axis": x_axis,
                         "optimizeData": optimizeData, 
                    }), 200
                if chart_type == "Butterfly":
                    print("Dual y-axis chart detected") 
                    
                    df, x_axis, y_axis = apply_calculation_to_df(df, calculation_data, x_axis, y_axis)

                    grouped_df = df.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
                    print("Grouped DataFrame (dual y-axis): ", grouped_df.head())

                    # categories = grouped_df[x_axis[0]].tolist()
                    # categories = [category.strftime('%Y-%m-%d') for category in grouped_df[x_axis[0]]]
                    categories = grouped_df[x_axis[0]].tolist()

                    # Check if the elements are datetime objects before formatting
                    if isinstance(categories[0], pd.Timestamp):  # Assumes at least one value is present
                        categories = [category.strftime('%Y-%m-%d') for category in categories]
                    else:
                        categories = [str(category) for category in categories]  


                    
                    values1 = [float(value) for value in grouped_df[y_axis[0]]]  # Convert Decimal to float
                    values2 = [float(value) for value in grouped_df[y_axis[1]]]  # Convert Decimal to float
                    print("duel axis categories====================", categories)

                    # Filter categories and values based on filter_options
                    filtered_categories = []
                    filtered_values1 = []
                    filtered_values2 = []

                    # Extract the filter values dynamically (e.g., filter_options['region'])
                    filter_values = list(filter_options.values())[0]  # Extract the first filter list dynamically

                    for category, value1, value2 in zip(categories, values1, values2):
                        if category in filter_values:  # Dynamically check category
                            filtered_categories.append(category)
                            filtered_values1.append(value1)
                            filtered_values2.append(value2)

                    print("filtered_categories====================", filtered_categories)
                    print("filtered_values1====================", filtered_values1)
                    print("filtered_values2====================", filtered_values2)


                    connection.close()

                    # Return the filtered data for both series
                    return jsonify({
                        "message": "Chart details received successfully!",
                        "categories": filtered_categories,
                        "series1": filtered_values1,
                        "series2": filtered_values2,
                        "chart_type": chart_type,
                        "chart_heading": chart_heading,
                        "x_axis": x_axis,
                        "optimizeData": optimizeData, 
                        "optimizeData": optimizeData, 
                    }), 200
                else:
                    
                  

                    df, x_axis, y_axis = apply_calculation_to_df(df, calculation_data, x_axis, y_axis)
                    grouped_df = df.groupby(x_axis[0])[y_axis].agg(aggregate_py).reset_index()

                    print("Grouped DataFrame: ", grouped_df.head())

                    categories = grouped_df[x_axis[0]].tolist()
                    if isinstance(categories[0], pd.Timestamp):  # Assumes at least one value is present
                        categories = [category.strftime('%Y-%m-%d') for category in categories]
                    else:
                        categories = [str(category) for category in categories]  

                    values = [float(value) for value in grouped_df[y_axis[0]]]  # Convert Decimal to float

                    print("categories====================22222", categories) 
                    print("values====================22222", values)
                    # allowed_categories = filter_options.get(x_axis[0], [])  # Get the list of valid categories
                    allowed_categories = list(map(str, filter_options.get(x_axis[0], [])))

                    filtered_categories = []
                    filtered_values = []

                    # for category, value in zip(categories, values):
                    #     # if category.strip() in allowed_categories:  # Ensure category matches the filter
                    #     if str(category).strip() in allowed_categories:

                    #         filtered_categories.append(category)
                    #         filtered_values.append(value)
                    #     else:
                    #         print(f"Category '{category}' not in filter_options[{x_axis[0]}]")
                    for category, value in zip(categories, values):
                        print("Type of category:", type(category), "Value:", category)
                        if str(category).strip() in allowed_categories:
                            filtered_categories.append(category)
                            filtered_values.append(value)
                        else:
                            print(f"Category '{category}' not in filter_options[{x_axis[0]}]")


                    print("filtered_categories====================1111", filtered_categories)
                    print("filtered_values====================", filtered_values)

                    # Create a combined list of (category, value) pairs
                    category_value_pairs = list(zip(filtered_categories, filtered_values))

                    # Sort based on optimization strategy
                    optimized_categories = []
                    optimized_values = []

                    if optimizeData == "top10":
                        # Sort by values in descending order and take top 10
                        sorted_pairs = sorted(category_value_pairs, key=lambda x: x[1], reverse=True)
                        optimized_pairs = sorted_pairs[:10]
                        
                    elif optimizeData == "bottom10":
                        # Sort by values in ascending order and take bottom 10
                        sorted_pairs = sorted(category_value_pairs, key=lambda x: x[1])
                        optimized_pairs = sorted_pairs[:10]
                        
                    elif optimizeData == "both10":
                        # Get bottom 5
                        sorted_asc = sorted(category_value_pairs, key=lambda x: x[1])
                        bottom5_pairs = sorted_asc[:5]
                        
                        # Get top 5
                        sorted_desc = sorted(category_value_pairs, key=lambda x: x[1], reverse=True)
                        top5_pairs = sorted_desc[:5]
                        
                        # Combine bottom 5 and top 5
                        optimized_pairs = bottom5_pairs + top5_pairs
                        
                    else:
                        # Default: return all filtered data
                        optimized_pairs = category_value_pairs

                    # Separate back into categories and values
                    optimized_categories = [pair[0] for pair in optimized_pairs]
                    optimized_values = [pair[1] for pair in optimized_pairs]

                    print(f"{optimizeData} optimized_categories====================", optimized_categories)
                    print(f"{optimizeData} optimized_values====================", optimized_values)

                    connection.close()

                    # Return the optimized data
                    return jsonify({
                        "message": "Chart details received successfully!",
                        "categories": optimized_categories,
                        "values": optimized_values,
                        "chart_type": chart_type,
                        "chart_heading": chart_heading,
                        "tableName": tableName,
                        "x_axis": x_axis,
                        "optimizeData": optimizeData, 
                    }), 200
        else:
            print("Tree hierarchy chart detected")
            print("tableName====================", tableName)
            print("x_axis====================", x_axis)
            print("filter_options====================", filter_options)
            print("y_axis====================", y_axis)
            print("aggregate====================", aggregate)
            print("databaseName====================", databaseName)
            print("selectedUser====================", selectedUser)
            
            data = fetch_data_tree(tableName, x_axis, filter_options, y_axis, aggregate, databaseName,selectedUser,calculation_data)
            categories = data.get("categories", [])
            values = data.get("values", [])
            category_value_pairs = list(zip(categories, values))
            optimized_categories = []
            optimized_values = []

            if optimizeData == "top10":
                        # Sort by values in descending order and take top 10
                sorted_pairs = sorted(category_value_pairs, key=lambda x: x[1], reverse=True)
                optimized_pairs = sorted_pairs[:10]
                        
            elif optimizeData == "bottom10":
                        # Sort by values in ascending order and take bottom 10
                sorted_pairs = sorted(category_value_pairs, key=lambda x: x[1])
                optimized_pairs = sorted_pairs[:10]
                        
            elif optimizeData == "both10":
                        # Get bottom 5
                sorted_asc = sorted(category_value_pairs, key=lambda x: x[1])
                bottom5_pairs = sorted_asc[:5]
                        
                        # Get top 5
                sorted_desc = sorted(category_value_pairs, key=lambda x: x[1], reverse=True)
                top5_pairs = sorted_desc[:5]
                        
                        # Combine bottom 5 and top 5
                optimized_pairs = bottom5_pairs + top5_pairs
                        
            else:
                        # Default: return all filtered data
                optimized_pairs = category_value_pairs

                    # Separate back into categories and values
            optimized_categories = [pair[0] for pair in optimized_pairs]
            optimized_values = [pair[1] for pair in optimized_pairs]
            print("categories",categories)
            print("values",values)
            # Return the filtered data
            return jsonify({
                "message": "Chart details received successfully!",
                "categories": optimized_categories,
                "values": optimized_values,
                "chart_type": chart_type,
                "chart_heading": chart_heading,
                "x_axis": x_axis,
                 "optimizeData": optimizeData, 
            }), 200

    except Exception as e:
        print("Error: ", e)
        return jsonify({"message": "Error processing request", "error": str(e)}), 500

def get_dashboard_data(dashboard_name, company_name,user_id):
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            query = """
                SELECT *
                FROM table_dashboard
                WHERE file_name = %s AND company_name = %s AND user_id = %s
            """
            print("query",query)
            print("user_id",user_id)
            cursor.execute(query, (dashboard_name, company_name,user_id))
            data = cursor.fetchone()
            
            if data is None:
                print(f"No data found for Chart: {dashboard_name} and Company: {company_name}")
                return None

            cursor.close()
            conn.close()
            return data
        except psycopg2.Error as e:
            print(f"Error fetching data for Chart {dashboard_name} and Company {company_name}: {e}")
            conn.close()
            return None
    else:
        return None

@app.route('/Dashboard_data/<dashboard_name>/<company_name>', methods=['GET'])
def dashboard_data(dashboard_name,company_name):
    user_id, dashboard_name = dashboard_name.split(",", 1)  # Split only once
    # user_id = request.args.get('user_id')
    data = get_dashboard_data(dashboard_name,company_name,user_id)
    # print("chart datas------------------------------------------------------------------------------------------------------------------",data) 
    if data is not None:
        chart_ids = data[4]
        positions=data[5]
        filter_options=data[11]
        areacolour=data[15]
        droppableBgColor=data[16]
        opacity=data[17]
        image_ids=data[18]
        chart_type=data[7]
        fontStyleLocal =data[20]
        fontColor =data[22]
        fontSize =data[21]
        wallpaper_id=data[23]
        
        print("chart_ids====================",chart_ids)    
        print("chart_areacolour====================",areacolour)   
        print("image_ids",image_ids)
        chart_datas=get_dashboard_view_chart_data(chart_ids,positions,filter_options,areacolour,droppableBgColor,opacity,image_ids,chart_type)
        # print("dashboarddata",data)
        # print("chart_datas====================",chart_datas)
        image_data_list = []
        conn = create_connection() 
        if image_ids:
            # if isinstance(image_ids, str):
            #     image_ids = list(map(str.strip, image_ids.strip('{}').split(',')))
            # if isinstance(image_ids, str):
            #     image_ids = list(filter(None, map(str.strip, image_ids.strip('{}').split(','))))
            if isinstance(image_ids, str):
                try:
                    image_ids = json.loads(image_ids)
                    print("Loaded JSON image_ids:", image_ids)
                except json.JSONDecodeError:
                    # fallback: manually parse
                    image_ids = list(filter(None, map(str.strip, image_ids.strip('{}').split(','))))

            cursor = conn.cursor()
            print("Parsed image_ids:", image_ids)
            

            for img_id in image_ids:
                print("Looking for img_id:", img_id)

                cursor.execute("""
                        SELECT image_id, src, x, y, width, height, zIndex, disableDragging 
                        FROM image_positions 
                        WHERE image_id = %s

                    """, (img_id,))
                img_data = cursor.fetchone()

                
                
                if img_data:
                    print(" img_data[0]", img_data[0])
                    image_data_list.append({
                            'image_id': img_data[0],
                            'src': img_data[1],
                            'x': img_data[2],
                            'y': img_data[3],
                            'width': img_data[4],
                            'height': img_data[5],
                            'zIndex': img_data[6],
                            'disableDragging': img_data[7]
                        })
            cursor.close()
            wallpaper_src = None 
            if wallpaper_id:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT src FROM dashboard_wallpapers WHERE wallpaper_id = %s
                """, (wallpaper_id,))
                result = cursor.fetchone()
                if result:
                    wallpaper_src = result[0]
                cursor.close()
            conn.close()
            print("fontStyleLocal",fontStyleLocal)
            print("fontColor",fontColor)
            print("fontSize",fontSize)
            print("Chart Data=======>",chart_datas)
        # return jsonify(data,chart_datas)
        return jsonify({
            "data": data,
            "chart_datas": chart_datas,
            "positions": positions,
            "image_data_list":image_data_list,
            "fontStyleLocal":fontStyleLocal,
            "fontColor":fontColor,
            "fontSize":fontSize,
            "wallpaper_src": wallpaper_src 
        })
    else:
        return jsonify({'error': 'Failed to fetch data for Chart {}'.format(dashboard_name)})
    
    

@app.route('/saved_dashboard_total_rows', methods=['GET'])
def saved_dashboard_names():
    database_name = request.args.get('company')  # Getting the database_name
    project=request.args.get("project_name")
    print(f"Received user_id:",database_name)
    user_id = request.args.get('user_id')  # Retrieve user_id from query parameters

    print(f"Received user_id: {user_id}")  # Debugging output
    if not user_id:
        return jsonify({'error': 'User ID is required'}), 400

    names = get_dashboard_names(user_id,database_name,project)
    
    print("names====================", names)   
    if names is not None:
        return jsonify({'chart_names': names})
    else:
        return jsonify({'error': 'Failed to fetch chart names'})

@app.route('/project_names', methods=['GET'])
@employee_required
def get_project_names_route():
    database_name = request.args.get('company')
    user_id = request.args.get('user_id')

    if not user_id or not database_name:
        return jsonify({'error': 'User ID and company are required'}), 400

    project_names = fetch_project_names(user_id, database_name)

    if project_names is not None:
        return jsonify({'project_names': project_names})
    else:
        return jsonify({'error': 'Failed to fetch project names'}), 500
    
@app.route('/saved_Editdashboard_total_rows', methods=['GET'])
def saved_Editdashboard_names():
    database_name = request.args.get('company')  # Getting the database_name
    print(f"Received user_id:",database_name)
    user_id = request.args.get('user_id')  # Retrieve user_id from query parameters

    print(f"Received user_id: {user_id}")  # Debugging output
    if not user_id:
        return jsonify({'error': 'User ID is required'}), 400

    names = get_Edit_dashboard_names(user_id,database_name)
    
    print("names====================", names)   
    if names is not None:
        return jsonify({'chart_names': names})
    else:
        return jsonify({'error': 'Failed to fetch chart names'})
    
@app.route('/api/usersignup', methods=['POST'])
def usersignup():
    data = request.json
    print("Received Data:", data)
    company=data.get("company")
    company = company.lower()  # Convert company name to lowercase
    print("company:", company)
    register_type = data.get("registerType")
    print("Register Type:", register_type)

    user_details = data.get("userDetails")
    print("User Details:", user_details)

    if register_type == "manual":
        return handle_manual_registration(user_details,company)
    elif register_type == "File_Upload":
        return handle_file_upload_registration(user_details,company)

    return jsonify({'message': 'Invalid registration type'}), 400

def get_db_connection(dbname="datasource"):
    conn = psycopg2.connect(
        dbname=dbname,
        user="postgres",
        password=password,
        host=HOST,
        port="5432"
    )
    return conn

@app.route('/api/companies', methods=['GET'])
def get_companies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT id,organizationName FROM organizationdatatest')
    companies = cursor.fetchall()
    cursor.close()
    conn.close()
    company_list = [{'id': company[0], 'name': company[1]} for company in companies]
    print(company_list)
    return jsonify(company_list)

@app.route('/api/roles', methods=['GET'])
def get_roles(): 
    company_name = request.args.get('companyName')
    if not company_name:
        return jsonify({'message': 'Missing companyName'}), 400
    try:
        conn = get_company_db_connection(company_name)
        cur = conn.cursor()
        cur.execute('SELECT role_id ,role_name FROM role')
        roles = cur.fetchall()
        cur.close()
        conn.close()
        role_list = [{'id': role[0], 'name': role[1]} for role in roles]
        print(role_list)
        return jsonify(role_list)
    except Exception as e:
        print(f"Error fetching roles: {e}")
        return jsonify({'message': 'Failed to fetch roles'}), 500

@app.route('/fetchglobeldataframe', methods=['GET'])
def get_hello_data():
    dataframe = bc.global_df
    print("dataframe........................", dataframe)
    for col in dataframe.select_dtypes(include=['datetime64[ns]']).columns:
        dataframe[col] = dataframe[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('null')
    
    dataframe_dict = dataframe.to_dict(orient='records')
    return jsonify({"data frame": dataframe_dict})

@app.route('/aichartdata', methods=['GET'])
def ai_barchart():
    dataframe = bc.global_df  # Assuming bc.global_df is your DataFrame
    histogram_details = generate_histogram_details(dataframe)
    return jsonify({"histogram_details": histogram_details})

@app.route('/boxplotchartdata', methods=['GET'])
def ai_boxPlotChart():
    dataframe = bc.global_df  # Assuming bc.global_df is your DataFrame
    # axes = dataframe.hist()
    aaa = dataframe.plot(kind='box', subplots=True, layout=(4, 2), sharex=False, sharey=False)
    details = []

    for ax in aaa:
        ax_details = {
            "Title": ax.get_title(),
            "X-axis label": ax.get_xlabel(),
            "Y-axis label": ax.get_ylabel(),
            "X-axis limits": ax.get_xlim(),
            "Y-axis limits": ax.get_ylim(),
            "Number of elements (patches)": len(ax.patches)
        }
        details.append(ax_details)

        # Now 'details' contains all the axis details in an array of dictionaries
        print(details)
        # # Print non-empty histogram details
        # for detail in filtered_histogram_details:
        #     print(detail)

    # Return JSON response
    return jsonify({
        # "data_frame": dataframe_dict,
        "histogram_details": details
    })

@app.route('/api/fetch_categories', methods=['GET'])
def fetch_categories():
    company_name = request.args.get('companyName')
    try:
        conn = get_company_db_connection(company_name)
        cursor = conn.cursor()
        cursor.execute("SELECT category_id, category_name FROM category")
        categories = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify([{'id': row[0], 'name': row[1]} for row in categories])
    except Exception as e:
        print(f"Error fetching categories: {e}")
        return jsonify({'message': 'Error fetching categories'}),500

@app.route('/api/users', methods=['GET'])
def get_all_users():
    company_name = request.args.get('companyName')
    print("company_name====================",company_name)
    page = int(request.args.get('page', 1))  # Default to page 1 if not provided
    limit = int(request.args.get('limit', 10))  # Default to 10 users per page

    if not company_name:
        return jsonify({'error': 'Company name is required'}), 400

    try:
        conn = get_company_db_connection(company_name)
        cursor = conn.cursor()

        # Calculate the offset based on the current page and limit
        offset = (page - 1) * limit

        # Fetch users for the specified company with pagination
        cursor.execute("""
            SELECT employee_name, username, role_id, category,reporting_id 
            FROM employee_list
            LIMIT %s OFFSET %s;
        """, (limit, offset))

        users = cursor.fetchall()

        # Fetch the total number of users for the specified company (for pagination)
        cursor.execute("SELECT COUNT(*) FROM employee_list;")
        total_users = cursor.fetchone()[0]

        # Create a list of user dictionaries
        user_list = [
            {
                'employee_name': user[0],
                'username': user[1],
                'role_id': user[2],
                'category': user[3],
                'reporting_id':user[4]
            }
            for user in users
        ]

        return jsonify({
            'users': user_list,
            'total': total_users  # Total users count for pagination
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/fetch_user_data', methods=['POST'])
def fetch_user_data():
    data = request.json
    print("data",data)
    username = data.get('username')
    organization_name = data.get('organization_name')  # Changed from company_id to organization_name
    print("username",username)
    print("organization_name",organization_name)


    # Check if username and organization_name are provided
    if not username or not organization_name:
        return jsonify({'message': 'Username and organization_name are required'}), 400

    # Connect to the company's database using organization_name
    conn = get_company_db_connection(organization_name)
    if not conn:
        print(f"Failed to connect to company database for {organization_name}.")
        return jsonify({'message': 'Failed to connect to company database'}), 500

    try:
        # Fetch employee details based on username
        cursor = conn.cursor()
        cursor.execute("""
            SELECT employee_id, employee_name, role_id,category, username,email FROM employee_list WHERE username = %s
        """, (username,))
        employee_data = cursor.fetchone()
        cursor.close()

        if not employee_data:
            return jsonify({'message': 'Employee not found'}), 404

        # Extract employee data
        employee_id, employee_name, role_id, category_name,username,email = employee_data
        print("employee data",employee_data)
        # Fetch the category name from the signup database
        conn_datasource = get_db_connection("datasource")
        user_details = {
            'employee_id': employee_id,
            'employee_name': employee_name,
            'role_id': role_id,
            'username': username,
            'category_name': category_name,
            'email':email
        }
        print("userdetails",user_details)
        # Check if new role or category needs to be updated
        new_role_id = data.get('new_role_id')
        new_category_name = data.get('new_category_name')

        if new_role_id or new_category_name:
            # Update role if provided
            if new_role_id:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE employee_list SET role_id = %s WHERE employee_id = %s
                """, (new_role_id, employee_id))
                conn.commit()
                cursor.close()
                user_details['role_id'] = new_role_id  # Update in response

            # Update category if provided
            if new_category_name:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE category SET category_name = %s WHERE company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
                """, (new_category_name, organization_name))
                conn.commit()
                cursor.close()
                user_details['category_name'] = new_category_name  # Update in response

        # Return the employee details along with updated role/category if applicable
        return jsonify(user_details), 200

    except Exception as e:
        print(f"Error fetching or updating user data: {e}")
        return jsonify({'message': 'Error fetching or updating user data'}), 500

    finally:
        conn.close()

@app.route('/api/update_user_details', methods=['POST'])
def update_user_details():
    data = request.json
    print("data......",data)
    username = data.get('username')
    organization_name = data.get('companyName')
    new_role_id = data.get('roleId')
    reporting_id=data.get('reporting_id')
    print("new role id",new_role_id)
    category_name = data.get('categoryName')
    formatted_category_name = category_name  # Format category with curly braces
    print("category---------", formatted_category_name)

    # Connect to company database
    conn = connect_db(organization_name)
    if not conn:
        return jsonify({'message': f'Failed to connect to database for company: {organization_name}'}), 500

    try:
        cursor = conn.cursor()

        # Update role
        if new_role_id:
            cursor.execute("""
                UPDATE employee_list SET role_id = %s ,category=%s,reporting_id=%s WHERE username = %s
                
            """, (new_role_id,formatted_category_name,reporting_id,username))
            conn.commit()

        return jsonify({'message': 'User details updated successfully'}), 200
    except Exception as e:
        print(f"Error updating user details: {e}")
        return jsonify({'message': 'Error updating user details'}), 500
    finally:
        conn.close()

@app.route('/api/user/<username>', methods=['PUT'])
def update_user(username):
    data = request.get_json()
    company_name = data.get('company_name')
    role_id = data.get('role_id')
    category_name = data.get('category')

    if not company_name or not role_id or not category_name:
        return jsonify({'error': 'Company name, role, and category are required'}), 400

    try:
        conn = get_company_db_connection(company_name)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE employee_list 
            SET role_id = %s, category = %s 
            WHERE username = %s;
        """, (role_id, category_name, username))
        conn.commit()

        return jsonify({'message': 'User details updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

from predictions import load_and_predict  
@app.route('/api/predictions', methods=['GET','POST'])
def get_predictions():
    data = request.json
    x_axis = data.get('xAxis')
    y_axis = data.get('yAxis')    
    timePeriod = data.get('timePeriod')
    number_of_periods = data.get('number')   
    selectedTableName = data.get('selectedTable')      
    print("selectedTableName:", selectedTableName) 
    print("xAxis:", x_axis, "yAxis:", y_axis)
    print("timePeriod:", timePeriod, "number_of_periods:", number_of_periods)
    prediction_data = load_and_predict(x_axis, y_axis,number_of_periods,timePeriod)
    # prediction_data = load_and_predict(x_axis, y_axis)
    return jsonify(prediction_data)  # Return data as JSON

@app.route('/Hierarchial-backend-endpoint', methods=['POST', 'GET'])
def handle_hierarchical_bar_click():
    global global_df

    if request.method == 'POST':
        data = request.json
        print("Received request data:", data)

        clicked_category = data.get('category')
        x_axis_columns = data.get('xAxis')
        y_axis_column = data.get('yAxis')
        table_name = data.get('tableName')
        db_name = data.get('databaseName')
        current_depth = data.get('currentLevel', 0)
        selectedUser=data.get("selectedUser")
        print("Clicked Category:", clicked_category)
        print("X-axis Columns:", x_axis_columns)
        print("Y-axis Column:", y_axis_column)
        print("Table Name:", table_name)

        try:
            if global_df is None:
                global_df = fetch_hierarchical_data(table_name, db_name,selectedUser)
                print("Fetched data:", global_df.head() if global_df is not None else "No data returned")

            if global_df is None or global_df.empty:
                return jsonify({"error": "Data could not be loaded into global_df."}), 500

            if y_axis_column[0] not in global_df.columns:
                return jsonify({"error": f"Column {y_axis_column[0]} not found in global_df."}), 500
            
            global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
            drill_down_result = Hierarchial_drill_down(
                clicked_category=clicked_category, 
                x_axis_columns=x_axis_columns, 
                y_axis_column=y_axis_column, 
                depth=current_depth, 
                aggregation=data.get('aggregation')
            )
            print("Drill-down result:", drill_down_result)
            return jsonify(drill_down_result)

        except Exception as e:
            print("An error occurred in handle_hierarchical_bar_click:", str(e))
            return jsonify({"error": "An internal error occurred.", "message": str(e)}), 500

def normalize_text(text):
    """Normalize text by converting to lowercase and replacing underscores/hyphens with spaces."""
    # print(text)


    return text.lower().replace("_", " ").replace("-", " ")

def extract_chart_details_spacy(text, COLUMN_NAME_MAP):
    doc = nlp(text.lower())
    chart_type = None
    extracted_columns = []

    for token in doc:
        token_text = token.text.lower()
        if token_text in CHART_TYPES:
            chart_type = token_text

    for chunk in doc.noun_chunks:
        chunk_text = normalize_text(chunk.text)
        if chunk_text in COLUMN_NAME_MAP:
            extracted_columns.append(COLUMN_NAME_MAP[chunk_text])

    if extracted_columns:
        mid_index = len(extracted_columns) // 2
        columns = extracted_columns[:mid_index] if mid_index > 0 else [extracted_columns[0]]
        rows = extracted_columns[mid_index:] if mid_index > 0 else []

        return {
            "chart_type": chart_type,
            "columns": columns if columns else None,
            "rows": rows if rows else None
        }
    
    return {"chart_type": chart_type, "columns": None, "rows": None}

@app.route('/nlp_upload_audio', methods=['POST'])
def nlp_upload_audio():
    global global_column_names
    audio = request.files.get('audio')
    table_name = request.form.get('tableName')
    database_name = request.form.get('databaseName')

    

    # Fetch global column names
    # COLUMNS_NAMES = bc.global_column_names
    COLUMNS_NAMES = ['order_id', 'units_sold', 'unit_price', 'unit_cost', 'total_revenue', 'total_cost', 'total_profit','region', 'country', 'product', 'bank_name', 'order_priority', 'order_date', 'ship_date']
    print("Columns names:", COLUMNS_NAMES)

    if not COLUMNS_NAMES:
        return jsonify({"error": "Global column names are not available"}), 400

    # Build COLUMN_NAME_MAP dynamically
    COLUMN_NAME_MAP = {normalize_text(col): col for col in COLUMNS_NAMES}

    if not audio:
        return jsonify({"error": "No audio file uploaded"}), 400

    # Save audio file
    file_path = os.path.join(NLP_UPLOAD_FOLDER, audio.filename)
    try:
        audio.save(file_path)
    except Exception as e:
        print(f"Failed to save audio file: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"Failed to save audio file: {str(e)}"}), 500

    print(f"Audio file saved to: {file_path}")
    print(f"Table name: {table_name}")
    print(f"Database name: {database_name}")

    # Check if the audio file is valid (not empty)
    try:
        file_size = os.path.getsize(file_path)
        print(f"Audio file size: {file_size} bytes")
        if file_size == 0:
            return jsonify({"error": "Uploaded audio file is empty"}), 400
    except Exception as e:
        print(f"Failed to check audio file size: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"Failed to check audio file size: {str(e)}"}), 500

    # Load Whisper model and transcribe
    try:
        print("Loading Whisper model...")
        model = whisper.load_model("base")
        print("Model loaded successfully.")
        result = model.transcribe(file_path)
        transcribed_text = result['text']
        print(f"Transcribed Text: {transcribed_text}")

        # Extract chart details
        print("Extracting data using Spacy...")
        extracted_data = extract_chart_details_spacy(transcribed_text, COLUMN_NAME_MAP)
        print("Extracted Data:", extracted_data)

    except Exception as e:
        print(f"Error occurred during transcription or extraction: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500

    return jsonify({
        "transcription": transcribed_text,
        "extracted_data": extracted_data
    })

@app.route('/api/charts/<string:chart_name>', methods=['DELETE'])
def delete_chart(chart_name):
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        user_id, chart_name = chart_name.split(",", 1)  # Split only once
        # Delete the chart from the table
        cur.execute("DELETE FROM table_chart_save WHERE chart_name = %s", (chart_name,))
        rows_deleted = cur.rowcount
        
        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()

        # If no rows were deleted, return a 404 error
        if rows_deleted == 0:
            return jsonify({"message": f"No chart found with the name '{chart_name}'"}), 404

        return jsonify({"message": f"Chart '{chart_name}' deleted successfully"}), 200
    
    except Exception as e:
        print("Error while deleting chart:", e)
        return jsonify({"error": "Failed to delete chart"}), 500

@app.route('/delete-chart', methods=['DELETE'])
def delete_dashboard_name():
    chart_name = request.args.get('chart_name')  # Get the chart_name from JSON body
    user_id = request.args.get('user_id')  # Use query param for GET

    company_name=request.args.get('company_name')
    print("company_name",company_name)
    print("chart_name",chart_name)
    print("user_id",user_id)
    if not chart_name:
        return jsonify({"error": "Chart name is required"}), 400
    
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("DELETE FROM table_dashboard WHERE file_name = %s AND user_id = %s AND company_name=%s", (chart_name,user_id,company_name))
        rows_deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()

        if rows_deleted == 0:
            return jsonify({"message": f"No chart found with the name '{chart_name}'"}), 404

        return jsonify({"message": f"Chart '{chart_name}' deleted successfully"}), 200
    
    except Exception as e:
        print("Error while deleting chart:", e)
        return jsonify({"error": f"Error: {str(e)}"}), 500

@app.route('/api/are-charts-in-dashboard', methods=['POST'])
@employee_required
def are_charts_in_dashboard():
    data = request.get_json()
    chart_names_row = data.get('chart_names', [])
    chart_names=[name if  isinstance(name,str) else name[1] for name in chart_names_row]
    company_name = data.get('company_name')

    if not chart_names or not company_name:
        return jsonify({"error": "chart_names and company_name are required"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to database"}), 500

    try:
        cur = conn.cursor()

        # Step 1: Get all chart_ids that match the provided names and company
        cur.execute("""
            SELECT id, chart_name FROM table_chart_save 
            WHERE chart_name = ANY(%s) AND company_name = %s
        """, (chart_names, company_name))
        
        chart_id_map = {str(row[0]): row[1] for row in cur.fetchall()} # {chart_id: chart_name} for reverse lookup
        found_chart_ids = list(chart_id_map.keys())

        # If no charts were found, none can be in a dashboard
        if not found_chart_ids:
            cur.close()
            conn.close()
            return jsonify({name: False for name in chart_names}), 200

        # Step 2: Query dashboards to see which chart_ids are present
        # Use a more efficient query to check for existence
        cur.execute("""
            SELECT unnest(string_to_array(chart_ids, ',')) 
            FROM table_dashboard
            WHERE string_to_array(chart_ids, ',') && %s
        """, (found_chart_ids,))
        
        in_use_chart_ids = {row[0] for row in cur.fetchall()}

        cur.close()
        conn.close()

        # Step 3: Build the final result dictionary
        result = {}
        for chart_name in chart_names:
            # Check if the chart has a corresponding ID and if that ID is in the in_use_chart_ids set
            chart_id_for_name = next((id for id, name in chart_id_map.items() if name == chart_name), None)
            is_in_use = chart_id_for_name is not None and chart_id_for_name in in_use_chart_ids
            result[chart_name] = is_in_use

        return jsonify(result), 200

    except Exception as e:
        print("Error checking chart usage in bulk:", e)
        return jsonify({"error": "Server error"}), 500
    
@app.route('/api/table-columns/<table_name>', methods=['GET'])
def api_get_table_columns(table_name):
    try:
        # Get the company name from the query parameters
        company_name = request.args.get('companyName')
        
        # Ensure the company_name is provided
        if not company_name:
            return jsonify({"error": "Company name is required"}), 400
        
        columns = get_table_columns(table_name, company_name)
        print("columns", columns)
        return jsonify(columns), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/employees', methods=['GET'])
def get_employees():
    company = request.args.get('company')  # Get company name from query parameter
    print("company", company)
    if not company:
        return jsonify({"error": "Company parameter is missing"}), 400
    company = company.lower()  # Convert company name to lowercase
    print("company:", company)
    try:
        conn = get_company_db_connection(company)
        cur = conn.cursor()

        # Check if table exists first
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'employee_list'
            );
        """)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            print("employee_list table does not exist for this company.")
            return jsonify([])  # Return empty list if table doesn't exist

        cur.execute('SELECT employee_id, employee_name FROM employee_list')
        employees = cur.fetchall()

        cur.close()
        conn.close()

        employee_list = [{'employee_id': emp[0], 'employee_name': emp[1]} for emp in employees]
        print(employee_list)

        return jsonify(employee_list)

    except Exception as e:
        print("Error fetching employees:", e)
        return jsonify({"error": "An error occurred while fetching employees"}), 500
    
@app.route('/api/checkTableUsage', methods=['GET'])
def check_table_usage():
    table_name = request.args.get('tableName')

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    # Remove any surrounding quotes from the table name
    table_name = table_name.strip('"').strip("'")

    # Debugging: Print the received table name
    print(f"Received table name: {table_name}")

    # Check if the table is used for chart creation
    is_in_use = is_table_used_in_charts(table_name)
    print("is_in_use",is_in_use)
    return jsonify({"isInUse": is_in_use})

@app.route('/api/fetchTableDetailsexcel', methods=['GET'])
def get_table_data_excel():
    company_name = request.args.get('databaseName')  # Get company name from the query
    table_name = request.args.get('selectedTable')  # Get the table name from the query
    
    if not company_name or not table_name:
        return jsonify({'error': 'Database name and table name are required'}), 400

    try:
        # Connect to the database
        connection = get_company_db_connection(company_name)
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Query the selected table
        cursor.execute(f'SELECT * FROM {table_name};')
        rows = cursor.fetchall()

        # Close the connection
        cursor.close()
        connection.close()

        # Return the fetched data as JSON
        return jsonify(rows)

    except Exception as e:
        # Log the error for better debugging
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 500

from load import (
    get_table_data_with_cache,
    get_table_columns,
    get_table_columns_with_types,
    get_company_db_connection,
    clear_cache,
    get_cache_information,
    get_cached_dataframe,
    check_view_exists,
    create_database_view,
    get_user_views,get_table_columns_with_typesdb,
    drop_database_view,check_view_existsdb,create_database_viewdb
)# app.py - Updated Flask routes with column selection and view management functionality

from flask import Flask, request, jsonify
from load import (
    get_table_data_with_cache,
    get_table_columns,
    get_company_db_connection,
    clear_cache,
    get_cache_information,
    get_cached_dataframe,
    check_view_exists,
    create_database_view,
    get_user_views,
    drop_database_view
)



@app.route('/api/fetchTableDetails', methods=['GET'])
def get_table_data():
    """API endpoint to fetch table data with caching, column selection, and column conditions"""
    try:
        company_name = request.args.get('databaseName')
        table_name = request.args.get('selectedTable')
        date_column = request.args.get('dateColumn')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        
        # Get selected columns from request (comma-separated string)
        selected_columns_str = request.args.get('selectedColumns')
        selected_columns = None
        if selected_columns_str:
            selected_columns = [col.strip() for col in selected_columns_str.split(',') if col.strip()]
        
        # Get column conditions from request (JSON string)
        import json
        column_conditions_str = request.args.get('columnConditions')
        column_conditions = None
        if column_conditions_str:
            try:
                column_conditions = json.loads(column_conditions_str)
            except json.JSONDecodeError:
                return jsonify({'error': 'Invalid column conditions format'}), 400
        
        # print(f"Received request: company={company_name}, table={table_name}, date_col={date_column}")
        # print(f"Selected columns: {selected_columns}")
        # print(f"Column conditions: {column_conditions}")
        
        if not company_name or not table_name:
            return jsonify({'error': 'Database name and table name are required'}), 400

        result = get_table_data_with_cache(
            company_name, table_name, date_column, start_date, end_date, selected_columns, column_conditions
        )
        return jsonify(result)
        
    except Exception as e:
        print(f"Error in get_table_data: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/fetchTableColumnsWithTypes', methods=['GET'])
def get_table_columns_with_types_api():
    """API endpoint to fetch all column names with their data types from a table"""
    try:
        company_name = request.args.get('databaseName')
        table_name = request.args.get('selectedTable')
        
        print(f"Fetching columns with types for: company={company_name}, table={table_name}")
        
        if not company_name or not table_name:
            return jsonify({'error': 'Database name and table name are required'}), 400

        columns = get_table_columns_with_types(company_name, table_name)
        return jsonify(columns)
        
    except Exception as e:
        print(f"Error fetching table columns with types: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/fetchTableColumnsWithTypesdb', methods=['GET'])
def get_table_columns_with_types_apidb():
    """API endpoint to fetch all column names with their data types from a table"""
    try:
        company_name = request.args.get('databaseName')
        table_name = request.args.get('selectedTable')
        selectedUser=request.args.get('selectedUser')
        print(f"Fetching columns with types for: company={company_name}, table={table_name}")
        
        if not company_name or not table_name:
            return jsonify({'error': 'Database name and table name are required'}), 400

        columns = get_table_columns_with_typesdb(company_name, table_name,selectedUser)
        return jsonify(columns)
        
    except Exception as e:
        print(f"Error fetching table columns with types: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/fetchTableColumns', methods=['GET'])
def get_table_columns_api():
    """API endpoint to fetch all column names from a table"""
    try:
        company_name = request.args.get('databaseName')
        table_name = request.args.get('selectedTable')
        
        print(f"Fetching columns for: company={company_name}, table={table_name}")
        
        if not company_name or not table_name:
            return jsonify({'error': 'Database name and table name are required'}), 400

        columns = get_table_columns(company_name, table_name)
        return jsonify(columns)
        
    except Exception as e:
        print(f"Error fetching table columns: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/fetchDateColumns', methods=['GET'])
def get_date_columns():
    """API endpoint to fetch date/datetime columns from a table"""
    company_name = request.args.get('databaseName')
    table_name = request.args.get('selectedTable')
    
    if not company_name or not table_name:
        return jsonify({'error': 'Database name and table name are required'}), 400

    try:
        # Connect to the database
        connection = get_company_db_connection(company_name)
        cursor = connection.cursor()

        # Query to get date/datetime/timestamp columns from the table
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND (data_type IN ('date', 'timestamp', 'datetime', 'timestamp without time zone', 'timestamp with time zone')
                 OR data_type LIKE '%%time%%'
                 OR data_type LIKE '%%date%%')
            ORDER BY column_name
        """, (table_name,))
        
        columns = [row[0] for row in cursor.fetchall()]

        # Close the connection
        cursor.close()
        connection.close()

        return jsonify(columns)

    except Exception as e:
        print(f"Error fetching date columns: {e}")
        return jsonify({'error': str(e)}), 500
    

def get_db_connectiondb(company_name, selected_user=None):
    """
    Establishes a PostgreSQL database connection.
    Connects to an external database if selected_user is provided,
    otherwise connects to the default local database.

    Args:
        db_name (str): The name of the database to connect to.
        selected_user (str, optional): The name of the selected external user/connection.
                                       Defaults to None for local connection.

    Returns:
        psycopg2.extensions.connection: A database connection object.

    Raises:
        Exception: If connection details are missing or connection fails.
    """
    connection = None
    if not company_name:
        raise ValueError("Database name is missing")

    if not selected_user or selected_user.lower() == 'null':
        print("Using default local database connection...")
        connection_string = f"dbname={company_name} user={USER_NAME} password={PASSWORD} host={HOST}"
        connection = psycopg2.connect(connection_string)
    else:  # External connection
        connection_details = fetch_external_db_connection(company_name, selected_user)
        if not connection_details:
            raise Exception(f"Unable to fetch external database connection details for user '{selected_user}'")

        db_details = {
            "host": connection_details[3],
            "database": connection_details[7],
            "user": connection_details[4],
            "password": connection_details[5],
            "port": int(connection_details[6])
        }
        
        print(f"Connecting to external database: {db_details['database']}@{db_details['host']}:{db_details['port']} as {db_details['user']}")
        connection = psycopg2.connect(
            dbname=db_details['database'],
            user=db_details['user'],
            password=db_details['password'],
            host=db_details['host'],
            port=db_details['port'],
        )
    return connection
@app.route('/api/fetchDateColumnsdb', methods=['GET'])
def get_date_columnsdb():
    """API endpoint to fetch date/datetime columns from a table"""
    company_name = request.args.get('databaseName')
    table_name = request.args.get('selectedTable')
    selectedUser=request.args.get('selectedUser')
    if not company_name or not table_name:
        return jsonify({'error': 'Database name and table name are required'}), 400

    try:
        # Connect to the database
        connection = get_db_connectiondb(company_name,selectedUser)
        cursor = connection.cursor()

        # Query to get date/datetime/timestamp columns from the table
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND (data_type IN ('date', 'timestamp', 'datetime', 'timestamp without time zone', 'timestamp with time zone')
                 OR data_type LIKE '%%time%%'
                 OR data_type LIKE '%%date%%')
            ORDER BY column_name
        """, (table_name,))
        
        columns = [row[0] for row in cursor.fetchall()]

        # Close the connection
        cursor.close()
        connection.close()

        return jsonify(columns)

    except Exception as e:
        print(f"Error fetching date columns: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/clearCache', methods=['POST'])
def clear_dataFrame_cache():
    """Clear all cached data"""
    try:
        clear_cache()
        return jsonify({'message': 'Cache cleared successfully'})
    except Exception as e:
        print(f"Error clearing cache: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/getCacheInfo', methods=['GET'])
def get_cache_info():
    """Get information about cached data"""
    try:
        cache_info = get_cache_information()
        return jsonify(cache_info)
    except Exception as e:
        print(f"Error getting cache info: {e}")
        return jsonify({'error': str(e)}), 500
@app.route('/api/checkViewExists', methods=['GET'])
def check_view_exists_api():
    """API endpoint to check if a view name already exists"""
    try:
        company_name = request.args.get('databaseName')
        view_name = request.args.get('viewName')
        
        if not company_name or not view_name:
            return jsonify({'error': 'Database name and view name are required'}), 400

        exists = check_view_exists(company_name, view_name)
        return jsonify({'exists': exists})
        
    except Exception as e:
        print(f"Error checking view exists: {str(e)}")
        return jsonify({'error': str(e)}), 500
@app.route('/api/checkViewExistsdb', methods=['GET'])
def check_view_exists_apidb():
    """API endpoint to check if a view name already exists"""
    try:
        company_name = request.args.get('databaseName')
        view_name = request.args.get('viewName')
        selectedUser=request.args.get('selectedUser')
        if not company_name or not view_name:
            return jsonify({'error': 'Database name and view name are required'}), 400

        exists = check_view_existsdb(company_name, view_name,selectedUser)
        return jsonify({'exists': exists})
        
    except Exception as e:
        print(f"Error checking view exists: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/createView', methods=['POST'])
def create_view_api():
    """API endpoint to create a database view"""
    try:
        data = request.get_json()
        company_name = data.get('databaseName')
        print("company_name",company_name)
        view_config = data.get('viewConfig')
        
        if not company_name or not view_config:
            return jsonify({'error': 'Database name and view configuration are required'}), 400
        
        if not view_config.get('viewName') or not view_config.get('baseTable'):
            return jsonify({'error': 'View name and base table are required'}), 400

        result = create_database_view(company_name, view_config)
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        print(f"Error creating view: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/createViewdb', methods=['POST'])
def create_view_apidb():
    """API endpoint to create a database view"""
    try:
        data = request.get_json()
        print("data------------------",data)
        company_name = data.get('databaseName')
        view_config = data.get('viewConfig')
        selectedUser=request.args.get('selectedUser')
        
        if not company_name or not view_config:
            return jsonify({'error': 'Database name and view configuration are required'}), 400
        
        if not view_config.get('viewName') or not view_config.get('baseTable'):
            return jsonify({'error': 'View name and base table are required'}), 400

        result = create_database_viewdb(company_name, view_config,selectedUser)
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        print(f"Error creating view: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/getUserViews', methods=['GET'])
def get_user_views_api():
    """API endpoint to get all user-created views"""
    try:
        company_name = request.args.get('databaseName')
        
        if not company_name:
            return jsonify({'error': 'Database name is required'}), 400

        views = get_user_views(company_name)
        return jsonify(views)
        
    except Exception as e:
        print(f"Error fetching user views: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dropView', methods=['DELETE'])
def drop_view_api():
    """API endpoint to drop a database view"""
    try:
        data = request.get_json()
        company_name = data.get('databaseName')
        view_name = data.get('viewName')
        
        if not company_name or not view_name:
            return jsonify({'error': 'Database name and view name are required'}), 400

        result = drop_database_view(company_name, view_name)
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        print(f"Error dropping view: {str(e)}")
        return jsonify({'error': str(e)}), 500









# Function to create a database connection
def create_connection( db_name="datasource",
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except psycopg2.Error as e:
        return str(e)


@app.route('/save_connection', methods=['POST'])
def save_connection():
    data = request.json
    dbType = data.get('dbType')
    provider = data.get('provider')
    dbUsername = data.get('dbUsername')
    dbPassword = data.get('dbPassword')
    saveName=data.get('saveName')
    port = data.get('port')
    dbName = data.get('dbName')
    company_name=data.get('company_name')
    print(data)
    try:
            # Save the connection details to your local database
            save_connection_details(company_name,dbType, provider, dbUsername, dbPassword, port, dbName,saveName)
            print("save_connection_details",save_connection_details)
            return jsonify(success=True, message="Connection details saved successfully.")
    except Exception as e:
            return jsonify(success=False, error=f"Failed to save connection details: {str(e)}")
def save_connection_details(company_name,dbType, provider, dbUsername, dbPassword, port, dbName,saveName):
    try:
        # Connect to your local database where you want to store the connection details
        conn = psycopg2.connect(
             dbname=company_name,  # Ensure this is the correct company database
        user=USER_NAME,password=PASSWORD,host=HOST,port=PORT
            
        )
        cursor = conn.cursor()
     # Create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS external_db_connections (
            id SERIAL PRIMARY KEY,
            saveName VARCHAR(100),
            db_type VARCHAR(100),
            provider VARCHAR(100),
            db_username VARCHAR(100),
            db_password VARCHAR(100),
            port VARCHAR(10),
            db_name VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        # Insert the connection details into the table
        insert_query = sql.SQL("""
            INSERT INTO external_db_connections (saveName,db_type, provider, db_username, db_password, port, db_name)
            VALUES (%s, %s, %s, %s, %s, %s,%s)
        """)
        cursor.execute(insert_query, (saveName,dbType, provider, dbUsername, dbPassword, port, dbName))

        # Commit the changes
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        raise Exception(f"Failed to save connection details: {str(e)}")

def get_Edb_connection(username, password, host, port, db_name):
    try:
        conn = psycopg2.connect(
            dbname=db_name, user=username, password=password, host=host, port=port
        )
        return conn
    except Exception as e:
        raise Exception(f"Unable to connect to the database: {str(e)}")

from pymongo import MongoClient
import mysql.connector
# import cx_Oracle
# try:
#     cx_Oracle.init_oracle_client(lib_dir="C:\instantclient-basic-windows\instantclient_23_6")  # Update the path for your system
# except cx_Oracle.InterfaceError as e:
#     if "already been initialized" in str(e):
#         print("Oracle Client library has already been initialized. Skipping re-initialization.")
#     else:
#         raise e


@app.route('/connect', methods=['POST'])
def connect_and_fetch_tables():
    data = request.json
    dbType = data.get('dbType')
    username = data.get('username')
    password = data.get('password')
    host = data.get('host', HOST)  # Default to localhost if not provided
    port = data.get('port')  # Port should be provided based on the database type
    db_name = data.get('dbName')
    print("data", data)

    try:
        if dbType == "Oracle":
            # Connect to Oracle
            # conn = cx_Oracle.connect(
            #     user=username,
            #     password=password,
            #     dsn=f"{host}:{port}/{db_name}"  # For Oracle, the DSN includes host, port, and service name
            # )
            # cursor = conn.cursor()
            # print("Connection successful:", conn)
            if username.lower() == "sys":
                conn = cx_Oracle.connect(
                    user=username,
                    password=password,
                    dsn=f"{host}:{port}/{db_name}",
                    mode=cx_Oracle.SYSDBA  # Use SYSDBA mode for SYS user
                )
            else:
                conn = cx_Oracle.connect(
                    user=username,
                    password=password,
                    dsn=f"{host}:{port}/{db_name}"
                )

            cursor = conn.cursor()
            print("Connection successful:", conn)

            # Query to get all table names from the current schema
            cursor.execute("""
                SELECT table_name 
                FROM all_tables 
                WHERE owner = :schema_name
            """, {"schema_name": username.upper()})  # Pass parameter as a dictionary

            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif dbType == "PostgreSQL":
            import psycopg2
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                dbname=db_name, user=username, password=password, host=host, port=port
            )
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif dbType == "MongoDB":
            from pymongo import MongoClient
            # Connect to MongoDB
            mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}"
            client = MongoClient(mongo_uri)
            db = client[db_name]
            tables = db.list_collection_names()

        elif dbType == "MySQL":
            import mysql.connector
            # Connect to MySQL
            conn = mysql.connector.connect(
                host=host, user=username, password=password, database=db_name, port=port
            )
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        else:
            return jsonify(success=False, error="Unsupported database type.")

        return jsonify(success=True, tables=tables)

    except cx_Oracle.DatabaseError as e:
        return jsonify(success=False, error=f"Oracle Database Error: {str(e)}")

    except cx_Oracle.InterfaceError as e:
        return jsonify(success=False, error=f"Oracle Interface Error: {str(e)}")

    except Exception as e:
        return jsonify(success=False, error=f"Failed to connect: {str(e)}")


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

def fetch_table_names_from_external_db(db_details):
    try:
        db_type = db_details.get('dbType')
        if db_type == "PostgreSQL":
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=db_details['host'],
                database=db_details['database'],
                user=db_details['user'],
                password=db_details['password']
            )
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            table_names = [table[0] for table in cursor.fetchall()]
            conn.close()
        elif db_type == "MongoDB":
            # Connect to MongoDB
            mongo_uri = f"mongodb://{db_details['user']}:{db_details['password']}@{db_details['host']}:{db_details['port']}/{db_details['database']}"
            client = MongoClient(mongo_uri)
            db = client[db_details['database']]
            table_names = db.list_collection_names()
        elif db_type == "MySQL":
            # Connect to MySQL
            conn = mysql.connector.connect(
                host=db_details['host'],
                user=db_details['user'],
                password=db_details['password'],
                database=db_details['database'],
                port=db_details.get('port', 3306)
            )
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES;")
            table_names = [table[0] for table in cursor.fetchall()]
            conn.close()
        elif db_type == "Oracle":
            # Connect to Oracle
             
            # Connect to Oracle
            if username.lower() == "sys":
                conn = cx_Oracle.connect(
                    user=db_details['user'],
                    password=db_details['password'],
                    dsn=f"{db_details['host']}:{db_details.get('port', 1521)}/{db_details['database']}",
                    mode=cx_Oracle.SYSDBA  # Use SYSDBA mode for SYS user
                )
            else:
                conn = cx_Oracle.connect(
                    user=db_details['user'],
                    password=db_details['password'],
                    dsn=f"{db_details['host']}:{db_details.get('port', 1521)}/{db_details['database']}"
                )

            cursor = conn.cursor()
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM all_tables")
            table_names = [table[0] for table in cursor.fetchall()]
            conn.close()
        else:
            raise ValueError("Unsupported database type provided.")
        
        return table_names  # Return list of table names

    except Exception as e:
        print(f"Error fetching table names: {e}")
        return []


@app.route('/external-db/tables', methods=['GET'])
def get_external_db_table_names():
    
    company_name = request.args.get('databaseName')
    savename=request.args.get('user')
    print("companydb", company_name)
    print("savename", savename)
    external_db_connection = fetch_external_db_connection(company_name, savename)
    print("External DB Connection Details:", external_db_connection)

    if external_db_connection:
        db_details = {
            "host": external_db_connection[3],
            "database": external_db_connection[7],
            "user": external_db_connection[4],
            "password": external_db_connection[5],
            "dbType": external_db_connection[2],
            "port": external_db_connection[6],
        }
        table_names = fetch_table_names_from_external_db(db_details)
        return jsonify(table_names)
    else:
        return jsonify({"error": "Could not retrieve external database connection details"}), 500
@app.route('/external-db/tables/<table_name>', methods=['GET'])
def get_externaltable_data(table_name):
    company_name = request.args.get('databaseName')
    savename=request.args.get('user')
    print("companydb", company_name)
    print("username", savename)
    external_db_connection = fetch_external_db_connection(company_name,savename)
    if external_db_connection:
        db_details = {
             "host": external_db_connection[3],
            "database": external_db_connection[7],
            "user": external_db_connection[4],
            "password": external_db_connection[5],
            "dbType": external_db_connection[2],
            "port": external_db_connection[6],
        }
        try:
            # Fetch data from the specified table
            table_data = fetch_data_from_table(db_details, table_name)
            return jsonify(table_data)
        except Exception as e:
            return jsonify({"error": f"Error fetching data from table '{table_name}': {str(e)}"}), 500
    else:
        return jsonify({"error": "Could not retrieve external database connection details"}), 500

def fetch_data_from_table(db_details, table_name):
    try:
        dbType = db_details.get('dbType')
        if dbType == "PostgreSQL":
            conn = psycopg2.connect(
                host=db_details["host"],
                database=db_details["database"],
                user=db_details["user"],
                password=db_details["password"]
            )
        elif dbType == "MongoDB":
            mongo_uri = f"mongodb://{db_details['user']}:{db_details['password']}@{db_details['host']}:{db_details['port']}/{db_details['database']}"
            client = MongoClient(mongo_uri)
            db = client[db_details['database']]
            collection = db[table_name]
            data = list(collection.find().limit(10))  # Fetch data from MongoDB
            return data
        elif dbType == "MySQL":
            conn = mysql.connector.connect(
                host=db_details["host"],
                user=db_details["user"],
                password=db_details["password"],
                database=db_details["database"],
                port=db_details["port"]
            )
        elif dbType == "Oracle":
            if db_details["user"].lower() == "sys":
                conn = cx_Oracle.connect(
                    user=db_details["user"],
                    password=db_details["password"],
                    dsn=f"{db_details['host']}:{db_details.get['port']}/{db_details['database']}",
                    mode=cx_Oracle.SYSDBA  # Use SYSDBA mode for SYS user
                )
            else:
                conn = cx_Oracle.connect(
                user=db_details["user"],
                password=db_details["password"],
                dsn=f"{db_details['host']}:{db_details['port']}/{db_details['database']}"
            )
        else:
            raise Exception("Unsupported database type.")

        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name} FETCH FIRST 10 ROWS ONLY" if dbType == "Oracle" else f"SELECT * FROM {table_name} LIMIT 10;"
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise Exception(f"Error querying table '{table_name}': {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()


@app.route('/api/dbusers', methods=['GET'])
def fetch_saved_names():
    company_name = request.args.get('databaseName')  # Get the company name from the request
    print("Received databaseName:", company_name)  # Debugging

    if not company_name:
        print("Company name is missing!")  # Log this case
        return jsonify({'error': 'Company name is required'}), 400

    try:
        conn = psycopg2.connect(
            dbname=company_name,  # Ensure this points to the correct company database
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = conn.cursor()

        # Fetch saveName from the external_db_connections table
        cursor.execute("SELECT id, saveName FROM external_db_connections")
        results = cursor.fetchall()
        # Modify the response format in the Flask backend

        print("result",results)
        # Close the database connection
        cursor.close()
        conn.close()

        # Return the results as JSON
        # return jsonify(results)
        return jsonify([{'id': row[0], 'saveName': row[1]} for row in results])


    except psycopg2.Error as db_error:
        print(f"Database connection error: {db_error}")
        return jsonify({'error': 'Database connection failed'}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({'error': str(e)}), 500



@app.route('/ai_ml_chartdata', methods=['GET'])
def ai_ml_charts():
    dataframe = bc.global_df  # Assuming bc.global_df is your DataFrame
    ai_ml_charts_details = analyze_data(dataframe)
    print("ai_ml_charts_details====================", ai_ml_charts_details)
    return jsonify({"ai_ml_charts_details": ai_ml_charts_details})


@app.route('/boxplot-data', methods=['POST'])
def boxplot_data():
    try:
        data = request.json
        db_name = data.get("databaseName", "")  # Moved assignment here
        selectedUser=data.get("selectedUser")
        table_name=data.get("tableName")
        
        filterOptions=data.get("filterOptions")
        if not db_name:
            return jsonify({"error": "Database name is missing"}), 400
        if not selectedUser or selectedUser.lower() == 'null':
                print("Using default database connection...")
                connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
                connection = psycopg2.connect(connection_string)
        else:  # External connection
                connection_details = fetch_external_db_connection(db_name,selectedUser)
                if connection_details:
                    db_details = {
                        "host": connection_details[3],
                        "database": connection_details[7],
                        "user": connection_details[4],
                        "password": connection_details[5],
                        "port": int(connection_details[6])
                    }
                if not connection_details:
                    raise Exception("Unable to fetch external database connection details.")
                
                connection = psycopg2.connect(
                    dbname=db_details['database'],
                    user=db_details['user'],
                    password=db_details['password'],
                    host=db_details['host'],
                    port=db_details['port'],
                )
        cur = connection.cursor()
        category = data.get("category", [])  # Expected to be a list
        yAxis = data.get("yAxis", [""])[0]  # Assuming this is a single string
        table_name = data.get("tableName", "")

        if not category or not yAxis or not table_name:
            return jsonify({"error": "Missing required parameters"}), 400

        category_columns = ', '.join(category)
        print(f"Category columns: {category_columns}")
        print(f"yAxis: {yAxis}")
        print(f"table_name: {table_name}")
        print("data",data)
        flat_filter_options = [item[0] for item in filterOptions]
        # Format the list into a string for SQL
        formatted_filter_options = ', '.join(f"'{option}'" for option in flat_filter_options)

        # Debugging: Verify filter values
        print(f"Formatted filter options: {formatted_filter_options}")


        query = f"""
        SELECT 
            {category_columns},  
            MIN({yAxis}) AS min_value,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY {yAxis}) AS Q1,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY {yAxis}) AS median,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY {yAxis}) AS Q3,
            MAX({yAxis}) AS max_value
        FROM {table_name}
        where {category_columns} in ({formatted_filter_options})
        GROUP BY {category_columns};
        """

        print("Executing query:", query)
        cur.execute(query)
        results = cur.fetchall()

        if not results:
            print("No results returned from the database query.")
            return jsonify({"error": "No data available."}), 404

        print("Results fetched from database:", results)

        # Create DataFrame
        df = pd.DataFrame(results, columns=[*category, "min", "Q1", "median", "Q3", "max"])
        if df.empty:
            print("DataFrame is empty after creation.")
            return jsonify({"error": "No data available to display."}), 404

        # Prepare the response data
        response_data = [
            {
                "category": {col: row[i] for i, col in enumerate(category)},
                "min": row[len(category)],
                "Q1": row[len(category) + 1],
                "median": row[len(category) + 2],
                "Q3": row[len(category) + 3],
                "max": row[len(category) + 4]
            }
            for row in results
        ]
        

        cur.close()
        connection.close()
        return jsonify(response_data), 200

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

   
@app.route('/api/checkSaveName', methods=['POST'])
def check_save_name():
    data = request.get_json()
    save_name = data.get('saveName')
    company_name = data.get('company_name')  # Get user_id from request
    user_id=data.get('user_id')
    print("company_name",company_name)
    print("save_name",save_name)
    if not save_name or not company_name:
        return jsonify({'error': 'Save name and user_id are required'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Query to check if saveName exists for the given user_id
        query = "SELECT COUNT(*) FROM table_chart_save WHERE chart_name = %s AND company_name = %s AND user_id=%s"
        print("query",query)
        try:
            cursor.execute(query, (save_name, company_name,user_id))
            exists = cursor.fetchone()[0] > 0
        except psycopg2.errors.UndefinedTable:
            print("Table 'table_chart_save' does not exist.")
            exists = False

        cursor.close()
        conn.close()

        return jsonify({'exists': exists})
    except Exception as e:
        print("Error checking save name:", e)
        return jsonify({'error': 'Database error'}), 500


@app.route('/check_filename/<fileName>/<company_name>', methods=['GET'])
def check_filename(fileName, company_name):
    user_id = request.args.get('user_id')  # Use query param for GET

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        create_dashboard_table(conn)
        
        # Query to check if the file name exists for the given company name
        query = """
            SELECT COUNT(*) 
            FROM table_dashboard 
            WHERE file_name = %s AND company_name = %s AND user_id= %s
        """
        cursor.execute(query, (fileName, company_name,user_id))
        exists = cursor.fetchone()[0] > 0

        cursor.close()
        conn.close()

        return jsonify({'exists': exists})
    except Exception as e:
        print("Error checking file name:", e)
        return jsonify({'error': 'Database error'}), 500



import bcrypt
import binascii



@app.route('/api/validate_user', methods=['GET'])
def validate_user():
    email = request.args.get('email')
    password = request.args.get('password')
    company = request.args.get('company')  # The company is passed in the query params
    print("Password received:", password)
    
    if not email or not password:
        return jsonify({"message": "Email and password are required"}), 400

    cursor = None
    conn = None
    try:
        # Check if company is None or "null" string and connect accordingly
        if company is None or company.lower() == 'null':  # Check for 'null' string
            conn = get_db_connection()  # Fallback to default database
        else:
            conn = get_company_db_connection(company)
            
        cursor = conn.cursor()

        # Use the appropriate table based on the database being queried
        table_name =  "organizationdatatest" if company is None or company.lower() == 'null' else "employee_list"

        # Check if the user exists in the appropriate table
        cursor.execute(f"SELECT password FROM {table_name} WHERE LOWER(email) = LOWER(%s)", (email,))
        hashed_password_row = cursor.fetchone()

        # If no user is found, return an error
        if not hashed_password_row:
            return jsonify({"message": "User not found", "isValid": False}), 404

        # Extract the stored password
        stored_password = hashed_password_row[0]  # Get the stored password from the result
        print("Stored password in DB:", stored_password)

        if company is None or company.lower() == 'null':  # If company is None or 'null', use plain text comparison
            if stored_password == password:
                print("Password matched successfully!")
                return jsonify({"isValid": True})
            else:
                print("Password mismatch!")
                return jsonify({"isValid": False, "message": "Incorrect password"})
        else:  # If company is provided, use bcrypt to compare hashed password
            # If the password is stored in hex format, convert it to bytes
            if stored_password.startswith('\\x'):
                stored_hash_bytes = binascii.unhexlify(stored_password[2:])
            else:
                stored_hash_bytes = stored_password.encode('utf-8')

            print("Decoded stored hash (bytes):", stored_hash_bytes)

            if bcrypt.checkpw(password.encode('utf-8'), stored_hash_bytes):
                print("Password matched successfully!")
                return jsonify({"isValid": True})

            print("Password mismatch!")
            return jsonify({"isValid": False, "message": "Incorrect password"})

    except Exception as e:
        print(f"Error validating user: {e}")
        return jsonify({"message": "Internal server error"}), 500
    finally:
        if cursor:  # Ensure cursor exists before closing
            cursor.close()
        if conn:  # Ensure connection exists before closing
            conn.close()



def create_roles_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS role (
            role_id SERIAL PRIMARY KEY,
            role_name VARCHAR(255) UNIQUE NOT NULL,
            permissions VARCHAR(255)
        );
        """
    )
    conn.commit()
    cursor.close()
    conn.close()

# @app.route('/updateroles', methods=['POST'])
# def add_role():
#     try:
#         create_roles_table()
#         data = request.json
#         print(data)
#         role_name = data['role_name']
#         permissions = data['permissions']  # Expect permissions as a list

#         # Convert list to comma-separated string
#         permissions_str = ', '.join(permissions)

#         conn = get_db_connection()
#         cursor = conn.cursor()
#         cursor.execute(
#             """
#             INSERT INTO role (role_name, permissions)
#             VALUES (%s, %s)
#             ON CONFLICT (role_name) DO NOTHING;
#             """,
#             (role_name, permissions_str)
#         )
#         conn.commit()
#         cursor.close()
#         conn.close()
#         return jsonify({"message": "Role added successfully"}), 201

#     except Exception as e:
#         logging.error(f"Error adding role: {str(e)}")
#         return jsonify({"error": str(e)}), 500
@app.route('/updateroles', methods=['POST'])
def add_role():
    try:
        data = request.json
        role_name = data['role_name']
        permissions = data['permissions']  # dict format
        company_name = data.get('company_name')

        if not company_name:
            return jsonify({"error": "Missing company_name"}), 400

        # Generate a role_permission_code from permission keys (or use UUID if needed)
        role_permission_code = role_name[:2].upper()  # or generate a hash/UUID here

        # Step 1: Create tables if not exist
        conn = get_company_db_connection(company_name)
        cursor = conn.cursor()

        # Create role_permission table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS role_permission (
                id SERIAL PRIMARY KEY,
                role_permission_code VARCHAR(10) UNIQUE NOT NULL,
                can_datasource BOOLEAN DEFAULT FALSE,
                can_view BOOLEAN DEFAULT FALSE,
                can_edit BOOLEAN DEFAULT FALSE,
                can_design BOOLEAN DEFAULT FALSE,
                can_load BOOLEAN DEFAULT FALSE,
                can_update BOOLEAN DEFAULT FALSE,
                can_edit_profile BOOLEAN DEFAULT FALSE
            );
        """)

        # Create role table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS role (
                role_id SERIAL PRIMARY KEY,
                role_name VARCHAR(100) UNIQUE NOT NULL,
                permissions VARCHAR(10),
                CONSTRAINT fk_permissions FOREIGN KEY (permissions)
                  REFERENCES role_permission(role_permission_code)
                  ON DELETE CASCADE
            );
        """)

        # Step 2: Insert into role_permission if not exists
        cursor.execute("""
            INSERT INTO role_permission (
                role_permission_code, can_datasource, can_view, can_edit,
                can_design, can_load, can_update, can_edit_profile
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (role_permission_code) DO NOTHING;
        """, (
            role_permission_code,
            permissions.get('can_datasource', False),
            permissions.get('can_view', False),
            permissions.get('can_edit', False),
            permissions.get('can_design', False),
            permissions.get('can_load', False),
            permissions.get('can_update', False),
            permissions.get('can_edit_profile', False)
        ))

        # Step 3: Insert into role table with foreign key
        cursor.execute("""
            INSERT INTO role (role_name, permissions)
            VALUES (%s, %s)
            ON CONFLICT (role_name) DO NOTHING;
        """, (role_name, role_permission_code))

        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Role added successfully"}), 201

    except Exception as e:
        logging.error(f"Error adding role: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/update-chart-position', methods=['POST'])
def update_chart_position():
    data = request.get_json()
    chart_ids = data.get('chart_id')
    positions = data.get('position')
    print("chart_ids",chart_ids)
    print("positions",positions)
    if not chart_ids or not positions or len(chart_ids) != len(positions):
        return jsonify({"error": "Missing or mismatched chart_ids or positions data"}), 400

    # Connect to the database
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Check if the data already exists in the table
        cur.execute("SELECT * FROM table_dashboard WHERE chart_ids = %s;", (chart_ids,))

        existing_record = cur.fetchone()
        print("existing_record",existing_record)

        if existing_record:
            # Update existing record with new positions
            cur.execute("""
                UPDATE table_dashboard
                SET position = %s, updated_at = CURRENT_TIMESTAMP
                WHERE chart_ids = %s;
            """, (json.dumps(positions), chart_ids))
        else:
            # Insert new record if it doesn't exist
            cur.execute("""
                INSERT INTO table_dashboard (chart_ids, position, created_at, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            """, (chart_ids, json.dumps(positions)))

        conn.commit()
        return jsonify({"message": "Chart positions updated successfully"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        cur.close()
        conn.close()
def dashboard_wallpapers_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_wallpapers (
            wallpaper_id SERIAL PRIMARY KEY,
            src TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
@app.route('/api/saveDashboard', methods=['POST'])
def save_dashboard():
    data = request.json
    user_id = data['user_id']
    # dashboard_name = data['dashboard_name']
    # dashboard_name = data['dashboard_name'].strip()
    dashboard_name_data = data.get("dashboard_name")

    if isinstance(dashboard_name_data, list):
            # Expect [id, name]
        if len(dashboard_name_data) == 2:
            user_id, dashboard_name = dashboard_name_data
        else:
            return jsonify({"message": "Invalid dashboard_name format"}), 400
    elif isinstance(dashboard_name_data, str):
        user_id, dashboard_name = dashboard_name_data.split(",", 1)
    else:
        return jsonify({"message": "dashboard_name must be string or list"}), 400

    chart_details = data['chart_details']
    DashboardHeading = data.get('DashboardHeading', '').strip()
    position = data['position']  # Get position from the new structure
    droppableBgColor=data['droppableBgColor']
    imagePositions=data['imagePositions']
    fontStyleState = data.get('fontStyleLocal', '')
    fontSize = data.get('fontSize', '')
    fontColor = data.get('fontColor', '')
    wallpaper=data.get("wallpaper")

    # Bgcolour=data['bgcolor']
    print("user_id:", user_id)
    print("dashboard_name:", dashboard_name)
    print("DashboardHeading:", DashboardHeading)
    print("chart_details:", chart_details)
    print("positions:", position)
    print("droppableBgColor",droppableBgColor)
    # print("imagePositions",imagePositions)
    # print("Bgcolour",Bgcolour)

    try:
        conn = get_db_connection()
        dashboard_wallpapers_table(conn)
        cur = conn.cursor()
        # Step: Delete old image records not in current imagePositions list
        cur.execute("""
            SELECT image_ids FROM table_dashboard
            WHERE user_id = %s AND TRIM(file_name) = %s
        """, (user_id, dashboard_name))

        result = cur.fetchone()
        existing_image_ids = []

        if result and result[0]:
            try:
                existing_image_ids = json.loads(result[0]) if isinstance(result[0], str) else result[0]
            except Exception as parse_err:
                print("Error parsing existing image_ids:", parse_err)

        new_image_ids = [img.get('image_id') for img in imagePositions if 'image_id' in img]

        # Get image IDs to delete
        image_ids_to_delete =existing_image_ids
        print("Image IDs to delete:", image_ids_to_delete)

        if image_ids_to_delete:
            cur.execute(
                "DELETE FROM image_positions WHERE image_id = ANY(%s)",
                (image_ids_to_delete,)
            )
            conn.commit()
            print("Old images removed successfully")
        wallpaper_id = None
        if wallpaper:
            # If wallpaper is a dict with 'src'
            if isinstance(wallpaper, dict) and 'src' in wallpaper:
                wallpaper_src = wallpaper['src']
            # If wallpaper is already a string
            elif isinstance(wallpaper, str):
                wallpaper_src = wallpaper
            else:
                return jsonify({"message": "Invalid wallpaper format"}), 400

            cur.execute("""
                INSERT INTO dashboard_wallpapers (src)
                VALUES (%s)
                RETURNING wallpaper_id;
            """, (wallpaper_src,))
            wallpaper_id = cur.fetchone()[0]
            conn.commit()
            print("Saved wallpaper with ID:", wallpaper_id)

        # **Step 1: Clear existing chart details while keeping other columns intact**
        update_query = """
            UPDATE table_dashboard
            SET chart_ids = NULL, heading = NULL, position = NULL, chart_size = NULL, chart_type = NULL, chart_xaxis = NULL, chart_yaxis = NULL, chart_aggregate = NULL,filterdata=NULL,droppableBgColor=NULL,opacity=NULL,chartcolor=NULL,font_style_state=NULL,font_size=NULL,font_color=NULL,wallpaper_id=NULL            
            WHERE user_id = %s AND file_name = %s;
        """
        cur.execute(update_query, (user_id, dashboard_name))
        conn.commit()
        print("Cleared existing chart details successfully!")
        

        if chart_details:
            chart_ids = [chart['chart_id'] for chart in chart_details]
            chart_ids_str = '{' + ','.join(map(str, chart_ids)) + '}'  # Convert to PostgreSQL array format

            # Extract x and y from the new position structure
            positions = json.dumps([{'x': p['x'], 'y': p['y']} for p in position])
            # Extract width and height from the new position structure
            chart_sizes = json.dumps([{'width': p['width'], 'height': p['height']} for p in position])

            aggregations = json.dumps([chart.get('aggregation', None) for chart in chart_details]) # Handle optional aggregation
            xaxes = json.dumps([chart.get('x_axis', []) for chart in chart_details])  # Correct key name
            yaxes = json.dumps([chart.get('y_axis', []) for chart in chart_details])  # Correct key name
            opacities = json.dumps([chart.get('opacity', 1) for chart in chart_details])  # default to 1 if missing
            bgcolors = json.dumps([chart.get('bgcolor', '#ffffff') for chart in chart_details])

            filter_options_list = [chart.get('filter_options', None) for chart in chart_details]
            filter_options_json_list = []
            for item in filter_options_list:
                if item is None:
                    filter_options_json_list.append(None)
                elif isinstance(item, dict) or isinstance(item, list):
                    filter_options_json_list.append(json.dumps(item))
                elif isinstance(item, str):
                    try:
                        json.loads(item)
                        filter_options_json_list.append(item) # It's already a valid JSON string
                    except json.JSONDecodeError:
                        filter_options_json_list.append(json.dumps(item)) # Treat as a regular string and convert to JSON string
                else:
                    filter_options_json_list.append(json.dumps(item)) # Convert other data types to JSON string

            filter_options = json.dumps(filter_options_json_list)

            chart_type = json.dumps([chart.get('chart_type', None) for chart in chart_details])
            cur.execute("SELECT user_id, file_name FROM table_dashboard")
            all_dashboards = cur.fetchall()
            print("Existing dashboards in DB:")
            for row in all_dashboards:
                print(f"user_id: {row[0]}, file_name: '{row[1]}'")
            print("font_style_state",fontStyleState)
            update_chart_query = """
                UPDATE table_dashboard
                SET chart_ids = %s, heading = %s, position = %s::jsonb, chart_size = %s::jsonb, chart_aggregate = %s::jsonb, chart_xaxis = %s::jsonb, chart_yaxis = %s::jsonb,chart_type = %s::jsonb,filterdata = %s,droppableBgColor=%s, opacity = %s::jsonb,chartcolor = %s::jsonb,font_style_state = %s,
                font_size = %s,font_color = %s,wallpaper_id = %s WHERE user_id = %s AND file_name = %s;
            """
            cur.execute(update_chart_query, (chart_ids_str, DashboardHeading, positions, chart_sizes, aggregations, xaxes, yaxes, chart_type, filter_options,droppableBgColor,opacities,bgcolors,fontStyleState, fontSize, fontColor,wallpaper_id, user_id, dashboard_name))
            conn.commit()
            
            for chart in chart_details:
                chart_id = chart.get("chart_id")
                chart_type = chart.get("chart_type")
                if chart_id and chart_type:
                    cur.execute("""
                        UPDATE table_chart_save
                        SET chart_type = %s
                        WHERE id = %s;
                    """, (chart_type, chart_id))
            conn.commit()

            print("Updated chart_type in table_chart_save successfully!")
            
            print("update_chart_query ",update_chart_query)
            print("Inserted new chart details successfully!")
            if imagePositions:
                print("Processing imagePositions...")

                image_ids = []

                for img in imagePositions:
                    image_id = img.get('image_id')
                    image_ids.append(image_id)

                    cur.execute("""
                        INSERT INTO image_positions (image_id, src, x, y, width, height, zIndex, disableDragging)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (image_id) DO UPDATE SET
                            src = EXCLUDED.src,
                            x = EXCLUDED.x,
                            y = EXCLUDED.y,
                            width = EXCLUDED.width,
                            height = EXCLUDED.height,
                            zIndex = EXCLUDED.zIndex,
                            disableDragging = EXCLUDED.disableDragging
                    """, (
                        image_id, img['src'], img['x'], img['y'],
                        img['width'], img['height'], img['zIndex'],
                        img['disableDragging']
                    ))
                    conn.commit()

                # # Then update table_dashboard with image_ids
                # image_ids_pg_array = '{' + ','.join(image_ids) + '}'
                # cur.execute("""
                #     UPDATE table_dashboard
                #     SET image_ids = %s
                #     WHERE user_id = %s AND file_name = %s;
                # """, (image_ids_pg_array, user_id, dashboard_name))
                image_ids_json = json.dumps(image_ids)
                print("image_ids_json",image_ids_json)

                cur.execute("""
                    UPDATE table_dashboard
                    SET image_ids = %s::jsonb
                    WHERE user_id = %s AND file_name = %s;
                """, (image_ids_json, user_id, dashboard_name))
                print("Updated rows:", cur.rowcount)
                conn.commit()



            cur.execute("""
                SELECT chart_ids, position, chart_size, droppableBgColor 
                FROM table_dashboard 
                WHERE user_id = %s AND TRIM(file_name) = %s
            """, (user_id, dashboard_name))



        cur.close()
        conn.close()

        return jsonify({"message": "Dashboard updated successfully"}), 200

    except Exception as e:
        import traceback
        print("Error occurred:", e)
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

from flask import request
from urllib.parse import quote_plus
import sweetviz as sv
from sqlalchemy import create_engine
from flask import Flask, jsonify

if not os.path.exists('static'):
    os.makedirs('static')

@app.route('/api/generate-dashboard', methods=['GET'])
def generate_dashboard():
    try:
        db_name = request.args.get('db_name')
        table_name = request.args.get('table_name')

        if not db_name or not table_name:
            return jsonify({'success': False, 'error': 'Database name and table name are required'}), 400

        db_username = 'postgres'
        db_password = password
        db_host = HOST
        db_port = '5432'
        encoded_password = quote_plus(db_password)

        DATABASE_URL = f'postgresql+psycopg2://{db_username}:{encoded_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(DATABASE_URL)

        # Only fetch sample data
        query = f"SELECT * FROM {table_name} LIMIT 5000"
        df = pd.read_sql(query, engine)

        dashboard_filename = f'dashboard_{db_name}_{table_name}.html'
        print("dashboard_filename",dashboard_filename)
        # dashboard_path = os.path.join('static', dashboard_filename)
        dashboard_path = os.path.join(app.static_folder, dashboard_filename) # Use app.static_folder here
        
        # Ensure the static directory exists
        os.makedirs(app.static_folder, exist_ok=True)
        print("dashboard_path",dashboard_path)
        # Generate report using sample
        report = sv.analyze(df)
        report.show_html(dashboard_path, open_browser=False)
        dashboard_url = f'http://localhost:5000/static/{dashboard_filename}'
        print(f"Dashboard URL generated: {dashboard_url}")
        
        return jsonify({
            'success': True,
            'message': 'Dashboard created successfully!',
            'dashboard_url':dashboard_url
        })
       
    except Exception as e:
        print(f"Error generating dashboard: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/dbconnect', methods=['POST'])
def connect_and_fetch_dbtables():
    data = request.json
    db_type = data.get('dbType')
    username = data.get('username')  # changed
    password = data.get('password')  # changed
    host = data.get('provider', HOST)  # changed
    port = data.get('port')  # same
    db_name = data.get('dbName')  # same
    print("Received DB connection data:", data)

    try:
        if db_type == "Oracle":
            import cx_Oracle
            if username.lower() == "sys":
                conn = cx_Oracle.connect(
                    user=username,
                    password=password,
                    dsn=f"{host}:{port}/{db_name}",
                    mode=cx_Oracle.SYSDBA
                )
            else:
                conn = cx_Oracle.connect(
                    user=username,
                    password=password,
                    dsn=f"{host}:{port}/{db_name}"
                )

            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM all_tables 
                WHERE owner = :schema_name
            """, {"schema_name": username.upper()})
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif db_type == "PostgreSQL":
            import psycopg2
            conn = psycopg2.connect(
                dbname=db_name, user=username, password=password, host=host, port=port
            )
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif db_type == "MongoDB":
            from pymongo import MongoClient
            mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}"
            client = MongoClient(mongo_uri)
            db = client[db_name]
            tables = db.list_collection_names()

        elif db_type == "MySQL":
            import mysql.connector
            conn = mysql.connector.connect(
                host=host, user=username, password=password, database=db_name, port=port
            )
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        else:
            return jsonify(success=False, error="Unsupported database type.")

        return jsonify(success=True, tables=tables)

    except Exception as e:
        return jsonify(success=False, error=f"Failed to connect: {str(e)}")
    


# from apscheduler.schedulers.background import BackgroundScheduler

# scheduler = BackgroundScheduler()
# scheduler.start()


# Initialize scheduler
app.config['SCHEDULER_API_ENABLED'] = True



# scheduler.scheduler.jobstores = {
#     'default': SQLAlchemyJobStore(
#         url=f'postgresql://{USER_NAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}'
#     )
# }
# app.config['SCHEDULER_JOBSTORES'] = {
#     'default': SQLAlchemyJobStore(
#         url=f'postgresql://{USER_NAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}'
#     )
# }


encoded_password = urllib.parse.quote_plus(PASSWORD)

app.config['SCHEDULER_JOBSTORES'] = {
    'default': SQLAlchemyJobStore(
        url=f'postgresql://{USER_NAME}:{encoded_password}@{HOST}:{PORT}/{DB_NAME}'
    )
}

app.config['SCHEDULER_JOB_DEFAULTS'] = {
    'coalesce': False,
    'max_instances': 3,
    'misfire_grace_time': 60
}
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
def update_last_transfer_status(source_table, dest_table, status, message):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS last_transfer_status (
                id SERIAL PRIMARY KEY,
                source_table VARCHAR NOT NULL,
                destination_table VARCHAR NOT NULL,
                last_transfer_time TIMESTAMP,
                status VARCHAR,
                message TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (source_table, destination_table)
            );
        """)
        conn.commit()

        upsert_query = """
            INSERT INTO last_transfer_status (
                source_table, destination_table, last_transfer_time, status, message
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_table, destination_table)
            DO UPDATE SET
                last_transfer_time = EXCLUDED.last_transfer_time,
                status = EXCLUDED.status,
                message = EXCLUDED.message,
                updated_at = CURRENT_TIMESTAMP;
        """
        cur.execute(upsert_query, (
            source_table, dest_table, datetime.utcnow(), status, message
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Failed to update last transfer status: {str(e)}")


def create_log_table_if_not_exists():
    try:
        conn = get_db_connection()
        # conn = psycopg2.connect(**LOG_DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS data_transfer_logs (
                id SERIAL PRIMARY KEY,
                source_table VARCHAR,
                destination_table VARCHAR,
                schedule_type VARCHAR,
                run_time TIMESTAMP,
                status VARCHAR,
                message TEXT,
                record_count INTEGER,
                data_size_mb FLOAT,
                user_email VARCHAR,
                job_id VARCHAR,
                time_taken_seconds FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                inserted_count INTEGER DEFAULT 0,
                updated_count INTEGER DEFAULT 0,
                skipped_count INTEGER DEFAULT 0
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ 'data_transfer_logs' table ensured.")
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
def to_native(val):
    """Convert NumPy datatypes to Python-native types."""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    return val
def log_data_transfer(source_table, dest_table, schedule_type, run_time,
                      status, message, record_count, data_size_mb, email, job_id,time_taken_seconds, inserted_count=0, updated_count=0, skipped_count=0):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,  # Update this
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cur = conn.cursor()
        insert_query = """
            INSERT INTO data_transfer_logs (
                source_table, destination_table, schedule_type, run_time,
                status, message, record_count, data_size_mb, user_email, job_id,time_taken_seconds, inserted_count, updated_count, skipped_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s)
        """
        # cur.execute(insert_query, (
        #     source_table, dest_table, schedule_type, run_time,
        #     status, message, record_count, data_size_mb, email, job_id,time_taken_seconds, inserted_count, updated_count, skipped_count
        # ))
        values = (
            source_table, dest_table, schedule_type, run_time,
            status, message, record_count, data_size_mb,
            email, job_id, time_taken_seconds,
            inserted_count, updated_count, skipped_count
        )

        cur.execute(insert_query, tuple(to_native(v) for v in values))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Failed to log data transfer: {str(e)}")
# def send_notification_email(recipient, subject, body):
#     try:
#         print("Using Hotmail SMTP to send email...")

#         # Create email
#         msg = MIMEText(body, "plain")
#         # msg = MIMEText(body, "plain", "utf-8") 
#         msg["Subject"] = subject
#         msg["From"] = HOTMAIL_USER
#         msg["To"] = recipient

#         with smtplib.SMTP("smtp.hostinger.com", 587) as server:
#             server.starttls()
#             server.login(HOTMAIL_USER, HOTMAIL_PASS)
#             server.sendmail(HOTMAIL_USER, recipient, msg.as_string())

#         print("Email sent successfully")
#     except Exception as e:
#         print("Hotmail SMTP error:", e)
def send_notification_email(recipient, subject, body):
    try:
        print("Using Hotmail SMTP to send email...")

        msg = MIMEText(body, "plain", "utf-8")  # enforce UTF-8
        msg["Subject"] = subject
        msg["From"] = HOTMAIL_USER
        msg["To"] = recipient

        with smtplib.SMTP("smtp.hostinger.com", 587) as server:
            server.starttls()
            server.login(HOTMAIL_USER, HOTMAIL_PASS)
            # ✅ encode to UTF-8 before sending
            server.sendmail(HOTMAIL_USER, recipient, msg.as_string().encode("utf-8"))

        print("✅ Email sent successfully")
    except Exception as e:
        print("Hotmail SMTP error:", e)


    # try:
    #     print("Using SendGrid API to send email...")
    #     message = Mail(
    #         from_email=FROM_EMAIL,
    #         to_emails=recipient,
    #         subject=subject,
    #         plain_text_content=body
    #     )
    #     sg = SendGridAPIClient(SENDGRID_API_KEY)
    #     print("message", message)

    #     response = sg.send(message)
    #     print(f"SendGrid response: {response.status_code}")
    # except Exception as e:
    #     print("SendGrid error:", e)

# def job_logic(cols=None, source_config=None, destination_config=None,
#               source_table_name=None, dest_table_name=None,
#               update_existing_table=False, create_view_if_exists=False, email=None):
#     print(f"[{datetime.now()}] Job triggered with columns: {cols}")
#     try:
#         source_df, fetch_error = fetch_data_with_columns(source_config, source_table_name, cols)
#         if fetch_error:
#             msg = f"Failed to fetch data: {fetch_error}"
#             print(msg)
#             if email:
#                 send_notification_email(email, "Data Transfer Failed", msg)
#             return {"success": False, "error": msg}

#         if source_df is None or source_df.empty:
#             msg = f"No data found in table '{source_table_name}'"
#             print(msg)
#             if email:
#                 send_notification_email(email, "Data Transfer Skipped", msg)
#             return {"success": True, "message": msg}

#         insert_success, insert_error, view_created, view_name = insert_dataframe_with_upsert(
#             destination_config, dest_table_name, source_df, source_table_name, cols, create_view_if_exists
#         )

#         if not insert_success:
#             msg = f"Failed to insert data: {insert_error}"
#             print(msg)
#             if email:
#                 send_notification_email(email, "Data Transfer Failed", msg)
#             return {"success": False, "error": msg}

#         records_count = len(source_df)
#         data_size = source_df.memory_usage(deep=True).sum() / (1024 * 1024)
#         transfer_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
#         view_info = f"\nA view named '{view_name}' was created." if view_name else ""

#         email_body = f"""\nSubject: 3A Vision Data Transfer Completed

# Hello {email},

# Data transfer from '{source_config.get('dbName')}' to '{destination_config.get('dbName')}' completed.

# Details:
# Table: {source_table_name}
# Records: {records_count}
# Size: {data_size:.2f} MB
# Time: {transfer_time}
# {view_info}

# Regards,
# 3A Vision Team
# """
#         if email:
#             send_notification_email(email, "3A Vision Data Transfer Completed", email_body)

#         return {"success": True, "message": f"Data transferred from '{source_table_name}' to '{dest_table_name}'."}

#     except Exception as e:
#         error_msg = f"Unexpected error: {str(e)}"
#         print(traceback.format_exc())
#         if email:
#             send_notification_email(email, "Data Transfer Failed", error_msg)
#         return {"success": False, "error": error_msg}
def job_logic(cols=None, source_config=None, destination_config=None,
              source_table_name=None, dest_table_name=None,
              update_existing_table=False, create_view_if_exists=False,
              email=None, schedule_type=None, job_id=None):
    start_time = datetime.utcnow()
    print(f"[{datetime.now()}] Job triggered for table: {source_table_name}")
    create_log_table_if_not_exists()

    try:
        # Placeholder for your actual data fetch logic
        source_df, fetch_error = fetch_data_with_columns(source_config, source_table_name, cols)
        if fetch_error:
            msg = f"Failed to fetch data: {fetch_error}"
            print(msg)
            if email:
                send_notification_email(email, "Data Transfer Failed", msg)
            return {"success": False, "error": msg}

        if source_df is None:
            msg = f"No data found in '{source_table_name}'"
            if email:
                send_notification_email(email, "Data Transfer Skipped", msg)
            log_data_transfer(source_table_name, dest_table_name, schedule_type,
                              datetime.utcnow(), "Skipped", msg, 0, 0.0, email, job_id, 0.0,inserted_count=0, updated_count=0, skipped_count=0)
            update_last_transfer_status(source_table_name, dest_table_name, "Skipped", msg)
            return {"success": True, "message": msg}

        # Placeholder for insert logic
        insert_success = True  # Replace with actual insert result
        insert_error = None
        view_name = None
        insert_success, insert_error, view_created, view_name ,inserted_count, updated_count = insert_dataframe_with_upsert(
            destination_config, dest_table_name, source_df, source_table_name, cols, create_view_if_exists
        )
        skipped_count = len(source_df) - inserted_count - updated_count
        if not insert_success:
            msg = f"Insert failed: {insert_error}"
            if email:
                send_notification_email(email, "Data Transfer Failed", msg)
            log_data_transfer(source_table_name, dest_table_name, schedule_type,
                              datetime.utcnow(), "Failed", msg, 0, 0.0, email, job_id, 0.0,inserted_count, updated_count, skipped_count)
            update_last_transfer_status(source_table_name, dest_table_name, "Failed", msg)
            return {"success": False, "error": msg}

        records_count = len(source_df)
        data_size = source_df.memory_usage(deep=True).sum() / (1024 * 1024)

        msg = f"Transferred {records_count} records from {source_table_name} to {dest_table_name}."

        email_body = f"""
Subject: 3A Vision Data Transfer Completed

Hello {email},

✅ Data transfer from '{source_config.get('dbName')}' to '{destination_config.get('dbName')}' completed.

Details:
Table: {source_table_name}
Records: {records_count}
Size: {data_size:.2f} MB
Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

Regards,
3A Vision Team
"""
        if email:
            send_notification_email(email, "3A Vision Data Transfer Completed", email_body)
        end_time = datetime.utcnow()
        time_taken_seconds = (end_time - start_time).total_seconds()
        log_data_transfer(source_table_name, dest_table_name, schedule_type,
                          datetime.utcnow(), "Success", msg, records_count, data_size, email, job_id,time_taken_seconds,inserted_count, updated_count, skipped_count)
        update_last_transfer_status(source_table_name, dest_table_name, "Success", msg)
        return {"success": True, "message": msg}

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(traceback.format_exc())
        if email:
            send_notification_email(email, "Data Transfer Failed", error_msg)
        log_data_transfer(source_table_name, dest_table_name, schedule_type,
                          datetime.utcnow(), "Failed", error_msg, 0, 0.0, email, job_id,0.0,inserted_count=0, updated_count=0, skipped_count=0)
        return {"success": False, "error": error_msg}

# @app.route('/api/transfer_data', methods=['POST'])
# def transfer_and_verify_data():
#     data = request.get_json()
#     source_config = data.get('source')
#     destination_config = data.get('destination')
#     source_table_name = data.get('sourceTable')
#     dest_table_name = data.get('destinationTable')
#     selected_columns = data.get('selectedColumns')
#     schedule_type = data.get('scheduleType')
#     schedule_time = data.get('scheduleTime')
#     update_existing_table = data.get('updateExistingTable', False)
#     create_view_if_exists = data.get('createViewIfExists', False)
#     email = data.get('email')

#     if not dest_table_name:
#         timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
#         dest_table_name = f"{source_table_name}_copy_{timestamp}"

#     if not source_config or not destination_config or not source_table_name or not dest_table_name:
#         return jsonify({"success": False, "error": "Missing configuration or table names"}), 400

#     if not schedule_type or schedule_type == '':
#         result = job_logic(selected_columns)
#         return jsonify(result)

#     # Scheduled job
#     job_id = f"{source_table_name}_to_{dest_table_name}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
#     try:
#         schedule_hour, schedule_minute = map(int, schedule_time.split(':')) if schedule_time else (0, 0)
#         now = datetime.now()
#         # job_kwargs = {"cols": selected_columns}
#         job_kwargs = {
#     "cols": selected_columns,
#     "source_config": source_config,
#     "destination_config": destination_config,
#     "source_table_name": source_table_name,
#     "dest_table_name": dest_table_name,
#     "update_existing_table": update_existing_table,
#     "create_view_if_exists": create_view_if_exists,
#     "email": email
# }

#         print("job_kwargs",job_kwargs)

#         if schedule_type == 'once':
#             run_time = now.replace(hour=schedule_hour, minute=schedule_minute, second=0, microsecond=0)
#             if run_time < now:
#                 run_time += timedelta(days=1)
#             scheduler.add_job(func=job_logic, trigger='date', run_date=run_time, id=job_id, kwargs=job_kwargs)
#         elif schedule_type == 'hourly':
#             scheduler.add_job(
#     func=job_logic,
#     trigger='interval',
#     hours=1,
#     id=job_id,
#     kwargs=job_kwargs,
#     replace_existing=True,
#     misfire_grace_time=3600,
#     next_run_time=datetime.now()
# )


#             # scheduler.add_job(func=job_logic, trigger='interval', hours=1, id=job_id, kwargs=job_kwargs)
#         elif schedule_type == 'daily':
#             scheduler.add_job(func=job_logic, trigger='cron', hour=schedule_hour, minute=schedule_minute, id=job_id, kwargs=job_kwargs,next_run_time=datetime.now())
#         else:
#             return jsonify({"success": False, "error": f"Invalid schedule type: {schedule_type}"}), 400

#         # return jsonify({"success": True, "message": f"Job scheduled successfully ({schedule_type})", "job_id": job_id})
#         # Print all scheduled jobs in terminal
#         print("\n📋 Scheduled Jobs List:")
#         for job in scheduler.get_jobs():
#             print(job)
#             print(f"- ID: {job.id}")
#             print(f"  Next Run Time: {job.next_run_time}")
#             print(f"  Trigger: {job.trigger}")
#             print(f"  Function: {job.func_ref}")
#             print(f"  Args: {job.args}")
#             print(f"  Kwargs: {job.kwargs}")
#             print("-" * 50)
#         if email:
#             schedule_msg = f"""\
#             Subject: 3A Vision Data Transfer Scheduled

#             Hello {email},

#             Your data transfer job has been successfully scheduled.

#             Details:
#             Source Table: {source_table_name}
#             Destination Table: {dest_table_name}
#             Schedule Type: {schedule_type}
#             Scheduled Time: {schedule_time if schedule_time else 'N/A'}
#             Job ID: {job_id}

#             Regards,
#             3A Vision Team
#             """
#             send_notification_email(email, "3A Vision Data Transfer Scheduled", schedule_msg)

#             return jsonify({"success": True, "message": f"Job scheduled successfully ({schedule_type})", "job_id": job_id})

#         # return jsonify({"success": True, "message": f"Job scheduled successfully ({schedule_type})", "job_id": job_id})

#     except Exception as e:
#         error_msg = f"Failed to schedule job: {str(e)}"
#         print(error_msg)
#         if email:
#             send_notification_email(email, "Data Transfer Scheduling Failed", error_msg)
#         return jsonify({"success": False, "error": error_msg}), 500
@app.route('/api/transfer_data', methods=['POST'])
def transfer_and_verify_data():
    data = request.get_json()
    source_config = data.get('source')
    destination_config = data.get('destination')
    source_table_name = data.get('sourceTable')
    dest_table_name = data.get('destinationTable')
    selected_columns = data.get('selectedColumns')
    schedule_type = data.get('scheduleType')
    schedule_time = data.get('scheduleTime')
    update_existing_table = data.get('updateExistingTable', False)
    create_view_if_exists = data.get('createViewIfExists', False)
    email = data.get('email')
    print("dest_table_name",dest_table_name)

    # if not dest_table_name:

    #     dest_table_name = f"{source_table_name}_copy_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    if not source_config or not destination_config or not source_table_name:
        return jsonify({"success": False, "error": "Missing configuration or table names"}), 400
        # if not source_config or not destination_config or not source_table_name:
        # return jsonify({"success": False, "error": "Missing configuration or table names"}), 400

    # If destination table name is not provided, determine it dynamically
    if not dest_table_name:
        try:
            conn = psycopg2.connect(
            dbname=destination_config['dbName'],
            user=destination_config['dbUsername'],
            password=destination_config['dbPassword'],
            host=destination_config['provider'] or 'localhost',
            port=destination_config['port'] or '5432'
            )
            dest_cursor = conn.cursor()
            # Connect to destination DB
            # dest_conn = psycopg2.connect(
            #     host=destination_config["host"],
            #     port=destination_config["port"],
            #     user=destination_config["user"],
            #     password=destination_config["password"],
            #     database=destination_config["database"]
            # )
            # dest_cursor = dest_conn.cursor()

            # Check if source table exists in destination DB
            dest_cursor.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                );
            """, (source_table_name,))
            table_exists = dest_cursor.fetchone()[0]

            if table_exists:
                # If exists, create a copy name
                dest_table_name = f"{source_table_name}_copy_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            else:
                # If not exists, keep the same as source
                dest_table_name = source_table_name

            dest_cursor.close()
            conn.close()
        except Exception as e:
            return jsonify({"success": False, "error": f"Error checking destination table: {str(e)}"}), 500

    print("Final destination table name:", dest_table_name)
    

    job_id = f"{source_table_name}_to_{dest_table_name}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    job_kwargs = {
        "cols": selected_columns,
        "source_config": source_config,
        "destination_config": destination_config,
        "source_table_name": source_table_name,
        "dest_table_name": dest_table_name,
        "update_existing_table": update_existing_table,
        "create_view_if_exists": create_view_if_exists,
        "email": email,
        "schedule_type": schedule_type or "instant",
        "job_id": job_id
    }
    print("job_kwargs",job_kwargs)
    if not schedule_type or schedule_type == '':
        result = job_logic(**job_kwargs)
        return jsonify(result)

    try:
        schedule_hour, schedule_minute = map(int, schedule_time.split(':')) if schedule_time else (0, 0)
        now = datetime.now()

        if schedule_type == 'once':
            run_time = now.replace(hour=schedule_hour, minute=schedule_minute, second=0, microsecond=0)
            if run_time < now:
                run_time += timedelta(days=1)
            scheduler.add_job(func=job_logic, trigger='date', run_date=run_time, id=job_id, kwargs=job_kwargs)
        elif schedule_type == 'hourly':
            scheduler.add_job(func=job_logic, trigger='interval', hours=1, id=job_id, kwargs=job_kwargs, next_run_time=now)
        elif schedule_type == 'daily':
            scheduler.add_job(func=job_logic, trigger='cron', hour=schedule_hour, minute=schedule_minute, id=job_id, kwargs=job_kwargs, next_run_time=now)
        else:
            return jsonify({"success": False, "error": f"Invalid schedule type: {schedule_type}"}), 400
        print("\n📋 Scheduled Jobs List:")
        print(scheduler.get_jobs())

        for job in scheduler.get_jobs():
            print(job)
            print(f"- ID: {job.id}")
            print(f"  Next Run Time: {job.next_run_time}")
            print(f"  Trigger: {job.trigger}")
            print(f"  Function: {job.func_ref}")
            print(f"  Args: {job.args}")
            print(f"  Kwargs: {job.kwargs}")
            print("-" * 50)
        if email:
            msg = f"""
Subject: 3A Vision Data Transfer Scheduled

Hello {email},

Your data transfer job has been successfully scheduled.

Details:
Source Table: {source_table_name}
Destination Table: {dest_table_name}
Schedule Type: {schedule_type}
Scheduled Time: {schedule_time if schedule_time else 'N/A'}
Job ID: {job_id}

Regards,
3A Vision Team
"""
            send_notification_email(email, "3A Vision Data Transfer Scheduled", msg)

        return jsonify({"success": True, "message": "Job scheduled successfully", "job_id": job_id})

    except Exception as e:
        error_msg = f"Failed to schedule job: {str(e)}"
        if email:
            send_notification_email(email, "Data Transfer Scheduling Failed", error_msg)
        return jsonify({"success": False, "error": error_msg}), 500


    
# from flask import Flask, request, jsonify
# from datetime import datetime, timedelta

# # from sendgrid.helpers.mail import Mail
# import os

# # app = Flask(__name__)
# # scheduler = APScheduler()
# # scheduler.init_app(app)
# # scheduler.start()

# import requests
# import requests

# # Replace this with your actual Mailgun domain (e.g., sandbox****.mailgun.org)
# MAILGUN_DOMAIN = 'sandbox0b04e19327dd4497a05f05cf2ec275ee.mailgun.org'
# MAILGUN_API_KEY = 'e71583bb-e10355bcxxxxxxxxxxxx'  # Use full correct API key
# FROM_EMAIL = f"DataApp <postmaster@{MAILGUN_DOMAIN}>"

# def send_notification_email(to_email, subject, message):
#     return requests.post(
#         f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
#         auth=("api", MAILGUN_API_KEY),
#         data={
#             "from": FROM_EMAIL,
#             "to": [to_email],
#             "subject": subject,
#             "text": message
#         }
#     )

@app.route('/get_tables', methods=['POST'])
def get_tables():
    data = request.get_json()
    db_type = data.get('dbType')
    username = data.get('username')
    password = data.get('password')
    host = data.get('host')
    port = data.get('port')
    dbName = data.get('dbName')
    tables = []

    try:
        if db_type == 'PostgreSQL':
            conn = psycopg2.connect(dbname=dbName, user=username, password=password, host=host or 'localhost', port=port or '5432')
            cur = conn.cursor()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
        elif db_type == 'MySQL':
            import mysql.connector
            conn = mysql.connector.connect(host=host or 'localhost', port=port or '3306', database=dbName, user=username, password=password)
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
        elif db_type == 'MongoDB':
            from pymongo import MongoClient
            client = MongoClient(f"mongodb://{username}:{password}@{host or 'localhost'}:{port or '27017'}/{dbName}", serverSelectionTimeoutMS=5000)
            db = client[dbName]
            tables = db.list_collection_names()
            client.close()
        elif db_type == 'Oracle':
            import cx_Oracle
            dsn_tns = cx_Oracle.makedsn(host or 'localhost', port or '1521', service_name=dbName)
            conn = cx_Oracle.connect(user=username, password=password, dsn_tns=dsn_tns)
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM user_tables")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
        else:
            return jsonify({"success": False, "error": f"Unsupported database type: {db_type}"}), 400
        return jsonify({"success": True, "tables": tables})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500




@app.route('/api/get_table_columns', methods=['POST'])
def get_table_column():
    data = request.get_json()
    source_config = data.get('source')
    source_table_name = data.get('sourceTable')

    if not source_config or not source_table_name:
        return jsonify({"success": False, "error": "Missing source configuration or source table name"}), 400

    columns, error = fetch_table_details(source_config, source_table_name)
    if error:
        return jsonify({"success": False, "error": f"Error fetching columns: {error}"}), 500
    return jsonify({"success": True, "columns": columns})
    
@app.route('/api/create-view', methods=['POST'])
def create_single_table_view():
    data = request.json
    print("Received data for view creation:", data)

    database_name = data.get('databaseName')
    original_table_name = data.get('originalTableName') # This is the selectedTable from frontend
    view_name = data.get('viewName')
    selected_columns = data.get('selectedColumns')

    # --- Input Validation ---
    if not all([database_name, original_table_name, view_name, selected_columns]):
        return jsonify({"success": False, "message": "Missing required parameters for view creation."}), 400
    if not isinstance(selected_columns, list) or not selected_columns:
        return jsonify({"success": False, "message": "Selected columns must be a non-empty list."}), 400

    # --- SQL Injection Prevention (CRITICAL!) ---
    # Database object names (table names, column names, view names) cannot be parameterized.
    # Therefore, we must rigorously sanitize them.
    # This example uses basic alphanumeric and underscore sanitization.
    # For production, consider using a library or ensuring names come from a trusted whitelist.

    def sanitize_sql_identifier(identifier):
        # Basic sanitization: allow alphanumeric and underscore.
        # Replace any other character with an underscore or remove it.
        # Check your database's specific rules for identifiers.
        return ''.join(c for c in identifier if c.isalnum() or c == '_')

    sanitized_view_name = sanitize_sql_identifier(view_name)
    sanitized_original_table_name = sanitize_sql_identifier(original_table_name)
    sanitized_columns = [sanitize_sql_identifier(col) for col in selected_columns]

    if not sanitized_view_name or not sanitized_original_table_name or not sanitized_columns:
        return jsonify({"success": False, "message": "Invalid characters found in view name, table name, or columns after sanitization. Please use only alphanumeric characters and underscores."}), 400

    # Ensure columns list is not empty after sanitization
    if not sanitized_columns:
        return jsonify({"success": False, "message": "No valid columns selected for the view after sanitization."}), 400

    # --- Construct the CREATE VIEW SQL Query ---
    # Using CREATE OR REPLACE VIEW is safer as it updates the view if it already exists,
    # preventing an error if the user tries to save a view with the same name.
    columns_list_str = ", ".join(sanitized_columns)

    # Note: If your database driver expects schema.table_name format,
    # you might use f"CREATE OR REPLACE VIEW {database_name}.{sanitized_view_name} AS ..."
    # For MySQL, it's usually just the table/view name if you are connected to the specific database.
    # If your `database_name` is truly a schema, adjust accordingly.
    
    query = (
            f'CREATE OR REPLACE VIEW "{sanitized_view_name}" AS '
            f'SELECT {columns_list_str} '
            f'FROM "{sanitized_original_table_name}";'
        )

    # For PostgreSQL, use double quotes for identifiers:
    # query = (
    #     f'CREATE OR REPLACE VIEW "{sanitized_view_name}" AS '
    #     f'SELECT {columns_list_str} '
    #     f'FROM "{sanitized_original_table_name}";'
    # )

    print("Executing query:", query)

    connection = None
    try:
        connection = connect_db(database_name)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()  # Commit to save the view in the database
        print(f"View '{view_name}' created successfully in database '{database_name}'")
        return jsonify({"success": True, "message": f"View '{view_name}' created successfully!"}), 200

    except mysql.connector.Error as err: # Specific exception for MySQL
    # except Exception as err: # Generic exception for other databases like PostgreSQL
        print("Database error executing query:", err)
        error_message = str(err)

        # You can add more specific error handling based on database error codes
        if "already exists" in error_message.lower() and "view" in error_message.lower():
            return jsonify({"success": False, "message": f"View '{view_name}' already exists. Please choose a different name."}), 409 # Conflict
        elif "access denied" in error_message.lower() or "permission denied" in error_message.lower():
             return jsonify({"success": False, "message": "Database permission denied to create view. Check user privileges."}), 403 # Forbidden
        elif "table" in error_message.lower() and "doesn't exist" in error_message.lower():
            return jsonify({"success": False, "message": f"Original table '{original_table_name}' not found in database '{database_name}'."}), 404 # Not Found
        else:
            return jsonify({"success": False, "message": f"Failed to create view: {error_message}"}), 500
    except Exception as e:
        print("An unexpected error occurred:", e)
        return jsonify({"success": False, "message": f"An unexpected server error occurred: {str(e)}"}), 500
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")

@app.route("/upload_logo", methods=["POST"])
def upload_logo():
    logo = request.files.get("logo")
    organizationName = request.form.get("organizationName")

    if not logo or not organizationName:
        return jsonify({"message": "Missing logo or organization name"}), 400

    # Get DB connection and cursor
    db_conn = get_db_connection()  # your function
    db_cursor = db_conn.cursor()

    try:
        # relative_path = save_logo(logo, organizationName, db_cursor, db_conn)
        # print("relative_path",relative_path)
        relative_path = save_logo(logo, organizationName, db_cursor, db_conn)

        url_path = f"{relative_path}"  # <-- Construct URL
        
        print("✅ Final logo URL returned to frontend:", url_path)          # <-- Print it

        return jsonify({
            "message": "Logo uploaded successfully",
            "logo_url": f"http://localhost:5000/static/{url_path}" if url_path else None
        })

        # return jsonify({
        #     "message": "Logo uploaded successfully",
        #     "logo_url": f"/{UPLOAD_ROOT}/{relative_path.replace(os.sep, '/')}"
        #     # "logo_url": f"http://localhost:5000/{UPLOAD_ROOT}/{relative_path.replace(os.sep, '/')}"

        # })

    except Exception as e:
        print("Error:", e)
        return jsonify({"message": "Error uploading logo"}), 500
    finally:
        db_cursor.close()
        db_conn.close()

import os
from werkzeug.utils import secure_filename

def save_logo(logo, organizationName, db_cursor, db_conn):
    logo_filename = None

    # 🔧 Step 1: Ensure "logo" column exists in the table
    db_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='organizationdatatest' AND column_name='logo'
    """)
    if db_cursor.fetchone() is None:
        db_cursor.execute("""ALTER TABLE organizationdatatest ADD COLUMN logo TEXT""")
        db_conn.commit()
        print("✅ 'logo' column added to organizationdatatest table.")

    if logo:
        # 📁 Step 2: Create and clean org folder
        org_folder = secure_filename(organizationName.lower().replace(" ", "_"))
        org_folder_path = os.path.join(UPLOAD_ROOT, org_folder)
        os.makedirs(org_folder_path, exist_ok=True)

        # Remove any existing logo files
        for file in os.listdir(org_folder_path):
            file_path = os.path.join(org_folder_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"🗑️ Removed old logo: {file_path}")

        # 💾 Step 3: Save new logo
        filename = secure_filename(logo.filename)
        logo_path = os.path.join(org_folder_path, filename)
        logo.save(logo_path)

        # Store relative path (for frontend)
        logo_filename = os.path.join(org_folder, filename).replace("\\", "/")

        # 📝 Step 4: Update logo path in DB
        db_cursor.execute("""
            UPDATE organizationdatatest
            SET logo = %s
            WHERE organizationname = %s
        """, (logo_filename, organizationName))
        db_conn.commit()

    return logo_filename



# @app.route('/api/calculation-suggestions', methods=['GET'])
# def get_calculation_suggestions():
#     try:
#         conn = psycopg2.connect(
#             dbname=DB_NAME, user=USER_NAME, password=PASSWORD,
#             host=HOST, port=PORT
#         )
#         cursor = conn.cursor()
#         cursor.execute("SELECT keyword, template FROM calculation_suggestions ORDER BY keyword;")
#         suggestions = [{'keyword': row[0], 'template': row[1]} for row in cursor.fetchall()]
#         cursor.close()
#         conn.close()
#         return jsonify(suggestions)
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500
    
@app.route('/api/calculation-suggestions', methods=['GET'])
def get_calculation_suggestions():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=USER_NAME, password=PASSWORD,
            host=HOST, port=PORT
        )
        cursor = conn.cursor()

        # Step 1: Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calculation_suggestions (
                id SERIAL PRIMARY KEY,
                keyword VARCHAR(50) UNIQUE NOT NULL,
                template TEXT NOT NULL,
                category VARCHAR(50) DEFAULT 'default'
            );
        """)

        # Step 2: Insert default suggestions using ON CONFLICT DO NOTHING
        cursor.execute("""
            INSERT INTO calculation_suggestions (keyword, template, category) VALUES
            ('if', 'if ([column] == "value") then "Result1" else "Result2"', 'default'),
            ('switch', 'switch([column], "A", "X", "B", "Y", default, "Z")', 'default'),
            ('iferror', 'iferror([numerator] / [denominator], 0)', 'default'),
            ('calculate', 'calculate(sum([sales]), [region]="Asia")', 'default'),
            ('sum', 'sum([column])', 'default'),
            ('avg', 'avg([column])', 'default'),
            ('count', 'count([column])', 'default'),
            ('min', 'min([column])', 'default'),
            ('max', 'max([column])', 'default'),
            ('abs', 'abs([column])', 'default'),
            ('maxx', 'maxx([column])', 'default'),
            ('minx', 'minx([column])', 'default'),
            ('len', 'len([column])', 'default'),
            ('lower', 'lower([column])', 'default'),
            ('upper', 'upper([column])', 'default'),
            ('concat', 'concat([first_name], " ", [last_name])', 'default'),
            ('round', 'round([column], 2)', 'default'),
            ('isnull', 'isnull([column], "default value")', 'default'),
            ('year', 'year([date_column])', 'date'),
            ('month', 'month([date_column])', 'date'),
            ('day', 'day([date_column])', 'date'),
            ('in', '[region] in ("Asia", "Europe")', 'default'),
            ('datediff', 'datediff([end_date], [start_date])', 'date'),
            ('today', 'today()', 'date'),
            ('now', 'now()', 'date'),
            ('dateadd', 'dateadd([date_column], 7, "day")', 'date'),
            ('formatdate', 'formatdate([date_column], "YYYY-MM-DD")', 'date'),
            ('replace', 'replace([column], "old", "new")', 'text'),
            ('trim', 'trim([column])', 'text')
            ON CONFLICT (keyword) DO NOTHING;
        """)

        # Step 3: Fetch all suggestions
        cursor.execute("SELECT keyword, template FROM calculation_suggestions ORDER BY keyword;")
        suggestions = [{'keyword': row[0], 'template': row[1]} for row in cursor.fetchall()]

        conn.commit()  # Commit changes
        cursor.close()
        conn.close()

        return jsonify(suggestions)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_role_permissions/<role_code>/<company>', methods=['GET'])
def get_role_permissions(role_code,company):
    
    if not role_code or not company:
        return jsonify({'error': 'Missing role or company'}), 400

    try:
        conn = get_company_db_connection(company)
        cur = conn.cursor()
        print("cur",cur)

        cur.execute("""
            SELECT can_datasource, can_view, can_edit, can_design, can_load, can_update, can_edit_profile
            FROM role_permission
            WHERE role_permission_code = %s
        """, (role_code,))
        
        row = cur.fetchone()
        print("row",row)
        if not row:
            return jsonify({'error': 'Role not found'}), 404

        permissions = {
            'can_datasource': row[0],
            'can_view': row[1],
            'can_edit': row[2],
            'can_design': row[3],
            'can_load': row[4],
            'can_update': row[5],
            'can_edit_profile': row[6]
        }
        return jsonify(permissions)
    

    except Exception as e:
        return jsonify({'error': str(e)}), 500
# Example Flask endpoint
@app.route('/delete_table', methods=['POST'])
def delete_table():
    data = request.json
    database = data.get('database')
    table = data.get('table')
    # company_name = data.get('company')

    if not all([database, table]):
        return jsonify({'status': 'error', 'message': 'Missing required fields'}), 400

    try:
        # Connect to the company-specific database
        conn = psycopg2.connect(
            dbname=database,
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cur = conn.cursor()

        # Connect to global DB where table_chart_save resides
        con = psycopg2.connect(
            dbname=DB_NAME,
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = con.cursor()

        # Step 1: Check if table is used in chart creation
        check_query = """
            SELECT 1 FROM table_chart_save
            WHERE company_name = %s AND selected_table::text ILIKE %s
            LIMIT 1;
        """
        cursor.execute(check_query, (database, f'%{table}%'))
        if cursor.fetchone():  # ✅ FIXED: now correctly using `cursor`
            cursor.close()
            con.close()
            cur.close()
            conn.close()
            return jsonify({
                'status': 'error',
                'message': f'Table "{table}" is currently used in chart creation and cannot be deleted.'
            }), 400

        # Step 2: Proceed to delete the table
        cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
        conn.commit()

        # Close all cursors and connections
        cur.close()
        conn.close()
        cursor.close()
        con.close()

        return jsonify({'status': 'success', 'message': f'Table "{table}" deleted successfully'})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
        


# @app.route('/api/get_all_users', methods=['GET'])
# def get_all_user_id():
#     company_name = request.args.get('company_name')

#     if not company_name:
#         return jsonify({'message': 'company_name is required'}), 400

#     # Connect to the specific company's database
#     conn = get_company_db_connection(company_name)
#     if not conn:
#         print(f"Failed to connect to company database for {company_name}.")
#         return jsonify({'message': 'Failed to connect to company database'}), 500

#     try:
#         cursor = conn.cursor()
#         cursor.execute("SELECT employee_id, employee_name FROM employee_list")
#         rows = cursor.fetchall()
#         cursor.close()

#         # Convert the result to a list of dictionaries
#         users = [{'employee_id': row[0], 'employee_name': row[1]} for row in rows]

#         return jsonify(users), 200

#     except Exception as e:
#         print(f"Error fetching users: {e}")
#         return jsonify({'message': 'Error fetching users'}), 500

#     finally:
#         conn.close()


@app.route('/api/get_all_users', methods=['GET'])
def get_all_user_id():
    company_name = request.args.get('company_name')
    user_id = request.args.get('user_id')

    if not company_name or not user_id:
        return jsonify({'message': 'company_name and user_id are required'}), 400

    # Connect to the specific company's database
    conn = get_company_db_connection(company_name)
    if not conn:
        print(f"Failed to connect to company database for {company_name}.")
        return jsonify({'message': 'Failed to connect to company database'}), 500

    try:
        cursor = conn.cursor()

        # Get the reporting_id for this user
        cursor.execute("""
            SELECT reporting_id
            FROM employee_list
            WHERE employee_id = %s
        """, (user_id,))
        reporting_row = cursor.fetchone()
        reporting_id = reporting_row[0] if reporting_row else None
        print("reporting_id",reporting_id)

        # Fetch all employees excluding current user and their reporting manager
        if reporting_id:
            cursor.execute("""
                SELECT employee_id, employee_name
                FROM employee_list
                WHERE employee_id != %s
                  AND employee_id != %s
            """, (user_id, reporting_id))
        else:
            cursor.execute("""
                SELECT employee_id, employee_name
                FROM employee_list
                WHERE employee_id != %s
            """, (user_id,))

        rows = cursor.fetchall()
        cursor.close()

        # Convert to a list of dictionaries
        users = [{'employee_id': row[0], 'employee_name': row[1]} for row in rows]

        return jsonify(users), 200

    except Exception as e:
        print(f"Error fetching users: {e}")
        return jsonify({'message': 'Error fetching users'}), 500

    finally:
        conn.close()

def safe_json(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value

@app.route('/api/share_dashboard', methods=['POST'])
def share_dashboard():
    conn = None 
    try:
        data = request.get_json()
        print("Received Data:", data)
        # user_id, dashboard_name = dashboard_name.split(",", 1)  # Split only once
        # Support both field names
        dashboard_name_data = data.get("dashboard_name")

        if isinstance(dashboard_name_data, list):
            # Expect [id, name]
            if len(dashboard_name_data) == 2:
                user_id, dashboard_name = dashboard_name_data
            else:
                return jsonify({"message": "Invalid dashboard_name format"}), 400
        elif isinstance(dashboard_name_data, str):
            user_id, dashboard_name = dashboard_name_data.split(",", 1)
        else:
            return jsonify({"message": "dashboard_name must be string or list"}), 400

        to_user_id = data.get("to_user_id") or data.get("to_username")
        # dashboard_name = data.get("dashboard_name")
        from_user = data.get("from_user")  # original user ID (who is sharing)
        company_name = data.get("company_name")
        print("user_id",user_id)
        print("dashboard_name",dashboard_name)
        if not all([to_user_id, dashboard_name, from_user, company_name]):
            print("Missing one or more required fields.")
            return jsonify({"message": "Missing required fields"}), 400

        conn = get_db_connection()
        if not conn:
            print("Database connection failed.")
            return jsonify({"message": "Failed to connect to database"}), 500

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Step 1: Get original dashboard
        cursor.execute("""
            SELECT * FROM table_dashboard 
            WHERE user_id = %s AND file_name = %s AND company_name = %s
        """, (user_id, dashboard_name, company_name))
        original_dashboard = cursor.fetchone()

        if not original_dashboard:
            print("Original dashboard not found.")
            return jsonify({"message": "Original dashboard not found"}), 404

        print("Fetched original dashboard:", original_dashboard)
        raw_ids = original_dashboard['chart_ids']
        original_chart_ids = [int(id.strip()) for id in raw_ids.strip('{}').split(',') if id.strip()]

        # original_chart_ids = eval(original_dashboard['chart_ids'])  # assuming stored as list
        if not isinstance(original_chart_ids, list):
            print("chart_ids is not a list.")
            return jsonify({"message": "chart_ids should be a list"}), 400

        print("Original chart IDs:", original_chart_ids)

        new_chart_ids = []

        # Step 2: Copy each chart from table_chart_save
        # for old_chart_id in original_chart_ids:
        #     cursor.execute("""
        #         SELECT * FROM table_chart_save 
        #         WHERE id = %s 
        #     """, (old_chart_id, user_id))
        #     old_chart = cursor.fetchone()
        for old_chart_id in original_chart_ids:
            cursor.execute("""
                SELECT * FROM table_chart_save 
                WHERE id = %s
            """, (old_chart_id,))
            old_chart = cursor.fetchone()

            if not old_chart:
                print(f"Chart not found for ID: {old_chart_id}")
                continue

            # Ensure unique chart_id
            cursor.execute("SELECT MAX(id) FROM table_chart_save")
            last_chart_id = cursor.fetchone()['max'] or 0
            new_chart_id = last_chart_id + 1
            new_chart_ids.append(new_chart_id)
            print("new_chart_ids",new_chart_ids)
            cursor.execute("""
                INSERT INTO table_chart_save (
                    id, user_id, company_name, chart_name, timestamp, database_name,
                    selected_table, x_axis, y_axis, aggregate, chart_type, chart_color, chart_heading,
                    drilldown_chart_color, filter_options, ai_chart_data, selectedUser,
                    xFontSize, fontStyle, categoryColor, yFontSize, valueColor, headingColor,
                    ClickedTool, Bgcolour, OptimizationData, calculationData, selectedFrequency
                ) VALUES (
                    %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                new_chart_id,
                to_user_id,
                old_chart["company_name"],
                old_chart["chart_name"],
                old_chart["database_name"],
                old_chart["selected_table"],
                old_chart["x_axis"],
                old_chart["y_axis"],
                old_chart["aggregate"],
                old_chart["chart_type"],
                old_chart["chart_color"],
                old_chart["chart_heading"],
                old_chart["drilldown_chart_color"],
                json.dumps(old_chart["filter_options"]) if isinstance(old_chart["filter_options"], dict) else old_chart["filter_options"],
                json.dumps(old_chart["ai_chart_data"]) if isinstance(old_chart["ai_chart_data"], dict) else old_chart["ai_chart_data"],
                old_chart.get("selectedUser"),
                old_chart.get("xFontSize"),
                old_chart.get("fontStyle"),
                old_chart.get("categoryColor"),
                old_chart.get("yFontSize"),
                old_chart.get("valueColor"),
                old_chart.get("headingColor"),
                old_chart.get("ClickedTool"),
                old_chart.get("Bgcolour"),
                json.dumps(old_chart.get("OptimizationData")) if isinstance(old_chart.get("OptimizationData"), dict) else old_chart.get("OptimizationData"),
                json.dumps(old_chart.get("calculationData")) if isinstance(old_chart.get("calculationData"), dict) else old_chart.get("calculationData"),
                old_chart.get("selectedFrequency")

            ))
            print(f"Copied chart {old_chart_id} to new chart {new_chart_id}")

        print("All new chart IDs:", new_chart_ids)

        # Step 3: Copy dashboard and insert under new user
        # new_dashboard_id = str(uuid.uuid4())
        # Ensure unique dashboard_id
               
        cursor.execute("SELECT MAX(id) FROM table_dashboard")
        last_dashboard_id = cursor.fetchone()['max'] or 0
        new_dashboard_id = last_dashboard_id + 1

        # Get the original dashboard values
        original_dashboard_values = {
            "company_name": original_dashboard.get("company_name"),
            "file_name": original_dashboard.get("file_name"),
            "position": safe_json(original_dashboard.get("position")),
            "chart_size": safe_json(original_dashboard.get("chart_size")),
            "chart_type": safe_json(original_dashboard.get("chart_type")),
            "chart_xaxis": safe_json(original_dashboard.get("chart_xaxis")),
            "chart_yaxis": safe_json(original_dashboard.get("chart_yaxis")),
            "chart_aggregate": safe_json(original_dashboard.get("chart_aggregate")),
            "filterdata": safe_json(original_dashboard.get("filterdata")),
            "clicked_category": original_dashboard.get("clicked_category"),
            "heading": original_dashboard.get("heading"),
            "dashboard_name": original_dashboard.get("dashboard_name"),
            "chartcolor": safe_json(original_dashboard.get("chartcolor")),
            "droppablebgcolor": original_dashboard.get("droppablebgcolor"),
            "opacity": safe_json(original_dashboard.get("opacity")),
            "image_ids": safe_json(original_dashboard.get("image_ids", [])),
            "project_name": original_dashboard.get("project_name"),
            "font_style_state": original_dashboard.get("font_style_state"),
            "font_size": original_dashboard.get("font_size"),
            "font_color": original_dashboard.get("font_color"),
        }


        # The chart_ids field needs special handling for PostgreSQL array format
        chart_ids_str = f"{{{','.join(map(str, new_chart_ids))}}}"


        cursor.execute("""
            INSERT INTO table_dashboard (
                id, user_id, company_name, file_name, chart_ids, position, chart_size,
                chart_type, chart_xaxis, chart_yaxis, chart_aggregate, filterdata,
                clicked_category, heading, dashboard_name, chartcolor,
                droppablebgcolor, opacity, image_ids, project_name,
                font_style_state, font_size, font_color
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            new_dashboard_id,
            to_user_id,
            original_dashboard_values["company_name"],
            original_dashboard_values["file_name"],
            chart_ids_str,
            original_dashboard_values["position"],
            original_dashboard_values["chart_size"],
            original_dashboard_values["chart_type"],
            original_dashboard_values["chart_xaxis"],
            original_dashboard_values["chart_yaxis"],
            original_dashboard_values["chart_aggregate"],
            original_dashboard_values["filterdata"],
            original_dashboard_values["clicked_category"],
            original_dashboard_values["heading"],
            original_dashboard_values["dashboard_name"],
            original_dashboard_values["chartcolor"],
            original_dashboard_values["droppablebgcolor"],
            original_dashboard_values["opacity"],
            original_dashboard_values["image_ids"],
            original_dashboard_values["project_name"],
            original_dashboard_values["font_style_state"],
            original_dashboard_values["font_size"],
            original_dashboard_values["font_color"]
        ))



        print("Dashboard copied with new ID:", new_dashboard_id)

        conn.commit()
        return jsonify({"message": "Dashboard shared successfully!"}), 200

    except Exception as e:
        print(f"Error in share_dashboard: {e}")
        return jsonify({"message": "Error sharing dashboard"}), 500

    finally:
        if conn:
            conn.close()

def send_reset_email(recipient, subject, body):
    try:
        print("Using Hostinger SMTP to send email...")

        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"] = HOTMAIL_USER
        msg["To"] = recipient

        with smtplib.SMTP("smtp.hostinger.com", 587) as server:
            server.starttls()
            server.login(HOTMAIL_USER, HOTMAIL_PASS)
            server.sendmail(HOTMAIL_USER, recipient, msg.as_string())

        print("Email sent successfully")
    except Exception as e:
        print("SMTP error:", e)

# ---------------- CREATE RESET TABLE ----------------
def ensure_password_reset_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS password_resets (
            email VARCHAR(255),
            company VARCHAR(255),
            token TEXT,
            used BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
from datetime import datetime, timedelta

# ---------------- REQUEST RESET ----------------
@app.route('/api/request_password_reset', methods=['POST'])
def request_password_reset():
    try:
        data = request.json
        company_name = data.get("company")
        email = data.get("email")
        reset_type = data.get("type")  # employee / company

        if not company_name:
            return jsonify({"message": "Company name required"}), 400

        conn = get_db_connection()
        ensure_password_reset_table(conn)  # ✅ ensure table exists
        cur = conn.cursor()

        if reset_type == "employee":
            conn1 = get_company_db_connection(company_name)
            cur1 = conn1.cursor()
            cur1.execute("SELECT * FROM employee_list WHERE email = %s", (email,))
            user = cur1.fetchone()
            conn1.close()
            if not user:
                return jsonify({"message": "Employee not found"}), 404
        else:
            cur.execute("SELECT * FROM organizationdatatest WHERE organizationname = %s", (company_name,))
            user = cur.fetchone()
            if not user:
                return jsonify({"message": "Company not found"}), 404

        # ✅ Generate token valid for 5 minutes
        token = jwt.encode({
            "email": email,
            "company": company_name,
            "type": reset_type,
            "exp": datetime.utcnow() + timedelta(minutes=5)
        }, SECRET_KEY, algorithm="HS256")


        reset_url = f"http://localhost:3000/reset-password?token={token}"

        # ✅ Store token in DB
        cur.execute(
            "INSERT INTO password_resets (email, company, token, used) VALUES (%s, %s, %s, %s)",
            (email, company_name, token, False)
        )
        conn.commit()

        # ✅ Send email
        send_reset_email(
            recipient=email,
            subject="Password Reset Request",
            body=f"Click the link to reset your password (valid 5 min, one-time use): {reset_url}"
        )

        conn.close()
        return jsonify({"message": "Password reset link sent"}), 200

    except Exception as e:
        print("Error:", e)
        return jsonify({"message": "Internal server error"}), 500

# ---------------- RESET PASSWORD ----------------
@app.route('/api/reset_password', methods=['POST'])
def reset_password():
    try:
        data = request.json
        token = data.get("token")
        new_password = data.get("new_password")

        if not token or not new_password:
            return jsonify({"message": "Token and new password required"}), 400

        decoded = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        email = decoded.get("email")
        company = decoded.get("company")
        reset_type = decoded.get("type")

        conn = get_db_connection()
        ensure_password_reset_table(conn)  # ✅ ensure table exists
        cur = conn.cursor()

        # ✅ Check token validity
        cur.execute("SELECT used FROM password_resets WHERE token = %s", (token,))
        row = cur.fetchone()
        if not row:
            return jsonify({"message": "Invalid reset token"}), 400
        if row[0]:
            return jsonify({"message": "Reset link already used"}), 400

        # ✅ Hash new password
        hashed_pw = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())

        if reset_type == "employee":
            conn1 = get_company_db_connection(company)
            cur1 = conn1.cursor()
            cur1.execute("UPDATE employee_list SET password = %s WHERE email = %s", (hashed_pw, email))
            conn1.commit()
            conn1.close()
        else:
            cur.execute("UPDATE organizationdatatest SET password = %s WHERE organizationname = %s",
                        (hashed_pw, company))

        # ✅ Mark token as used
        cur.execute("UPDATE password_resets SET used = TRUE WHERE token = %s", (token,))
        conn.commit()
        conn.close()

        return jsonify({"message": "Password reset successful"}), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"message": "Reset link expired"}), 400
    except Exception as e:
        print("Error in reset_password:", str(e))
        return jsonify({"message": "Invalid or expired token"}), 400
# @app.route('/static/<path:filename>')
# def serve_static(filename):
#     return send_file(os.path.join('static', filename))
@app.route('/static/<path:filename>')
def serve_static(filename):
    # send_file will automatically look within the configured static_folder
    return send_file(os.path.join(app.static_folder, filename))

if __name__ == "__main__":
    # Use socketio.run to enable WebSocket support
    # socketio.run(app, debug=True, host='0.0.0.0', port=5000)
     socketio.run(app, host='0.0.0.0', port=5000, debug=True)
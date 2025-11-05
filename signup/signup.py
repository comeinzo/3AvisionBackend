from flask import Flask, request, jsonify
import psycopg2
import bcrypt
import logging

import binascii
from config import PASSWORD, USER_NAME, HOST, PORT,DB_NAME

def get_db_connection(dbname=DB_NAME):
    conn = psycopg2.connect(
        dbname=dbname,
        # user="postgres",
        # password="Gayu@123",
        # host="localhost",
        # port="5432"
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
        
    )
    return conn


def connect_db(company):
    try:
        conn = psycopg2.connect(
            dbname=company,
            # user="postgres",
            # password="Gayu@123",
            # host="localhost",
            # port="5432" 
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None



def create_database(organizationName):
    try:
        conn = get_db_connection(dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        logging.info(f"Creating database: {organizationName}")
        cursor.execute(f"CREATE DATABASE {organizationName}")
    except Exception as e:
        logging.error(f"Error creating database {organizationName}: {str(e)}")
        raise e
    finally:
        cursor.close()
        conn.close()



def create_table_if_not_exists(cursor):
    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS organizationdatatest (
        id SERIAL PRIMARY KEY,
        organizationName VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        userName VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL
    );
    """)

    # Check if 'logo' column exists
    cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'organizationdatatest' AND column_name = 'logo';
    """)
    column_exists = cursor.fetchone()

    # If not, add the column
    if not column_exists:
        cursor.execute("""
        ALTER TABLE organizationdatatest ADD COLUMN logo VARCHAR(255);
        """)

def insert_user_data(organizationName, email, userName, password,logo_filename):
    try:
        create_database(organizationName)  # Assuming this creates the database if needed
        conn = get_db_connection()
        cursor = conn.cursor()

        # Create table if it does not exist
        create_table_if_not_exists(cursor)
        print("organizationName",organizationName)
        organizationName = organizationName.lower() 
        # Insert data into table
        cursor.execute(
            """
            INSERT INTO organizationdatatest (organizationName, email, userName, password,logo)
            VALUES (%s, %s, %s, %s,%s)
            """,
            (organizationName, email, userName, password,logo_filename)
        )
        conn.commit()
        logging.info(f"User data inserted for {organizationName}")

    except Exception as e:
        logging.error(f"Error inserting user data: {str(e)}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()



def fetch_usersdata():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """

             SELECT * FROM organizationdatatest
            """
        )
                    # SELECT userName FROM organizationdatatest
        data = cursor.fetchall()
        return data
    except Exception as e:
        raise e
    finally:
        cursor.close()
        conn.close()




def fetch_login_data(email, password):
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_if_not_exists(cursor)

    # SQL query to check if email and password match
    cursor.execute("SELECT * FROM organizationdatatest WHERE email = %s AND password = %s", (email, password))
    user = cursor.fetchone()

    logo_path = None
    company_name = None
    if user and user[5]:  # Assuming user[5] is the column containing logo path
        logo_path = user[5].replace("\\", "/")  # Normalize path for URL
    if user and user[1]:  # Assuming user[5] is the column containing logo path
        company_name = user[1]
       

    cursor.close()
    conn.close()

    return {
        "user": user,
        "logo_url": f"http://localhost:5000/static/{logo_path}" if logo_path else None,
        "company_name": company_name
    }




def fetch_company_login_data(email, password, company):
    conn = connect_db(company)
    cursor = conn.cursor()
    conne=get_db_connection()
    cursor1 = conne.cursor()
    role_conn = None  # Initialize role_conn outside the try block
    role_cursor = None # Initialize role_cursor outside the try block

    # First query to fetch the user details (except password)
    cursor.execute("SELECT employee_id, employee_name, role_id, email FROM employee_list WHERE email = %s", (email,))
    user = cursor.fetchone()
    print(user)
    # role_cursor.execute("SELECT role_name FROM role_table WHERE role_id = %s", (role_id,))
    # role_data = role_cursor.fetchone()
    
    if user:
        # Second query to fetch the hashed password separately
        cursor.execute("SELECT password FROM employee_list WHERE email = %s", (email,))
        hashed_password_row = cursor.fetchone()
        print(hashed_password_row)
        if hashed_password_row:
            stored_hash_with_hex = hashed_password_row[0]  # Get the password from the result
            stored_hash_bytes = binascii.unhexlify(stored_hash_with_hex.replace('\\x', ''))
            print("------------------------------------",stored_hash_bytes)    
            # Check if the password matches the hashed password
            if bcrypt.checkpw(password.encode('utf-8'), stored_hash_bytes):
                print("Password match!")
                try:
                    cursor.execute("SELECT permissions FROM role WHERE role_id = %s", (user[2],))
                    role_data = cursor.fetchone()
                    permissions = role_data[0] if role_data else None  # Handle missing role

                    # Check if 'logo' column exists
                    cursor1.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'organizationdatatest' AND column_name = 'logo';
                    """)
                    logo_column_exists = cursor1.fetchone()

                    # Add the column if not exists
                    if not logo_column_exists:
                        cursor1.execute("""
                            ALTER TABLE organizationdatatest ADD COLUMN logo VARCHAR(255);
                        """)
                        conne.commit()

                    cursor1.execute("SELECT logo FROM organizationdatatest WHERE organizationname = %s", (company,))
                    logo_row = cursor1.fetchone()
                    logo_path = logo_row[0] if logo_row else None

                    # Get all table names except system tables
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name NOT IN ('employee_list', 'datasource')
                    """)
                    tables = cursor.fetchall()

                    # Close resources after all queries are done
                    cursor.close()
                    conn.close()
                    cursor1.close()
                    conne.close()

                    return {
                        "user": user,
                        "permissions": permissions,
                        "tables": tables,
                        "logo_url": f"http://localhost:5000/static/{logo_path}" if logo_path else None
                    }

                except Exception as e:
                    print("Error during role/logo/tables fetch:", e)
                    cursor.close()
                    conn.close()
                    cursor1.close()
                    conne.close()
                    return None


def fetch_company_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # SQL query to check if email and password match
    cursor.execute("SELECT organizationName from organizationdatatest")
    user = cursor.fetchone()

    cursor.close()
    conn.close()    
    return user

def fetch_role_id_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # SQL query to check if email and password match
    cursor.execute("SELECT role_id from role")
    user = cursor.fetchone()

    cursor.close()
    conn.close()    
    return user




def create_user_table(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employee_list (
                    employee_id SERIAL PRIMARY KEY,
                    employee_name VARCHAR(255),
                    role_id VARCHAR(255),
                    username varchar(255),     
                    email VARCHAR(255),
                    password VARCHAR(255),
                    category varchar(255),
                    action_type varchar(255), 
                    action_by varchar(255)
                    
                );
            """)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")





def encrypt_password(plain_password):
    """
    Encrypts a plain text password using bcrypt and returns the hashed password.
    """
    hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt())
    return hashed_password
def create_category_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL,
            company_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES organizationdatatest(id) ON DELETE CASCADE
        );
    """)
def create_user_table(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employee_list (
                    employee_id SERIAL PRIMARY KEY,
                    employee_name VARCHAR(255),
                    role_id VARCHAR(255),
                    username VARCHAR(255),
                    email VARCHAR(255),
                    password VARCHAR(255),
                    category VARCHAR(255),
                    action_type VARCHAR(255),
                    action_by VARCHAR(255),
                    reporting_id INTEGER
                );
            """)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")
def create_user_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "user" (
            id SERIAL PRIMARY KEY, 
            company_id INT NOT NULL,  -- Changed the column order to include company_id first
            user_id INT NOT NULL, 
            role_id INT NOT NULL, 
            category_id INT NOT NULL, 
            FOREIGN KEY (role_id) REFERENCES role(role_id),
            FOREIGN KEY (company_id) REFERENCES organizationdatatest(id),
            FOREIGN KEY (category_id) REFERENCES category(category_id)
        );
    """)



from flask import jsonify
import psycopg2
import bcrypt
from signup.signup import encrypt_password
from config import PASSWORD, USER_NAME, HOST, PORT

# Connect to a database (defaults to datasource)
def get_db_connection(dbname="datasource"):
    return psycopg2.connect(
        dbname=dbname,
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )

# Get a connection to the company's database
def get_company_db_connection(company_name):
    return psycopg2.connect(
        dbname=company_name,
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )

# ==================== MANUAL REGISTRATION ====================

def handle_manual_registration(user_details, company):
    conn_datasource = get_db_connection()
    if not conn_datasource:
        return jsonify({'message': 'Failed to connect to datasource database'}), 500

    try:
        # Extract user details
        employee_name = user_details.get("employeeName")
        role_name = user_details.get("roleId")
        organization_name = user_details.get("company")
        username = user_details.get("userName")
        email = user_details.get("email")
        password = user_details.get("password")
        retype_password = user_details.get("retypePassword")
        categories = user_details.get("categories", [])
        reporting_id = user_details.get("reportingId") or None

        if password != retype_password:
            return jsonify({'message': 'Passwords do not match'}), 400

        # Fetch role_id from datasource
        

        # Connect to company database
        conn = get_company_db_connection(company)
        if not conn:
            return jsonify({'message': f'Failed to connect to company database for {company}'}), 500
        role_id = fetch_role_id(conn, role_name)
        # Create necessary tables in company database
        create_user_table(conn)
        create_category_table_if_not_exists(conn)
        create_user_table_if_not_exists(conn)

        with conn.cursor() as cursor:
            if check_username_exists(cursor, username):
                return jsonify({'message': 'Username already exists in employee_list'}), 400

            hashed_password = encrypt_password(password)
            action_type, action_by = "add", "admin"
            employee_id = insert_user(cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

            # Save categories and user-permission in company DB
            handle_categories(conn, conn_datasource, employee_id, role_id, company, categories)

        conn.commit()
        conn_datasource.close()
        return jsonify({'message': 'User and categories created successfully'}), 200

    except Exception as e:
        print(f"Error during manual registration: {e}")
        return jsonify({'error': str(e)}), 500

    finally:
        conn_datasource.close()

# ==================== FILE UPLOAD REGISTRATION ====================

def handle_file_upload_registration(user_details, company):
    conn_datasource = get_db_connection()
    if not conn_datasource:
        return jsonify({'message': 'Failed to connect to datasource database'}), 500

    action_type, action_by = "add", "admin"

    processed_usernames = set()

    try:
        for user in user_details:
            try:
                employee_name = user.get("Employee Name")
                role_name = user.get("Role Name")
                organization_name = company
                username = user.get("Username")
                password = user.get("Password")
                email = user.get("Email")
                categories = user.get("Categories")
                reporting_id = user.get("Reporting ID") or None

                if not all([employee_name, role_name, username, categories]):
                    print(f"Skipping user due to missing required details: {user}")
                    continue

                if username in processed_usernames:
                    print(f"Skipping duplicate username: {username}")
                    continue
                processed_usernames.add(username)

                category_list = [c.strip() for c in categories.split(",")]
                print(f"Inserting user: {username} - Categories: {category_list}")

                conn = get_company_db_connection(organization_name)
                if not conn:
                    print(f"Failed to connect to company DB for {organization_name}")
                    continue

                role_id = fetch_role_id(conn, role_name)
                create_user_table(conn)
                create_category_table_if_not_exists(conn)
                create_user_table_if_not_exists(conn)

                with conn.cursor() as cursor:
                    if check_username_exists(cursor, username):
                        print(f"Username already exists: {username}")
                        continue

                    hashed_password = encrypt_password(password)
                    employee_id = insert_user(cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)
                    print(f"Inserted user ID: {employee_id}")

                    handle_categories(conn, conn_datasource, employee_id, role_id, organization_name, category_list)

                conn.commit()

            except Exception as user_error:
                print(f"Skipping user due to error: {user} | Error: {user_error}")
                continue

        return jsonify({'message': 'File upload processed successfully'}), 200

    except Exception as e:
        print(f"Error during file upload registration: {e}")
        return jsonify({'message': 'Error during file upload registration'}), 500

    finally:
        conn_datasource.close()



def fetch_role_id(conn, role_name):
    with conn.cursor() as cursor:
        cursor.execute("SELECT role_id FROM role WHERE LOWER(role_name) = LOWER(%s)", (role_name,))
        role_data = cursor.fetchone()
        if not role_data:
            raise ValueError(f"Role not found for role name: {role_name}")
        return role_data[0]


def check_username_exists(cursor, username):
    cursor.execute("SELECT COUNT(*) FROM employee_list WHERE username = %s", (username,))
    result = cursor.fetchone()
    return result and result[0] > 0

def insert_user(cursor, employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id=None):
    print("insert")
    cursor.execute("""
        INSERT INTO employee_list (employee_name, role_id, username, email, password, category, action_type, action_by, reporting_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id))
    cursor.execute("SELECT currval(pg_get_serial_sequence('employee_list', 'employee_id'))")
    return cursor.fetchone()[0]


def handle_categories(conn, conn_datasource, employee_id, role_id, company, categories):
    with conn_datasource.cursor() as source_cursor:
        source_cursor.execute("""
            SELECT id FROM organizationdatatest WHERE organizationname = %s
        """, (company,))
        org_result = source_cursor.fetchone()
        if not org_result:
            raise ValueError(f"Company '{company}' not found in organizationdatatest.")
        company_id = org_result[0]
        print("company_id",company_id)

    with conn.cursor() as cursor:
        for category in categories:
            # Check if category already exists
            cursor.execute("""
                SELECT category_id FROM category 
                WHERE LOWER(category_name) = LOWER(%s) AND company_id = %s
            """, (category.lower(), company_id))
            result = cursor.fetchone()

            if result:
                category_id = result[0]  # Use existing category_id
                print("category_id",category_id)
            else:
                # Insert new category
                cursor.execute("""
                    INSERT INTO category (category_name, company_id)
                    VALUES (%s, %s)
                    RETURNING category_id;
                """, (category, company_id))
                category_id = cursor.fetchone()[0]

            # Insert into user table
            cursor.execute("""
                INSERT INTO "user" (company_id, user_id, role_id, category_id)
                VALUES (%s, %s, %s, %s)
            """, (company_id, employee_id, role_id, category_id))


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
        print(f"Error creating employee_list table: {e}")

def create_category_table_if_not_exists(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS category (
                category_id SERIAL PRIMARY KEY,
                category_name VARCHAR(255) NOT NULL,
                company_id INT NOT NULL
                
            );
        """)
    conn.commit()

def create_user_table_if_not_exists(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS "user" (
                id SERIAL PRIMARY KEY, 
                company_id INT NOT NULL,
                user_id INT NOT NULL, 
                role_id INT NOT NULL, 
                category_id INT NOT NULL, 
                FOREIGN KEY (role_id) REFERENCES role(role_id),
                FOREIGN KEY (category_id) REFERENCES category(category_id)
            );
        """)
    conn.commit()

def encrypt_password(plain_password):
    return bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt())

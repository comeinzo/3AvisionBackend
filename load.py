
import pandas as pd
from psycopg2.extras import RealDictCursor
import hashlib
import psycopg2
from config import ALLOWED_EXTENSIONS, DB_NAME, USER_NAME, PASSWORD, HOST, PORT

# Global cache storage
GLOBAL_CACHE = {}

def generate_cache_key(company_name, table_name, date_column, start_date, end_date, selected_columns, column_conditions):
    """Generate a unique cache key based on query parameters including selected columns and conditions"""
    columns_str = ','.join(sorted(selected_columns)) if selected_columns else 'all'
    conditions_str = str(sorted(column_conditions)) if column_conditions else 'none'
    key_data = f"{company_name}_{table_name}_{date_column}_{start_date}_{end_date}_{columns_str}_{conditions_str}"
    return hashlib.md5(key_data.encode()).hexdigest()


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
def get_db_connection(db_name, selected_user=None):
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
    if not db_name:
        raise ValueError("Database name is missing")

    if not selected_user or selected_user.lower() == 'null':
        print("Using default local database connection...")
        connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
        connection = psycopg2.connect(connection_string)
    else:  # External connection
        connection_details = fetch_external_db_connection(db_name, selected_user)
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

def get_company_db_connection(company_name):
    print("company_name",company_name)
    # This is where you define the connection string
    conn = psycopg2.connect(
        dbname=company_name,  # Ensure this is the correct company database
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

def get_table_columns_with_types(company_name, table_name):
    """
    Get all column names and their data types from a table
    
    Args:
        company_name (str): Name of the company/database
        table_name (str): Name of the table
    
    Returns:
        dict: Dictionary with column names as keys and data types as values
    """
    try:
        connection = get_company_db_connection(company_name)
        cursor = connection.cursor()

        # Query to get all column names and types from the table
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.close()
        connection.close()
        
        return columns

    except Exception as e:
        print(f"Error fetching table columns with types: {e}")
        raise

def get_table_columns_with_typesdb(company_name, table_name,selected_user=None):
    """
    Get all column names and their data types from a table
    
    Args:
        company_name (str): Name of the company/database
        table_name (str): Name of the table
    
    Returns:
        dict: Dictionary with column names as keys and data types as values
    """
    try:
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
            # connection = get_company_db_connection(company_name)
            cursor = connection.cursor()

        # Query to get all column names and types from the table
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.close()
        connection.close()
        
        return columns

    except Exception as e:
        print(f"Error fetching table columns with types: {e}")
        raise


def get_table_columns(table_name,company_name):
    company = company_name
    conn = get_company_db_connection(company)
    print("company",conn)
    if conn is None:
        print("Failed to connect to the database.")
        return []
    
    try:
        cur = conn.cursor()
        print(f"Executing query to fetch columns for table: {table_name}")
        cur.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'  -- Adjust schema if necessary
            """,
            (table_name,)
        )
        columns = [row[0] for row in cur.fetchall()]
        print("Columns fetched:", columns)
        cur.close()
        conn.close()
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []
    

def build_where_conditions(column_conditions):
    """
    Build WHERE clause conditions from column conditions list
    
    Args:
        column_conditions (list): List of condition dictionaries
    
    Returns:
        tuple: (where_clause_parts, query_params)
    """
    if not column_conditions:
        return [], []
    
    where_parts = []
    params = []
    
    print(f"Processing {len(column_conditions)} column conditions:")
    
    for i, condition in enumerate(column_conditions):
        if not condition.get('column') or not condition.get('operator'):
            print(f"Condition {i+1}: Skipped - missing column or operator")
            continue
            
        column = condition['column']
        operator = condition['operator']
        value = condition.get('value', '')
        value2 = condition.get('value2', '')
        
        print(f"Condition {i+1}: {column} {operator} {value}")
        
        if operator == 'IS NULL':
            where_parts.append(f'"{column}" IS NULL')
        elif operator == 'IS NOT NULL':
            where_parts.append(f'"{column}" IS NOT NULL')
        elif operator == 'BETWEEN':
            if value and value2:
                where_parts.append(f'"{column}" BETWEEN %s AND %s')
                params.extend([value, value2])
                print(f"  Added BETWEEN condition: {column} BETWEEN {value} AND {value2}")
        elif operator == 'IN':
            if value:
                # Parse comma-separated values and clean them
                values = [v.strip().strip("'\"") for v in value.split(',') if v.strip()]
                if values:
                    placeholders = ','.join(['%s'] * len(values))
                    where_parts.append(f'"{column}" IN ({placeholders})')
                    params.extend(values)
                    print(f"  Added IN condition: {column} IN ({', '.join(values)})")
        elif operator == 'LIKE' or operator == 'NOT LIKE':
            if value:
                where_parts.append(f'"{column}" {operator} %s')
                # Add wildcards if not present
                if '%' not in value:
                    value = f'%{value}%'
                params.append(value)
                print(f"  Added {operator} condition: {column} {operator} {value}")
        elif operator == 'ILIKE' or operator.lower() == 'contains (case insensitive)':
            if value:
                where_parts.append(f'"{column}" ILIKE %s')
                # Add wildcards if not present
                if '%' not in value:
                    value = f'%{value}%'
                params.append(value)
                print(f"  Added ILIKE condition: {column} ILIKE {value}")
        elif operator.lower() == 'contains':
            if value:
                where_parts.append(f'"{column}" LIKE %s')
                # Add wildcards if not present
                if '%' not in value:
                    value = f'%{value}%'
                params.append(value)
                print(f"  Added LIKE condition: {column} LIKE {value}")
        elif operator.lower() == 'equals':
            if value:
                where_parts.append(f'"{column}" = %s')
                params.append(value)
                print(f"  Added = condition: {column} = {value}")
        else:  # =, !=, >, >=, <, <=
            if value:
                # Handle numeric values properly
                where_parts.append(f'"{column}" {operator} %s')
                params.append(value)
                print(f"  Added {operator} condition: {column} {operator} {value}")
    
    print(f"Final WHERE parts: {where_parts}")
    print(f"Final parameters: {params}")
    return where_parts, params

def get_table_data_with_cache(company_name, table_name, date_column=None, start_date=None, end_date=None, selected_columns=None, column_conditions=None):
    """
    Fetch table data with caching functionality, column selection, and column conditions
    
    Args:
        company_name (str): Name of the company/database
        table_name (str): Name of the table to query
        date_column (str, optional): Column name for date filtering
        start_date (str, optional): Start date for filtering
        end_date (str, optional): End date for filtering
        selected_columns (list, optional): List of column names to select
        column_conditions (list, optional): List of column condition dictionaries
    
    Returns:
        list: List of dictionaries representing table rows
    """
    # Generate cache key including selected columns and conditions
    cache_key = generate_cache_key(company_name, table_name, date_column, start_date, end_date, selected_columns, column_conditions)
    
    # Check if data exists in cache
    if cache_key in GLOBAL_CACHE:
        print(f"Data found in cache for key: {cache_key}")
        cached_df = GLOBAL_CACHE[cache_key]
        print("Cached DataFrame:")
        print(cached_df)
        print(f"DataFrame shape: {cached_df.shape}")
        
        # Convert DataFrame back to JSON format for response
        return cached_df.to_dict('records')

    try:
        # Connect to the database
        print('dbcompany_name',company_name)
        connection = get_company_db_connection(company_name)
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Build column selection part of query
        if selected_columns and len(selected_columns) > 0:
            # Validate that all selected columns exist
            all_columns = get_table_columns( table_name,company_name)
            invalid_columns = [col for col in selected_columns if col not in all_columns]
            if invalid_columns:
                raise ValueError(f'Invalid columns: {invalid_columns}. Available columns: {all_columns}')
            
            # Use selected columns
            columns_str = ', '.join([f'"{col}"' for col in selected_columns])
        else:
            # Use all columns
            columns_str = '*'

        # Build the query based on date filtering and column conditions
        where_conditions = []
        query_params = []
        
        # Add date filter condition
        if date_column and start_date and end_date:
            # Validate that the date column exists in the table
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (table_name, date_column))
            
            if not cursor.fetchone():
                raise ValueError(f'Date column {date_column} not found in table {table_name}')
            
            where_conditions.append(f'"{date_column}" >= %s AND "{date_column}" <= %s')
            query_params.extend([start_date, end_date])
            print(f"Added date filter: {date_column} between {start_date} and {end_date}")

        # Add column conditions
        if column_conditions and len(column_conditions) > 0:
            print(f"Processing {len(column_conditions)} column conditions...")
            condition_parts, condition_params = build_where_conditions(column_conditions)
            if condition_parts:  # Only add if we have valid conditions
                where_conditions.extend(condition_parts)
                query_params.extend(condition_params)
                print(f"Added {len(condition_parts)} column condition(s)")
            else:
                print("No valid column conditions found")

        # Build WHERE clause - combine all conditions with AND
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)
            print(f"Complete WHERE clause: {where_clause}")

        # Build the complete query
        query = f'SELECT {columns_str} FROM "{table_name}"{where_clause}'
        
        # Add ORDER BY and LIMIT for queries without date filter
        if not (date_column and start_date and end_date):
            query += ' ORDER BY 1 LIMIT 1000'
        else:
            query += f' ORDER BY "{date_column}" DESC'

        print(f"=== EXECUTING QUERY ===")
        print(f"SQL: {query}")
        print(f"Parameters: {query_params}")
        print(f"========================")

        cursor.execute(query, query_params)
        
        rows = cursor.fetchall()
        print(f"Query returned {len(rows)} rows")
        print(f"Selected columns: {selected_columns if selected_columns else 'All columns'}")
        print(f"Date filter: {'Yes' if date_column and start_date and end_date else 'No'}")
        print(f"Column conditions: {len(column_conditions) if column_conditions else 0} conditions applied")

        # Close the connection
        cursor.close()
        connection.close()

        # Convert rows to DataFrame
        if rows:
            df = pd.DataFrame(rows)
            print("DataFrame created successfully:")
            print(f"DataFrame shape: {df.shape}")
            print(f"DataFrame columns: {list(df.columns)}")
            
            # Show sample of data for debugging
            if len(df) > 0:
                print("Sample data (first 3 rows):")
                print(df.head(3).to_string())
            
            # Store DataFrame in global cache
            GLOBAL_CACHE[cache_key] = df
            print(f"DataFrame stored in global cache with key: {cache_key}")
            
            # Convert DataFrame to JSON format for response
            json_data = df.to_dict('records')
        else:
            print("No data found matching the conditions")
            df = pd.DataFrame()
            GLOBAL_CACHE[cache_key] = df
            json_data = []

        print(f"Returning {len(json_data)} records")
        return json_data

    except Exception as e:
        print(f"Error in get_table_data_with_cache: {e}")
        import traceback
        traceback.print_exc()
        raise

def clear_cache():
    """Clear all cached data"""
    global GLOBAL_CACHE
    GLOBAL_CACHE.clear()
    print("Cache cleared successfully")

def get_cache_information():
    """
    Get information about cached data
    
    Returns:
        dict: Dictionary containing cache information
    """
    cache_info = {}
    for key, df in GLOBAL_CACHE.items():
        cache_info[key] = {
            'shape': df.shape,
            'columns': list(df.columns),
            'memory_usage': df.memory_usage(deep=True).sum()
        }
    
    return {
        'cache_keys': list(GLOBAL_CACHE.keys()),
        'total_cached_items': len(GLOBAL_CACHE),
        'cache_details': cache_info
    }

def get_cached_dataframe(company_name, table_name, date_column=None, start_date=None, end_date=None, selected_columns=None, column_conditions=None):
    """
    Retrieve DataFrame from cache
    
    Args:
        company_name (str): Name of the company/database
        table_name (str): Name of the table
        date_column (str, optional): Column name for date filtering
        start_date (str, optional): Start date for filtering
        end_date (str, optional): End date for filtering
        selected_columns (list, optional): List of selected columns
        column_conditions (list, optional): List of column conditions
    
    Returns:
        pandas.DataFrame or None: Cached DataFrame if found, None otherwise
    """
    cache_key = generate_cache_key(company_name, table_name, date_column, start_date, end_date, selected_columns, column_conditions)
    return GLOBAL_CACHE.get(cache_key, None)

def check_view_exists(company_name, view_name):
    """
    Check if a view already exists in the database
    
    Args:
        company_name (str): Name of the company/database
        view_name (str): Name of the view to check
    
    Returns:
        bool: True if view exists, False otherwise
    """
    try:
        connection = get_company_db_connection(company_name)
        cursor = connection.cursor()

        # Check if view exists in information_schema
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.views 
            WHERE table_name = %s AND table_schema = 'public'
        """, (view_name.lower(),))
        
        exists = cursor.fetchone()[0] > 0

        cursor.close()
        connection.close()
        
        return exists

    except Exception as e:
        print(f"Error checking if view exists: {e}")
        raise

def check_view_existsdb(company_name, view_name,selectedUser):
    """
    Check if a view already exists in the database
    
    Args:
        company_name (str): Name of the company/database
        view_name (str): Name of the view to check
    
    Returns:
        bool: True if view exists, False otherwise
    """
    try:
        # connection = get_db_connection(company_name)
        if not company_name:
            raise ValueError("Database name is missing")

        if not selectedUser or selectedUser.lower() == 'null':
            print("Using default local database connection...")
            connection_string = f"dbname={company_name} user={USER_NAME} password={PASSWORD} host={HOST}"
            connection = psycopg2.connect(connection_string)
        else:  # External connection
            connection_details = fetch_external_db_connection(company_name, selectedUser)
            if not connection_details:
                raise Exception(f"Unable to fetch external database connection details for user '{selectedUser}'")

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
            cursor = connection.cursor()

        # Check if view exists in information_schema
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.views 
            WHERE table_name = %s AND table_schema = 'public'
        """, (view_name.lower(),))
        
        exists = cursor.fetchone()[0] > 0

        cursor.close()
        connection.close()
        
        return exists

    except Exception as e:
        print(f"Error checking if view exists: {e}")
        raise

def create_database_view(company_name, view_config):
    """
    Create a database view based on the provided configuration
    
    Args:
        company_name (str): Name of the company/database
        view_config (dict): Configuration for the view
            - viewName (str): Name of the view
            - baseTable (str): Base table name
            - selectedColumns (list, optional): List of selected columns
            - dateFilter (dict, optional): Date filter configuration
            - columnConditions (list, optional): Column condition configurations
    
    Returns:
        dict: Success message and view details
    """
    try:
        print("company_name",view_config)
        connection = get_company_db_connection(company_name)
        print(f"[DEBUG] Connected to DB for company '{company_name}'")

        cursor = connection.cursor()

        view_name = view_config['viewName'].lower()
        base_table = view_config['baseTable']
        selected_columns = view_config.get('selectedColumns')
        date_filter = view_config.get('dateFilter')
        column_conditions = view_config.get('columnConditions')

        # Build the SELECT clause
        if selected_columns and len(selected_columns) > 0:
            # Validate that all selected columns exist
            all_columns = get_table_columns( base_table,company_name)
            invalid_columns = [col for col in selected_columns if col not in all_columns]
            if invalid_columns:
                raise ValueError(f'Invalid columns: {invalid_columns}')
            
            columns_str = ', '.join([f'"{col}"' for col in selected_columns])
        else:
            columns_str = '*'
        print("columns_str",columns_str)
        # Build the WHERE clause
        where_conditions = []
        query_params = []
        
        # Add date filter
        if date_filter:
            date_column = date_filter['column']
            start_date = date_filter['startDate']
            end_date = date_filter['endDate']
            
            # Validate date column exists
            print("base_table",base_table)
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (base_table, date_column))
            
            if not cursor.fetchone():
                raise ValueError(f'Date column {date_column} not found in table {base_table}')
            
            where_conditions.append(f'"{date_column}" >= %s AND "{date_column}" <= %s')
            query_params.extend([start_date, end_date])

        # Add column conditions
        if column_conditions:
            condition_parts, condition_params = build_where_conditions(column_conditions)
            where_conditions.extend(condition_parts)
            query_params.extend(condition_params)
                
        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)
        print("where_clause",where_clause)
        # Build the complete CREATE VIEW query
        create_view_query = f'''
            CREATE VIEW "{view_name}" AS 
            SELECT {columns_str} 
            FROM "{base_table}"{where_clause}
        '''

        print(f"Creating view with query: {create_view_query}")
        print(f"Query parameters: {query_params}")

        # Execute the CREATE VIEW statement
        if query_params:
            # For views with parameters, we need to substitute them directly
            # since CREATE VIEW doesn't support parameterized queries
            formatted_query = create_view_query
            for param in query_params:
                formatted_query = formatted_query.replace('%s', f"'{param}'", 1)
            cursor.execute(formatted_query)
        else:
            cursor.execute(create_view_query)

        # Commit the transaction
        connection.commit()

        # Get view information
        cursor.execute("""
            SELECT schemaname, viewname, definition 
            FROM pg_views 
            WHERE viewname = %s
        """, (view_name,))
        
        view_info = cursor.fetchone()

        cursor.close()
        connection.close()

        return {
            'success': True,
            'message': f'View "{view_name}" created successfully',
            'viewName': view_name,
            'baseTable': base_table,
            'columnsCount': len(selected_columns) if selected_columns else 'all',
            'hasDateFilter': bool(date_filter),
            'hasColumnConditions': bool(column_conditions),
            'definition': view_info[2] if view_info else None
        }

    except psycopg2.Error as e:
        print(f"PostgreSQL error creating view: {e}")
        if 'connection' in locals() and connection:
            connection.rollback()
            cursor.close()
            connection.close()
        
        # Handle specific PostgreSQL errors
        if 'already exists' in str(e):
            raise ValueError(f'View "{view_name}" already exists')
        else:
            raise ValueError(f'Database error: {str(e)}')
    
    except Exception as e:
        print(f"Error creating view: {e}")
        if 'connection' in locals() and connection:
            connection.rollback()
            cursor.close()
            connection.close()
        raise

# def create_database_viewdb(company_name, view_config,selectedUser):
#     print("company name =    ",company_name)
#     """
#     Create a database view based on the provided configuration
    
#     Args:
#         company_name (str): Name of the company/database
#         view_config (dict): Configuration for the view
#             - viewName (str): Name of the view
#             - baseTable (str): Base table name
#             - selectedColumns (list, optional): List of selected columns
#             - dateFilter (dict, optional): Date filter configuration
#             - columnConditions (list, optional): Column condition configurations
    
#     Returns:
#         dict: Success message and view details
#     """
#     try:
#         # connection = get_company_db_connection(company_name)
#         if not company_name:
#             raise ValueError("Database name is missing")

#         if not selectedUser or selectedUser.lower() == 'null':
#             print("Using default local database connection...")
#             # connection_string = f"dbname={company_name} user={USER_NAME} password={PASSWORD} host={HOST}"
#             connection = get_company_db_connection(company_name)
#         else:  # External connection
#             connection_details = fetch_external_db_connection(company_name, selectedUser)
#             if not connection_details:
#                 raise Exception(f"Unable to fetch external database connection details for user '{selectedUser}'")

#             db_details = {
#                 "host": connection_details[3],
#                 "database": connection_details[7],
#                 "user": connection_details[4],
#                 "password": connection_details[5],
#                 "port": int(connection_details[6])
#             }
            
#             print(f"Connecting to external database: {db_details['database']}@{db_details['host']}:{db_details['port']} as {db_details['user']}")
#             connection = psycopg2.connect(
#                 dbname=db_details['database'],
#                 user=db_details['user'],
#                 password=db_details['password'],
#                 host=db_details['host'],
#                 port=db_details['port'],
#             )
#         cursor = connection.cursor()

#         view_name = view_config['viewName'].lower()
#         base_table = view_config['baseTable']
#         selected_columns = view_config.get('selectedColumns')
#         date_filter = view_config.get('dateFilter')
#         column_conditions = view_config.get('columnConditions')

#         # Build the SELECT clause
#         if selected_columns and len(selected_columns) > 0:
#             # Validate that all selected columns exist
#             all_columns = get_table_columns(company_name, base_table)
#             invalid_columns = [col for col in selected_columns if col not in all_columns]
#             if invalid_columns:
#                 raise ValueError(f'Invalid columns: {invalid_columns}')
            
#             columns_str = ', '.join([f'"{col}"' for col in selected_columns])
#         else:
#             columns_str = '*'

#         # Build the WHERE clause
#         where_conditions = []
#         query_params = []
        
#         # Add date filter
#         if date_filter:
#             date_column = date_filter['column']
#             start_date = date_filter['startDate']
#             end_date = date_filter['endDate']
            
#             # Validate date column exists
#             cursor.execute("""
#                 SELECT column_name 
#                 FROM information_schema.columns 
#                 WHERE table_name = %s AND column_name = %s
#             """, (base_table, date_column))
            
#             if not cursor.fetchone():
#                 raise ValueError(f'Date column {date_column} not found in table {base_table}')
            
#             where_conditions.append(f'"{date_column}" >= %s AND "{date_column}" <= %s')
#             query_params.extend([start_date, end_date])

#         # Add column conditions
#         if column_conditions:
#             condition_parts, condition_params = build_where_conditions(column_conditions)
#             where_conditions.extend(condition_parts)
#             query_params.extend(condition_params)

#         # Build WHERE clause
#         where_clause = ""
#         if where_conditions:
#             where_clause = " WHERE " + " AND ".join(where_conditions)

#         # Build the complete CREATE VIEW query
#         create_view_query = f'''
#             CREATE VIEW "{view_name}" AS 
#             SELECT {columns_str} 
#             FROM "{base_table}"{where_clause}
#         '''

#         print(f"Creating view with query: {create_view_query}")
#         print(f"Query parameters: {query_params}")

#         # Execute the CREATE VIEW statement
#         if query_params:
#             # For views with parameters, we need to substitute them directly
#             # since CREATE VIEW doesn't support parameterized queries
#             formatted_query = create_view_query
#             for param in query_params:
#                 formatted_query = formatted_query.replace('%s', f"'{param}'", 1)
#             cursor.execute(formatted_query)
#         else:
#             cursor.execute(create_view_query)

#         # Commit the transaction
#         connection.commit()

#         # Get view information
#         cursor.execute("""
#             SELECT schemaname, viewname, definition 
#             FROM pg_views 
#             WHERE viewname = %s
#         """, (view_name,))
        
#         view_info = cursor.fetchone()

#         cursor.close()
#         connection.close()

#         return {
#             'success': True,
#             'message': f'View "{view_name}" created successfully',
#             'viewName': view_name,
#             'baseTable': base_table,
#             'columnsCount': len(selected_columns) if selected_columns else 'all',
#             'hasDateFilter': bool(date_filter),
#             'hasColumnConditions': bool(column_conditions),
#             'definition': view_info[2] if view_info else None
#         }

#     except psycopg2.Error as e:
#         print(f"PostgreSQL error creating view: {e}")
#         if 'connection' in locals() and connection:
#             connection.rollback()
#             cursor.close()
#             connection.close()
        
#         # Handle specific PostgreSQL errors
#         if 'already exists' in str(e):
#             raise ValueError(f'View "{view_name}" already exists')
#         else:
#             raise ValueError(f'Database error: {str(e)}')
    
#     except Exception as e:
#         print(f"Error creating view: {e}")
#         if 'connection' in locals() and connection:
#             connection.rollback()
#             cursor.close()
#             connection.close()
#         raise





def create_database_viewdb(company_name, view_config, selectedUser):
    print("company name =    ", company_name)
    """
    Create a database view based on the provided configuration
    
    Args:
        company_name (str): Name of the company/database
        view_config (dict): Configuration for the view
            - viewName (str): Name of the view
            - baseTable (str): Base table name
            - selectedColumns (list, optional): List of selected columns
            - dateFilter (dict, optional): Date filter configuration
            - columnConditions (list, optional): Column condition configurations
        selectedUser (str): User selection for database connection
    
    Returns:
        dict: Success message and view details
    """
    try:
        # Validate required parameters
        if not company_name:
            raise ValueError("Database name is missing")

        # Establish database connection
        if not selectedUser or selectedUser.lower() == 'null' or selectedUser.lower() == 'local':
            print("Using default local database connection...")
            connection = get_company_db_connection(company_name)
        else:  # External connection
            connection_details = fetch_external_db_connection(company_name, selectedUser)
            if not connection_details:
                raise Exception(f"Unable to fetch external database connection details for user '{selectedUser}'")

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
        
        cursor = connection.cursor()

        # Extract view configuration
        view_name = view_config['viewName'].lower()
        base_table = view_config['baseTable']
        selected_columns = view_config.get('selectedColumns')
        date_filter = view_config.get('dateFilter')
        column_conditions = view_config.get('columnConditions')

        # Build the SELECT clause with column validation
        if selected_columns and len(selected_columns) > 0:
            # Validate that all selected columns exist using the current connection
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (base_table,))
            
            all_columns = [row[0] for row in cursor.fetchall()]
            invalid_columns = [col for col in selected_columns if col not in all_columns]
            if invalid_columns:
                raise ValueError(f'Invalid columns: {invalid_columns}. Available columns: {all_columns}')
            
            columns_str = ', '.join([f'"{col}"' for col in selected_columns])
        else:
            columns_str = '*'

        # Build the WHERE clause
        where_conditions = []
        query_params = []
        
        # Add date filter
        if date_filter:
            date_column = date_filter['column']
            start_date = date_filter['startDate']
            end_date = date_filter['endDate']
            
            # Validate date column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (base_table, date_column))
            
            if not cursor.fetchone():
                raise ValueError(f'Date column "{date_column}" not found in table "{base_table}"')
            
            where_conditions.append(f'"{date_column}" >= %s AND "{date_column}" <= %s')
            query_params.extend([start_date, end_date])

        # Add column conditions
        if column_conditions:
            condition_parts, condition_params = build_where_conditions(column_conditions)
            where_conditions.extend(condition_parts)
            query_params.extend(condition_params)

        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)

        # Check if view already exists and drop it if it does
        cursor.execute("""
            SELECT 1 FROM pg_views WHERE viewname = %s
        """, (view_name,))
        
        if cursor.fetchone():
            print(f"View '{view_name}' already exists. Dropping it first...")
            cursor.execute(f'DROP VIEW IF EXISTS "{view_name}" CASCADE')

        # Build the complete CREATE VIEW query
        create_view_query = f'''
            CREATE VIEW "{view_name}" AS 
            SELECT {columns_str} 
            FROM "{base_table}"{where_clause}
        '''

        print(f"Creating view with query: {create_view_query}")
        print(f"Query parameters: {query_params}")

        # Execute the CREATE VIEW statement
        if query_params:
            # For views with parameters, we need to substitute them directly
            # since CREATE VIEW doesn't support parameterized queries
            formatted_query = create_view_query
            for param in query_params:
                if isinstance(param, str):
                    formatted_query = formatted_query.replace('%s', f"'{param}'", 1)
                else:
                    formatted_query = formatted_query.replace('%s', str(param), 1)
            cursor.execute(formatted_query)
        else:
            cursor.execute(create_view_query)

        # Commit the transaction
        connection.commit()

        # Get view information
        cursor.execute("""
            SELECT schemaname, viewname, definition 
            FROM pg_views 
            WHERE viewname = %s
        """, (view_name,))
        
        view_info = cursor.fetchone()

        # Get column count for response
        columns_count = len(selected_columns) if selected_columns else 'all'

        cursor.close()
        connection.close()

        return {
            'success': True,
            'message': f'View "{view_name}" created successfully',
            'viewName': view_name,
            'baseTable': base_table,
            'columnsCount': columns_count,
            'hasDateFilter': bool(date_filter),
            'hasColumnConditions': bool(column_conditions),
            'definition': view_info[2] if view_info else None
        }

    except psycopg2.Error as e:
        print(f"PostgreSQL error creating view: {e}")
        if 'connection' in locals() and connection:
            connection.rollback()
            if 'cursor' in locals() and cursor:
                cursor.close()
            connection.close()
        
        # Handle specific PostgreSQL errors
        if 'already exists' in str(e):
            raise ValueError(f'View "{view_name}" already exists')
        else:
            raise ValueError(f'Database error: {str(e)}')
    
    except Exception as e:
        print(f"Error creating view: {e}")
        if 'connection' in locals() and connection:
            try:
                connection.rollback()
                if 'cursor' in locals() and cursor:
                    cursor.close()
                connection.close()
            except:
                pass  # Connection might already be closed
        raise


def build_where_conditions(column_conditions):
    """
    Helper function to build WHERE conditions from column conditions
    
    Args:
        column_conditions (list): List of column condition dictionaries
    
    Returns:
        tuple: (condition_parts, condition_params)
    """
    condition_parts = []
    condition_params = []
    
    for condition in column_conditions:
        column = condition['column']
        operator = condition['operator'].upper()
        value = condition['value']
        value2 = condition.get('value2', '')
        data_type = condition.get('dataType', 'text')
        
        if operator in ['=', '!=', '<>', '<', '>', '<=', '>=']:
            condition_parts.append(f'"{column}" {operator} %s')
            condition_params.append(value)
        
        elif operator in ['LIKE', 'ILIKE']:
            condition_parts.append(f'"{column}" {operator} %s')
            condition_params.append(value)
        
        elif operator == 'IN':
            if isinstance(value, str):
                # Split comma-separated values
                values = [v.strip() for v in value.split(',')]
            else:
                values = value if isinstance(value, list) else [value]
            
            placeholders = ', '.join(['%s'] * len(values))
            condition_parts.append(f'"{column}" IN ({placeholders})')
            condition_params.extend(values)
        
        elif operator == 'BETWEEN':
            condition_parts.append(f'"{column}" BETWEEN %s AND %s')
            condition_params.extend([value, value2])
        
        elif operator == 'IS NULL':
            condition_parts.append(f'"{column}" IS NULL')
        
        elif operator == 'IS NOT NULL':
            condition_parts.append(f'"{column}" IS NOT NULL')
        
        else:
            # Default case
            condition_parts.append(f'"{column}" {operator} %s')
            condition_params.append(value)
    
    return condition_parts, condition_params












def get_user_views(company_name):
    """
    Get all user-created views in the database
    
    Args:
        company_name (str): Name of the company/database
    
    Returns:
        list: List of view information dictionaries
    """
    try:
        connection = get_company_db_connection(company_name)
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Get all views in the public schema
        cursor.execute("""
            SELECT 
                schemaname,
                viewname,
                viewowner,
                definition
            FROM pg_views 
            WHERE schemaname = 'public'
            ORDER BY viewname
        """)
        
        views = cursor.fetchall()

        cursor.close()
        connection.close()
        
        return [dict(view) for view in views]

    except Exception as e:
        print(f"Error fetching user views: {e}")
        raise

def drop_database_view(company_name, view_name):
    """
    Drop a database view
    
    Args:
        company_name (str): Name of the company/database
        view_name (str): Name of the view to drop
    
    Returns:
        dict: Success message
    """
    try:
        connection = get_company_db_connection(company_name)
        cursor = connection.cursor()

        # Check if view exists first
        if not check_view_exists(company_name, view_name):
            raise ValueError(f'View "{view_name}" does not exist')

        # Drop the view
        cursor.execute(f'DROP VIEW IF EXISTS "{view_name.lower()}" CASCADE')
        connection.commit()

        cursor.close()
        connection.close()

        return {
            'success': True,
            'message': f'View "{view_name}" dropped successfully'
        }

    except Exception as e:
        print(f"Error dropping view: {e}")
        if 'connection' in locals() and connection:
            connection.rollback()
            cursor.close()
            connection.close()
        raise
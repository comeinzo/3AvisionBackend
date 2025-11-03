


# import os
# import pandas as pd
# import psycopg2
# import re
# from psycopg2 import sql
# from psycopg2.extras import execute_values
# import traceback
# from tqdm import tqdm
# from user_upload import get_db_connection
# from flask import Flask, request, jsonify
# import io

# def is_table_used_in_charts(table_name):
#     conn = get_db_connection(dbname="datasource")
#     cur = conn.cursor()
#     cur.execute(
#         """
#         SELECT EXISTS (
#             SELECT 1 FROM table_chart_save WHERE selected_table = %s
#         )
#         """,
#         (table_name,)
#     )
#     return cur.fetchone()[0]

# def check_table_usage(cur, table_name):
#     if not table_name:
#         return jsonify({"error": "Table name is required"}), 400

#     # Remove any surrounding quotes from the table name
#     table_name = table_name.strip('"').strip("'")

#     # Debugging: Print the received table name
#     print(f"Received table name: {table_name}")

#     # Check if the table is used for chart creation
#     is_in_use = is_table_used_in_charts(table_name)
#     print("is_in_use", is_in_use)
#     return is_in_use

# # Function to sanitize column names (replace spaces and special characters with underscores)
# def sanitize_column_name(col_name):
#     if isinstance(col_name, str):
#         return re.sub(r'\W+', '_', col_name).lower()
#     else:
#         return col_name

# # Function to validate if the table exists
# def validate_table_structure(cur, table_name, df):
#     cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
#     table_exists = cur.fetchone()[0]
#     return table_exists

# # Function to map pandas dtypes to PostgreSQL types
# def determine_sql_data_type(value):
#     if pd.api.types.is_string_dtype(value):
#         return 'VARCHAR'
#     elif pd.api.types.is_integer_dtype(value):
#         return 'INTEGER'
#     elif pd.api.types.is_float_dtype(value):
#         return 'DOUBLE PRECISION'
#     elif pd.api.types.is_bool_dtype(value):
#         return 'BOOLEAN'
#     elif pd.api.types.is_numeric_dtype(value):
#         return 'DOUBLE PRECISION'  
#     else:
#         return 'VARCHAR'

# # Function to check for repeating columns
# def check_repeating_columns(df):
#     duplicate_columns = df.columns[df.columns.duplicated()].tolist()
#     return duplicate_columns

# # PERFORMANCE OPTIMIZATION: Bulk insert using COPY FROM
# def bulk_insert_with_copy(cur, conn, table_name, df):
#     """
#     High-performance bulk insert using PostgreSQL COPY FROM
#     This is significantly faster than executemany for large datasets
#     """
#     try:
#         # Create a StringIO buffer
#         output = io.StringIO()
        
#         # Convert DataFrame to CSV format in memory
#         df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
#         output.seek(0)
        
#         # Use COPY FROM to bulk insert
#         copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')").format(
#             sql.Identifier(table_name),
#             sql.SQL(', ').join(map(sql.Identifier, df.columns))
#         )
        
#         cur.copy_expert(copy_query.as_string(conn), output)
#         print(f"Bulk inserted {len(df)} rows into table '{table_name}' using COPY FROM.")
        
#     except Exception as e:
#         print(f"COPY FROM failed, falling back to batch insert: {str(e)}")
#         # Fallback to the original batch insert method
#         return False
    
#     return True

# # PERFORMANCE OPTIMIZATION: Optimized batch insert using execute_values
# def optimized_batch_insert(cur, conn, table_name, df, batch_size=5000):
#     """
#     Optimized batch insert using psycopg2's execute_values
#     Much faster than executemany
#     """
#     try:
#         # Prepare data for insertion
#         rows_to_insert = []
#         for _, row in df.iterrows():
#             # Handle datetime columns
#             processed_row = []
#             for col in df.columns:
#                 value = row[col]
#                 if pd.isna(value):
#                     processed_row.append(None)
#                 elif col in df.select_dtypes(include=['datetime']).columns:
#                     processed_row.append(value if not pd.isna(value) else None)
#                 else:
#                     processed_row.append(value)
#             rows_to_insert.append(tuple(processed_row))

#         # Use execute_values for better performance
#         placeholders = ', '.join(['%s'] * len(df.columns))
#         insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
#             sql.Identifier(table_name),
#             sql.SQL(', ').join(map(sql.Identifier, df.columns))
#         )

#         total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size

#         with tqdm(total=len(rows_to_insert), desc="Inserting rows", unit="row") as pbar:
#             for i in range(0, len(rows_to_insert), batch_size):
#                 batch = rows_to_insert[i:i + batch_size]
                
#                 execute_values(
#                     cur,
#                     insert_query.as_string(conn),
#                     batch,
#                     template=f"({placeholders})",
#                     page_size=batch_size
#                 )
                
#                 # Commit less frequently for better performance
#                 if i % (batch_size * 5) == 0 or i + batch_size >= len(rows_to_insert):
#                     conn.commit()
                
#                 # pbar.update(len(batch))
#                 processed_rows = i + len(batch)
#                 pbar.update(len(batch))
#                 percent = (processed_rows / len(rows_to_insert)) * 100
#                 print(f"Progress: {processed_rows}/{len(rows_to_insert)} rows ({percent:.2f}%)")


#         print(f"Optimized batch inserted {len(rows_to_insert)} rows into table '{table_name}'.")
        
#     except Exception as e:
#         print(f"Error during optimized batch insert: {str(e)}")
#         raise e

# # PERFORMANCE OPTIMIZATION: Chunked DataFrame processing
# def process_dataframe_in_chunks(df, chunk_size=10000):
#     """
#     Process large DataFrames in chunks to reduce memory usage
#     """
#     for start in range(0, len(df), chunk_size):
#         yield df.iloc[start:start + chunk_size]

# UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'excel')

# def upload_excel_to_postgresql(database_name, username, password, excel_file_name, primary_key_column, host, port='5432', selected_sheets=None):
#     try:
#         current_dir = os.getcwd()
#         excel_file_path = os.path.join(current_dir, excel_file_name)

#         # PERFORMANCE OPTIMIZATION: Use connection pooling settings
#         conn = psycopg2.connect(
#             dbname=database_name, 
#             user=username, 
#             password=password, 
#             host=host, 
#             port=port,
#             # Performance optimizations
#             connect_timeout=30,
#             application_name="excel_upload_optimized"
#         )
        
#         # PERFORMANCE OPTIMIZATION: Set session parameters for better performance
#         cur = conn.cursor()
#         cur.execute("SET synchronous_commit = OFF")  # Faster commits
#         cur.execute("SET commit_delay = 0")          # Reduce commit delays
#         cur.execute("SET work_mem = '256MB'")        # Increase work memory for sorting/hashing
        
#         if not excel_file_path.lower().endswith('.xlsx'):
#             return "Error: Only Excel files with .xlsx extension are supported."

#         xls = pd.ExcelFile(excel_file_path)

#         directory_name = os.path.splitext(os.path.basename(excel_file_name))[0]
#         directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
#         os.makedirs(directory_path, exist_ok=True)

#         for sheet_name in selected_sheets:
#             sheet_name_cleaned = sheet_name.strip('"').strip()
#             if sheet_name_cleaned not in xls.sheet_names:
#                 print(f"Sheet '{sheet_name_cleaned}' not found in the Excel file. Skipping...")
#                 continue
                
#             # PERFORMANCE OPTIMIZATION: Read Excel with optimized parameters
#             df = pd.read_excel(
#                 excel_file_name, 
#                 sheet_name=sheet_name_cleaned,
#                 engine='openpyxl',  # Generally faster for .xlsx files
#                 # Use dtype inference for better performance
#                 dtype_backend='numpy_nullable'
#             )
            
#             table_name = sanitize_column_name(sheet_name_cleaned)
#             df.columns = [sanitize_column_name(col) for col in df.columns]
#             print(f"Columns in {sheet_name}: {df.columns}")
#             print(f"DataFrame shape: {df.shape}")
    
#             table_exists = validate_table_structure(cur, table_name, df)
#             print(f"Table exists for {table_name}: {table_exists}")
#             is_table_in_use = check_table_usage(cur, table_name)
#             print(f"Table '{table_name}' is in use: {is_table_in_use}")

#             if table_exists and is_table_in_use == False:
#                 # If table exists and is NOT in use, handle missing columns
#                 cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
#                 existing_columns = [row[0] for row in cur.fetchall()]
#                 print(f"Existing columns in table '{table_name}': {existing_columns}")

#                 # Find columns to delete (existing in table but missing in uploaded file)
#                 columns_to_delete = [col for col in existing_columns if col not in df.columns]

#                 for col in columns_to_delete:
#                     alter_query = sql.SQL('ALTER TABLE {} DROP COLUMN {}').format(
#                         sql.Identifier(table_name),
#                         sql.Identifier(col)
#                     )
#                     try:
#                         cur.execute(alter_query)
#                         print(f"Deleted column '{col}' from table '{table_name}'.")
#                     except Exception as e:
#                         print(f"Error deleting column '{col}' from table '{table_name}': {str(e)}")
#                         continue

#             if table_exists:
#                 print(f"Validating and adding missing columns to table '{table_name}'.")
                
#                 # Detect and add missing columns
#                 cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
#                 existing_columns = [row[0] for row in cur.fetchall()]

#                 missing_columns = [col for col in df.columns if col not in existing_columns]

#                 for col in missing_columns:
#                     # Infer column type based on data in DataFrame
#                     if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
#                         col_type = 'VARCHAR'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
#                         col_type = 'INTEGER'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
#                         col_type = 'NUMERIC'
#                     elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
#                         df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
#                         col_type = 'DATE'
#                     else:
#                         col_type = 'VARCHAR'

#                     alter_query = sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
#                         sql.Identifier(table_name),
#                         sql.Identifier(col),
#                         sql.SQL(col_type)
#                     )

#                     try:
#                         cur.execute(alter_query)
#                         print(f"Added column '{col}' with type '{col_type}' to table '{table_name}'.")
#                     except Exception as e:
#                         print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")
#                         continue

#             else:
#                 print(f"Creating table '{table_name}'.")
                
#                 # Determine data types based on column values
#                 column_types = []
#                 for col in df.columns:
#                     if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
#                         col_type = 'VARCHAR'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
#                         col_type = 'INTEGER'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
#                         col_type = 'NUMERIC'
#                     elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
#                         df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
#                         col_type = 'DATE'
#                     else:
#                         col_type = 'VARCHAR'
                    
#                     column_types.append((col, col_type))

#                 columns = ', '.join(f'"{col}" {col_type}' for col, col_type in column_types)
#                 create_table_query = sql.SQL('CREATE TABLE {} ({})').format(sql.Identifier(table_name),
#                                                                               sql.SQL(columns))
#                 try:
#                     print(f"Create Table Query: {create_table_query.as_string(cur)}")
#                     cur.execute(create_table_query)
#                 except Exception as e:
#                     print(f"Error creating table {table_name}: {str(e)}")
#                     continue

#                 if primary_key_column in df.columns:
#                     alter_table_query = sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
#                         sql.Identifier(table_name), sql.Identifier(primary_key_column))
#                     try:
#                         cur.execute(alter_table_query)
#                     except Exception as e:
#                         print(f"Error adding primary key to {table_name}: {str(e)}")
#                         continue

#             # Check for duplicate primary keys
#             duplicate_primary_keys = df[df.duplicated(subset=[primary_key_column], keep=False)][primary_key_column].tolist()
#             if duplicate_primary_keys:
#                 return f"Error: Duplicate primary key values found: {', '.join(map(str, duplicate_primary_keys))}"

#             # PERFORMANCE OPTIMIZATION: Bulk delete using IN clause with batching
#             primary_key_values = df[primary_key_column].tolist()
            
#             # Process deletions in batches to avoid query size limits
#             delete_batch_size = 1000
#             total_deleted = 0
            
#             for i in range(0, len(primary_key_values), delete_batch_size):
#                 batch_values = primary_key_values[i:i + delete_batch_size]
                
#                 delete_query = sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
#                     sql.Identifier(table_name),
#                     sql.Identifier(primary_key_column)
#                 )
                
#                 cur.execute(delete_query, (batch_values,))
#                 total_deleted += cur.rowcount

#             if total_deleted > 0:
#                 print(f"Deleted {total_deleted} rows with matching primary key values in table '{table_name}'.")
#             else:
#                 print("No rows were deleted.")

#             # PERFORMANCE OPTIMIZATION: Choose best insertion method based on data size
#             print(f"Starting data insertion for {len(df)} rows...")
            
#             if len(df) > 50000:
#                 # For very large datasets, try COPY FROM first
#                 print("Using COPY FROM for large dataset...")
#                 copy_success = bulk_insert_with_copy(cur, conn, table_name, df)
                
#                 if not copy_success:
#                     print("COPY FROM failed, using optimized batch insert...")
#                     optimized_batch_insert(cur, conn, table_name, df, batch_size=5000)
#             else:
#                 # For smaller datasets, use optimized batch insert
#                 print("Using optimized batch insert...")
#                 optimized_batch_insert(cur, conn, table_name, df, batch_size=2000)

#             # Save Excel file
#             file_name = f"{table_name}.xlsx"
#             file_path = os.path.join(directory_path, file_name)
#             df.to_excel(file_path, index=False)

#         # Handle datasource table creation and insertion
#         cur.execute("""
#             SELECT EXISTS (
#                 SELECT FROM pg_tables 
#                 WHERE schemaname = 'public' 
#                 AND tablename = 'datasource'
#             );
#         """)
#         table_exists = cur.fetchone()[0]

#         if not table_exists:
#             cur.execute("""
#                 CREATE TABLE datasource (
#                     id SERIAL PRIMARY KEY,
#                     data_source_name VARCHAR(255),
#                     data_source_path VARCHAR(255)
#                 );
#             """)
#             print("Created 'datasource' table.")

#         insert_query = sql.SQL("""
#                     INSERT INTO datasource (data_source_name, data_source_path)
#                     VALUES (%s, %s)
#                 """)
#         cur.execute(insert_query, (directory_name, directory_path))
        
#         # Final commit
#         conn.commit()
        
#         # Reset session parameters
#         cur.execute("RESET synchronous_commit")
#         cur.execute("RESET commit_delay")
#         cur.execute("RESET work_mem")

#         cur.close()
#         conn.close()

#         return "Upload successful"
        
#     except Exception as e:
#         print("An error occurred:", e)
#         traceback.print_exc()
#         return f"Error: {str(e)}"




import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql
from psycopg2.extras import execute_values
import traceback
from tqdm import tqdm
from user_upload import get_db_connection
from flask import Flask, request, jsonify
import io
from datetime import datetime, date
import dateutil.parser as date_parser

def is_table_used_in_charts(table_name):
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

def check_table_usage(cur, table_name):
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    # Remove any surrounding quotes from the table name
    table_name = table_name.strip('"').strip("'")

    # Debugging: Print the received table name
    print(f"Received table name: {table_name}")

    # Check if the table is used for chart creation
    is_in_use = is_table_used_in_charts(table_name)
    print("is_in_use", is_in_use)
    return is_in_use

# Function to sanitize column names (replace spaces and special characters with underscores)
def sanitize_column_name(col_name):
    if isinstance(col_name, str):
        return re.sub(r'\W+', '_', col_name).lower()
    else:
        return col_name

# Enhanced function to parse and standardize date formats for PostgreSQL DATE type
def parse_and_standardize_date(date_value):
    """
    Parse various date formats and convert to Python date object for PostgreSQL DATE type
    Handles formats like: 10/18/2014, 11-07-2011, 13-jan-2012, etc.
    Returns a date object or None for NULL values
    """
    if pd.isna(date_value) or date_value == '' or date_value is None:
        return None
    
    # If it's already a datetime object, convert to date
    if isinstance(date_value, (pd.Timestamp, datetime)):
        return date_value.date()
    
    # Convert to string if it's not already
    date_str = str(date_value).strip()
    
    if date_str == '' or date_str.lower() == 'nan':
        return None
    
    try:
        # List of common date formats to try
        date_formats = [
            '%m/%d/%Y',     # 10/18/2014
            '%d/%m/%Y',     # 18/10/2014
            '%m-%d-%Y',     # 10-18-2014
            '%d-%m-%Y',     # 18-10-2014
            '%Y-%m-%d',     # 2014-10-18
            '%Y/%m/%d',     # 2014/10/18
            '%d-%b-%Y',     # 13-jan-2012
            '%d %b %Y',     # 13 jan 2012
            '%b %d, %Y',    # jan 13, 2012
            '%B %d, %Y',    # January 13, 2012
            '%d/%b/%Y',     # 13/jan/2012
            '%d %B %Y',     # 13 January 2012
        ]
        
        # First, try the predefined formats
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.date()  # Return date object instead of formatted string
            except ValueError:
                continue
        
        # If predefined formats fail, try dateutil parser (more flexible but slower)
        try:
            parsed_date = date_parser.parse(date_str, dayfirst=False)  # Assume month first for ambiguous dates
            return parsed_date.date()
        except:
            pass
        
        # Try with day first assumption
        try:
            parsed_date = date_parser.parse(date_str, dayfirst=True)
            return parsed_date.date()
        except:
            pass
        
        print(f"Warning: Could not parse date '{date_str}'. Setting to NULL.")
        return None
        
    except Exception as e:
        print(f"Error parsing date '{date_str}': {str(e)}. Setting to NULL.")
        return None

def detect_and_process_date_columns(df):
    """
    Detect potential date columns and standardize their formats to date objects
    """
    print("Detecting and processing date columns...")
    
    for col in df.columns:
        # Sample a few non-null values to check if they look like dates
        sample_values = df[col].dropna().head(10).tolist()
        
        if not sample_values:
            continue
        
        # Check if the column might contain dates
        date_like_count = 0
        for value in sample_values:
            if isinstance(value, (pd.Timestamp, datetime)):
                date_like_count += 1
            elif isinstance(value, str):
                # Look for common date patterns
                date_patterns = [
                    r'\d{1,2}[/-]\d{1,2}[/-]\d{4}',     # MM/DD/YYYY or DD/MM/YYYY
                    r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',     # YYYY/MM/DD
                    r'\d{1,2}[-/]\w{3}[-/]\d{4}',       # DD-MMM-YYYY
                    r'\w{3}\s+\d{1,2},?\s+\d{4}',       # MMM DD, YYYY
                ]
                
                for pattern in date_patterns:
                    if re.search(pattern, str(value)):
                        date_like_count += 1
                        break
        
        # If more than 50% of sample values look like dates, process this column
        if date_like_count / len(sample_values) > 0.5:
            print(f"Processing date column: {col}")
            
            # Apply date standardization to the entire column - returns date objects now
            standardized_dates = []
            for value in df[col]:
                standardized_date = parse_and_standardize_date(value)
                standardized_dates.append(standardized_date)
            
            df[col] = standardized_dates
            print(f"Standardized {len([d for d in standardized_dates if d is not None])} dates in column '{col}' to date objects")
    
    return df

# Function to validate if the table exists
def validate_table_structure(cur, table_name, df):
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
    table_exists = cur.fetchone()[0]
    return table_exists

# Function to map pandas dtypes to PostgreSQL types
def determine_sql_data_type(value):
    if pd.api.types.is_string_dtype(value):
        return 'VARCHAR'
    elif pd.api.types.is_integer_dtype(value):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(value):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(value):
        return 'BOOLEAN'
    elif pd.api.types.is_numeric_dtype(value):
        return 'DOUBLE PRECISION'  
    else:
        return 'VARCHAR'

# Enhanced column type detection with proper date handling
def determine_column_type(df, col):
    """
    Enhanced column type detection with proper date handling
    """
    sample_values = df[col].dropna()
    
    if len(sample_values) == 0:
        return 'VARCHAR'
    
    # Check if column contains date objects (from our date processing)
    if sample_values.apply(lambda x: isinstance(x, date)).any():
        return 'DATE'
    elif sample_values.apply(lambda x: isinstance(x, str)).all():
        return 'VARCHAR'
    elif sample_values.apply(lambda x: isinstance(x, int)).all():
        return 'INTEGER'
    elif sample_values.apply(lambda x: isinstance(x, float)).all():
        return 'NUMERIC'
    else:
        return 'VARCHAR'

# Function to check for repeating columns
def check_repeating_columns(df):
    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    return duplicate_columns

# PERFORMANCE OPTIMIZATION: Bulk insert using COPY FROM
def bulk_insert_with_copy(cur, conn, table_name, df):
    """
    High-performance bulk insert using PostgreSQL COPY FROM
    This is significantly faster than executemany for large datasets
    """
    try:
        # Create a StringIO buffer
        output = io.StringIO()
        
        # Convert DataFrame to CSV format in memory
        df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        
        # Use COPY FROM to bulk insert
        copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, df.columns))
        )
        
        cur.copy_expert(copy_query.as_string(conn), output)
        print(f"Bulk inserted {len(df)} rows into table '{table_name}' using COPY FROM.")
        
    except Exception as e:
        print(f"COPY FROM failed, falling back to batch insert: {str(e)}")
        # Fallback to the original batch insert method
        return False
    
    return True

# PERFORMANCE OPTIMIZATION: Optimized batch insert using execute_values
def optimized_batch_insert(cur, conn, table_name, df, batch_size=5000):
    """
    Optimized batch insert using psycopg2's execute_values
    Now properly handles date objects for PostgreSQL DATE columns
    """
    try:
        # Prepare data for insertion
        rows_to_insert = []
        for _, row in df.iterrows():
            # Handle different data types including dates
            processed_row = []
            for col in df.columns:
                value = row[col]
                if pd.isna(value) or value is None:
                    processed_row.append(None)
                elif isinstance(value, date):
                    # Date objects are automatically handled by psycopg2
                    processed_row.append(value)
                elif col in df.select_dtypes(include=['datetime']).columns:
                    processed_row.append(value if not pd.isna(value) else None)
                else:
                    processed_row.append(value)
            rows_to_insert.append(tuple(processed_row))

        # Use execute_values for better performance
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, df.columns))
        )

        total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size

        with tqdm(total=len(rows_to_insert), desc="Inserting rows", unit="row") as pbar:
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                
                execute_values(
                    cur,
                    insert_query.as_string(conn),
                    batch,
                    template=f"({placeholders})",
                    page_size=batch_size
                )
                
                # Commit less frequently for better performance
                if i % (batch_size * 5) == 0 or i + batch_size >= len(rows_to_insert):
                    conn.commit()
                
                processed_rows = i + len(batch)
                pbar.update(len(batch))
                percent = (processed_rows / len(rows_to_insert)) * 100
                print(f"Progress: {processed_rows}/{len(rows_to_insert)} rows ({percent:.2f}%)")

        print(f"Optimized batch inserted {len(rows_to_insert)} rows into table '{table_name}'.")
        
    except Exception as e:
        print(f"Error during optimized batch insert: {str(e)}")
        raise e

# PERFORMANCE OPTIMIZATION: Chunked DataFrame processing
def process_dataframe_in_chunks(df, chunk_size=10000):
    """
    Process large DataFrames in chunks to reduce memory usage
    """
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size]

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'excel')

def upload_excel_to_postgresql(database_name, username, password, excel_file_name, primary_key_column, host, port='5432', selected_sheets=None):
    try:
        current_dir = os.getcwd()
        excel_file_path = os.path.join(current_dir, excel_file_name)

        # PERFORMANCE OPTIMIZATION: Use connection pooling settings
        conn = psycopg2.connect(
            dbname=database_name, 
            user=username, 
            password=password, 
            host=host, 
            port=port,
            # Performance optimizations
            connect_timeout=30,
            application_name="excel_upload_optimized"
        )
        
        # PERFORMANCE OPTIMIZATION: Set session parameters for better performance
        cur = conn.cursor()
        cur.execute("SET synchronous_commit = OFF")  # Faster commits
        cur.execute("SET commit_delay = 0")          # Reduce commit delays
        cur.execute("SET work_mem = '256MB'")        # Increase work memory for sorting/hashing
        
        if not excel_file_path.lower().endswith('.xlsx'):
            return "Error: Only Excel files with .xlsx extension are supported."

        xls = pd.ExcelFile(excel_file_path)

        directory_name = os.path.splitext(os.path.basename(excel_file_name))[0]
        directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
        os.makedirs(directory_path, exist_ok=True)
        

        for sheet_name in selected_sheets:
            sheet_name_cleaned = sheet_name.strip('"').strip()
            if sheet_name_cleaned not in xls.sheet_names:
                print(f"Sheet '{sheet_name_cleaned}' not found in the Excel file. Skipping...")
                continue
                
            # PERFORMANCE OPTIMIZATION: Read Excel with optimized parameters
            df = pd.read_excel(
                excel_file_name, 
                sheet_name=sheet_name_cleaned,
                engine='openpyxl',  # Generally faster for .xlsx files
                # Use dtype inference for better performance
                dtype_backend='numpy_nullable'
            )
            
            table_name = sanitize_column_name(sheet_name_cleaned)
            df.columns = [sanitize_column_name(col) for col in df.columns]
            print(f"Columns in {sheet_name}: {df.columns}")
            print(f"DataFrame shape: {df.shape}")
            
            # NEW: Process date columns and standardize formats to date objects
            df = detect_and_process_date_columns(df)
    
            table_exists = validate_table_structure(cur, table_name, df)
            print(f"Table exists for {table_name}: {table_exists}")
            is_table_in_use = check_table_usage(cur, table_name)
            print(f"Table '{table_name}' is in use: {is_table_in_use}")

            if table_exists and is_table_in_use == False:
                # If table exists and is NOT in use, handle missing columns
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
                existing_columns = [row[0] for row in cur.fetchall()]
                print(f"Existing columns in table '{table_name}': {existing_columns}")

                # Find columns to delete (existing in table but missing in uploaded file)
                columns_to_delete = [col for col in existing_columns if col not in df.columns]

                for col in columns_to_delete:
                    alter_query = sql.SQL('ALTER TABLE {} DROP COLUMN {}').format(
                        sql.Identifier(table_name),
                        sql.Identifier(col)
                    )
                    try:
                        cur.execute(alter_query)
                        print(f"Deleted column '{col}' from table '{table_name}'.")
                    except Exception as e:
                        print(f"Error deleting column '{col}' from table '{table_name}': {str(e)}")
                        continue

            if table_exists:
                print(f"Validating and adding missing columns to table '{table_name}'.")
                
                # Detect and add missing columns
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
                existing_columns = [row[0] for row in cur.fetchall()]

                missing_columns = [col for col in df.columns if col not in existing_columns]

                for col in missing_columns:
                    # Use the new determine_column_type function
                    col_type = determine_column_type(df, col)

                    alter_query = sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
                        sql.Identifier(table_name),
                        sql.Identifier(col),
                        sql.SQL(col_type)
                    )

                    try:
                        cur.execute(alter_query)
                        print(f"Added column '{col}' with type '{col_type}' to table '{table_name}'.")
                    except Exception as e:
                        print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")
                        continue

            else:
                print(f"Creating table '{table_name}'.")
                
                # Use the new determine_column_type function for table creation
                column_types = []
                for col in df.columns:
                    col_type = determine_column_type(df, col)
                    column_types.append((col, col_type))

                columns = ', '.join(f'"{col}" {col_type}' for col, col_type in column_types)
                create_table_query = sql.SQL('CREATE TABLE {} ({})').format(sql.Identifier(table_name),
                                                                              sql.SQL(columns))
                try:
                    print(f"Create Table Query: {create_table_query.as_string(cur)}")
                    cur.execute(create_table_query)
                except Exception as e:
                    print(f"Error creating table {table_name}: {str(e)}")
                    continue

                if primary_key_column in df.columns:
                    alter_table_query = sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
                        sql.Identifier(table_name), sql.Identifier(primary_key_column))
                    try:
                        cur.execute(alter_table_query)
                    except Exception as e:
                        print(f"Error adding primary key to {table_name}: {str(e)}")
                        continue

            # Check for duplicate primary keys
            duplicate_primary_keys = df[df.duplicated(subset=[primary_key_column], keep=False)][primary_key_column].tolist()
            if duplicate_primary_keys:
                return f"Error: Duplicate primary key values found: {', '.join(map(str, duplicate_primary_keys))}"

            # PERFORMANCE OPTIMIZATION: Bulk delete using IN clause with batching
            primary_key_values = df[primary_key_column].tolist()
            
            # Process deletions in batches to avoid query size limits
            delete_batch_size = 1000
            total_deleted = 0
            
            for i in range(0, len(primary_key_values), delete_batch_size):
                batch_values = primary_key_values[i:i + delete_batch_size]
                
                delete_query = sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
                    sql.Identifier(table_name),
                    sql.Identifier(primary_key_column)
                )
                
                cur.execute(delete_query, (batch_values,))
                total_deleted += cur.rowcount

            if total_deleted > 0:
                print(f"Deleted {total_deleted} rows with matching primary key values in table '{table_name}'.")
            else:
                print("No rows were deleted.")

            # PERFORMANCE OPTIMIZATION: Choose best insertion method based on data size
            print(f"Starting data insertion for {len(df)} rows...")
            
            if len(df) > 50000:
                # For very large datasets, try COPY FROM first
                print("Using COPY FROM for large dataset...")
                copy_success = bulk_insert_with_copy(cur, conn, table_name, df)
                
                if not copy_success:
                    print("COPY FROM failed, using optimized batch insert...")
                    optimized_batch_insert(cur, conn, table_name, df, batch_size=5000)
            else:
                # For smaller datasets, use optimized batch insert
                print("Using optimized batch insert...")
                optimized_batch_insert(cur, conn, table_name, df, batch_size=2000)

            # Save Excel file
            file_name = f"{table_name}.xlsx"
            file_path = os.path.join(directory_path, file_name)
            df.to_excel(file_path, index=False)

        # Handle datasource table creation and insertion
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename = 'datasource'
            );
        """)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            cur.execute("""
                CREATE TABLE datasource (
                    id SERIAL PRIMARY KEY,
                    data_source_name VARCHAR(255),
                    data_source_path VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        
                );
            """)
            print("Created 'datasource' table.")

        insert_query = sql.SQL("""
                    INSERT INTO datasource (data_source_name, data_source_path)
                    VALUES (%s, %s)
                """)
        cur.execute(insert_query, (directory_name, directory_path))
        
        # Final commit
        conn.commit()
        
        # Reset session parameters
        cur.execute("RESET synchronous_commit")
        cur.execute("RESET commit_delay")
        cur.execute("RESET work_mem")

        cur.close()
        conn.close()

        return "Upload successful"
        
    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
        return f"Error: {str(e)}"
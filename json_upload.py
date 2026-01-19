
import psycopg2
import json
import os
import pandas as pd
import traceback
from psycopg2 import sql
import re
import json
import traceback
from tqdm import tqdm # For progress bars during batch operations
from psycopg2.extras import execute_values

# def sanitize_column_name(name):
#     """Sanitizes column names to be SQL-friendly."""
#     if isinstance(name, str):
#         # Remove non-alphanumeric (except underscore), replace spaces with underscore, convert to lowercase
#         return re.sub(r'\W+', '_', name).lower()
#     return name


# def apply_masking(df, mask_settings):
#     if not isinstance(mask_settings, dict):
#         return df
#     print("df",df)

#     for column, config in mask_settings.items():

#         # ✅ skip null / invalid configs
#         if not isinstance(config, dict):
#             continue

#         if column not in df.columns:
#             continue

#         mask_from = config.get("maskFrom")
#         characters = int(config.get("characters", 0))

#         if characters <= 0 or mask_from not in ("start", "end"):
#             continue

#         def mask_value(val):
#             if pd.isna(val):
#                 return val

#             val = str(val)

#             if len(val) <= characters:
#                 return "•" * len(val)

#             if mask_from == "start":
#                 return "•" * characters + val[characters:]

#             return val[:-characters] + "•" * characters

#         df[column] = df[column].apply(mask_value)

#     return df
def sync_masked_columns(cur, table_name, df):
    """
    Drop masked columns from DB that are not present in the current upload
    """
    # Masked columns in DataFrame
    df_masked_cols = {col for col in df.columns if col.endswith("_masked") or col.endswith("__masked")}

    # Fetch masked columns from DB
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name LIKE %s
        """,
        (table_name, '%masked')
    )

    rows = cur.fetchall()

    # ✅ Defensive: handle empty or malformed rows
    db_masked_cols = set()
    for row in rows:
        if row and len(row) > 0:
            db_masked_cols.add(row[0])

    # Columns to drop
    cols_to_drop = db_masked_cols - df_masked_cols

    for col in cols_to_drop:
        cur.execute(
            sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
                sql.Identifier(table_name),
                sql.Identifier(col)
            )
        )
        print(f"🗑 Dropped obsolete masked column '{col}' from '{table_name}'")
def apply_masking(df, mask_settings):
    if not isinstance(mask_settings, dict):
        return df

    for column, config in mask_settings.items():

        if not isinstance(config, dict):
            continue

        if column not in df.columns:
            continue

        mask_from = config.get("maskFrom")
        characters = int(config.get("characters", 0))

        if characters <= 0 or mask_from not in ("start", "end"):
            continue

        masked_col = f"{column}__masked"

        def mask_value(val):
            if pd.isna(val):
                return val

            val = str(val)

            if len(val) <= characters:
                return "•" * len(val)

            if mask_from == "start":
                return "•" * characters + val[characters:]

            return val[:-characters] + "•" * characters

        # ✅ Create masked column (DO NOT overwrite original)
        df[masked_col] = df[column].apply(mask_value)

    return df



def sanitize_column_name(header):
    """Sanitize column names to lowercase, replace non-alphanumeric with _, collapse multiple _"""
    sanitized = str(header).lower().strip()
    sanitized = re.sub(r'[^a-z0-9]+', '_', sanitized)  # replace non-alphanumeric with _
    sanitized = re.sub(r'_+', '_', sanitized)           # collapse multiple underscores
    sanitized = sanitized.strip('_')                    # remove leading/trailing _
    return sanitized


def determine_sql_data_type(series):
    """Determines the appropriate PostgreSQL data type for a pandas Series."""
    if pd.api.types.is_integer_dtype(series):
        # Use BIGINT to prevent overflow with large IDs/integers
        return 'BIGINT'
    elif pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP WITHOUT TIME ZONE' # Standard for datetime objects

    elif pd.api.types.is_string_dtype(series) or series.dtype == 'object':
        # Attempt to detect strict YYYY-MM-DD dates in string columns
        date_pattern_strict = r"^\d{4}-\d{2}-\d{2}$"
        if series.astype(str).str.match(date_pattern_strict).all() and not series.isnull().all():
            return 'DATE'
        # For all other strings, use TEXT for flexible length
        return 'TEXT'
    else:
        # Default for unknown types or mixed types that Pandas classifies as 'object'
        return 'TEXT'

import numpy as np # Make sure numpy is imported

def convert_to_correct_type(value, expected_type):
    """
    Converts a value to the specified Python type matching the SQL type for psycopg2,
    explicitly handling NumPy types.
    """
    if pd.isna(value):
        return None # Convert pandas NA to None for PostgreSQL NULL

    # --- Add explicit handling for NumPy types first ---
    if isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)
    elif isinstance(value, np.bool_):
        return bool(value)
    # --- End NumPy handling ---

    try:
        if expected_type == 'BIGINT':
            # After NumPy handling, this should primarily catch standard Python ints
            return int(value)
        elif expected_type == 'DOUBLE PRECISION':
            return float(value)
        elif expected_type == 'BOOLEAN':
            return bool(value)
        elif expected_type == 'TIMESTAMP WITHOUT TIME ZONE' or expected_type == 'DATE':
            if hasattr(value, 'to_pydatetime'):
                return value.to_pydatetime()
            # Attempt to convert other date-like objects/strings
            return pd.to_datetime(value, errors='coerce').to_pydatetime() if pd.notna(pd.to_datetime(value, errors='coerce')) else None
        elif expected_type == 'TEXT' or expected_type.startswith('VARCHAR'):
            return str(value)
        else:
            return value # Return as-is for unhandled types (e.g., standard Python types not needing special conversion)
    except (ValueError, TypeError):
        return None # Return None if conversion fails

def validate_table_structure(cur, table_name):
    """Checks if a table exists in the public schema of the database."""
    try:
        cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}')")
        return cur.fetchone()[0]
    except psycopg2.Error as e:
        print(f"Error checking table existence for '{table_name}': {e}")
        return False

def identify_primary_key(df):
    """Identifies a suitable primary key column in a DataFrame."""
    for col in df.columns:
        # Check for non-null values and uniqueness
        if df[col].notna().all() and df[col].duplicated().sum() == 0:
            # Prefer numeric or string types for PKs
            if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                return col
    return None

def update_table_structure_for_json(cur, conn, table_name, df, db_primary_key_column):
    """
    Updates the structure of an existing table based on the DataFrame's columns.
    Handles adding new columns, dropping missing columns, and altering column types.
    """
    print(f"Updating structure for existing table '{table_name}'.")

    # Get existing columns from DB
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
    existing_columns_info = cur.fetchall()
    existing_columns_names = {row[0] for row in existing_columns_info}
    existing_columns_types = {row[0]: row[1] for row in existing_columns_info}

    # 1. Identify and drop columns that exist in DB but not in new DF (unless it's the primary key)
    columns_to_drop = [
        col for col in existing_columns_names
        if col not in df.columns and col != db_primary_key_column
    ]
    for col in columns_to_drop:
        print(f"Dropping column '{col}' from table '{table_name}'. It's not in the new data.")
        try:
            cur.execute(sql.SQL('ALTER TABLE {} DROP COLUMN {} CASCADE').format( # CASCADE to drop dependent objects
                sql.Identifier(table_name),
                sql.Identifier(col)
            ))
            conn.commit()
        except Exception as e:
            print(f"Error dropping column '{col}': {e}")
            conn.rollback() # Rollback on DDL error

    # 2. Identify and add/alter columns present in new DF
    for col in df.columns:
        target_col_type = determine_sql_data_type(df[col])
        if col not in existing_columns_names:
            # Column doesn't exist, add it
            print(f"Adding new column '{col}' with type '{target_col_type}' to table '{table_name}'.")
            try:
                cur.execute(sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
                    sql.Identifier(table_name),
                    sql.Identifier(col),
                    sql.SQL(target_col_type)
                ))
                conn.commit()
            except Exception as e:
                print(f"Error adding column '{col}': {e}")
                conn.rollback()
        else:
            # Column exists, check if type needs altering (e.g., VARCHAR to TEXT)
            existing_db_type = existing_columns_types.get(col)
            if existing_db_type and existing_db_type != target_col_type:
                # Check for specific upgrades (e.g., VARCHAR(N) to TEXT or wider VARCHAR)
                is_type_upgrade = False
                if existing_db_type.startswith('character varying') and target_col_type == 'TEXT':
                    is_type_upgrade = True
                elif existing_db_type.startswith('character varying') and target_col_type.startswith('VARCHAR'):
                    # Compare lengths for VARCHAR to VARCHAR upgrade
                    try:
                        existing_len = int(re.search(r'\d+', existing_db_type).group())
                        target_len = int(re.search(r'\d+', target_col_type).group())
                        if target_len > existing_len:
                            is_type_upgrade = True
                    except (AttributeError, ValueError): # No length found, or invalid length
                        pass # Cannot compare, skip or consider as mismatch

                if is_type_upgrade:
                    print(f"Altering column '{col}' type from '{existing_db_type}' to '{target_col_type}'.")
                    try:
                        # Use USING clause to safely cast existing data if possible
                        alter_type_query = sql.SQL('ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}').format(
                            sql.Identifier(table_name),
                            sql.Identifier(col),
                            sql.SQL(target_col_type),
                            sql.Identifier(col),
                            sql.SQL(target_col_type)
                        )
                        cur.execute(alter_type_query)
                        conn.commit()
                    except Exception as e:
                        print(f"Error altering column '{col}' type from '{existing_db_type}' to '{target_col_type}': {e}")
                        conn.rollback()
                # else: print(f"Column '{col}' already has matching type '{existing_db_type}'.")


    # 3. Ensure primary key constraint is correct and applied
    if db_primary_key_column:
        cur.execute(f"""
            SELECT conname FROM pg_constraint pc
            JOIN pg_class pt ON pt.oid = pc.conrelid
            WHERE pt.relname = %s AND contype = 'p';
        """, (table_name,))
        pk_constraint_info = cur.fetchone()

        if pk_constraint_info:
            print(f"Table '{table_name}' already has a primary key constraint '{pk_constraint_info[0]}'.")
            # You might want to check if it's on the correct column and alter if not.
            # This is more complex and often involves dropping and re-adding the constraint.
        else:
            print(f"Adding PRIMARY KEY constraint on column '{db_primary_key_column}' to table '{table_name}'.")
            try:
                cur.execute(sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
                    sql.Identifier(table_name),
                    sql.Identifier(db_primary_key_column)
                ))
                conn.commit()
            except Exception as e:
                print(f"Error adding primary key constraint: {e}")
                conn.rollback()

def flatten_json_data(data):
    """
    Flattens a JSON array of objects with nested structures.
    Converts nested arrays into rows while preserving parent fields.
    """
    def flatten_record(record, parent_key='', parent_row={}):
        """
        Recursively flattens a single JSON object into a row(s). Handles nested objects and lists.
        """
        rows = []
        current_row = parent_row.copy() # Start with inherited parent fields

        # Collect scalar values and process nested structures
        nested_lists_to_process = []
        for key, value in record.items():
            new_key = f"{parent_key}_{key}" if parent_key else key
            if isinstance(value, dict):
                # Recursively flatten dictionaries, passing current_row to maintain context
                rows.extend(flatten_record(value, new_key, current_row))
                current_row = {} # Reset current_row for this branch if it was extended
            elif isinstance(value, list):
                # Defer list processing to ensure all current_row scalars are gathered
                nested_lists_to_process.append((new_key, value))
            else:
                # For scalar values, add to the current row
                current_row[new_key] = value

        # If there were no nested structures or lists, just return the current_row
        if not nested_lists_to_process and not rows:
            rows.append(current_row)

        # Process deferred nested lists
        if nested_lists_to_process:
            if not rows: # If no rows from nested dicts, start with current_row for list expansion
                rows.append(current_row)
            
            new_expanded_rows = []
            for existing_row in rows: # Expand each previously generated row with list content
                for list_key, list_value in nested_lists_to_process:
                    for item in list_value:
                        row_copy = existing_row.copy()
                        if isinstance(item, dict):
                            new_expanded_rows.extend(flatten_record(item, list_key, row_copy))
                        else:
                            row_copy[list_key] = item
                            new_expanded_rows.append(row_copy)
            rows = new_expanded_rows if new_expanded_rows else rows # Use expanded rows if any, else keep original

        return rows if rows else [current_row] # Ensure at least one row is returned


    flattened_rows = []
    for record in data:
        flattened_rows.extend(flatten_record(record))

    # Convert to pandas DataFrame, filling missing keys with NaN
    df = pd.DataFrame(flattened_rows)
    return df


# def insert_or_update_batch(cur, conn, table_name, df, primary_key_column, batch_size=5000):
#     """
#     Performs batch UPSERT (INSERT ... ON CONFLICT DO UPDATE) or batch INSERT
#     into the specified table.
#     """
#     if df.empty:
#         print(f"No data to insert into '{table_name}' in this batch.")
#         return

#     # Get actual columns from the database and identify SERIAL columns
#     cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
#     db_cols_info = cur.fetchall()
#     db_columns = [c_info[0] for c_info in db_cols_info]
#     serial_cols_in_db = [c_info[0] for c_info in db_cols_info if 'nextval' in c_info[1] or 'serial' in c_info[1].lower() or 'identity' in c_info[1].lower()]

#     # Columns from DataFrame that will be explicitly inserted/updated.
#     # Exclude DB-managed SERIAL/IDENTITY PKs from the list of columns to provide values for.
#     cols_to_insert = [col for col in db_columns if col in df.columns and col not in serial_cols_in_db]

#     # Prepare values for batch insertion/update
#     rows_to_process = []
#     for _, row in df.iterrows():
#         processed_row = []
#         for col in cols_to_insert:
#             # Ensure value is converted to a type psycopg2 can handle
#             expected_db_type = next((info[1] for info in db_cols_info if info[0] == col), 'TEXT') # Get actual DB type for conversion
#             processed_row.append(convert_to_correct_type(row.get(col), expected_db_type))
#         rows_to_process.append(tuple(processed_row))

#     if primary_key_column and primary_key_column in db_columns: # Ensure PK is actually in DB schema
#         # UPSERT scenario:
#         # Construct the ON CONFLICT DO UPDATE SET clause
#         update_columns = [col for col in cols_to_insert if col != primary_key_column]
#         update_set_clause = sql.SQL(', ').join(
#             sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
#             for col in update_columns
#         )

#         upsert_query = sql.SQL("""
#             INSERT INTO {} ({}) VALUES %s
#             ON CONFLICT ({}) DO UPDATE SET {}
#         """).format(
#             sql.Identifier(table_name),
#             sql.SQL(', ').join(map(sql.Identifier, cols_to_insert)),
#             sql.Identifier(primary_key_column),
#             update_set_clause
#         )
#         print(f"Using UPSERT strategy for table '{table_name}' on primary key '{primary_key_column}'.")
#         query_str = upsert_query.as_string(conn)
#     else:
#         # Simple INSERT scenario (e.g., for tables with SERIAL PK where no explicit PK is provided from data)
#         print(f"Using simple INSERT strategy for table '{table_name}'. No UPSERT possible without a conflict target.")
#         insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
#             sql.Identifier(table_name),
#             sql.SQL(', ').join(map(sql.Identifier, cols_to_insert))
#         )
#         query_str = insert_query.as_string(conn)

#     try:
#         with tqdm(total=len(rows_to_process), desc=f"Inserting into {table_name}", unit="row") as pbar:
#             for i in range(0, len(rows_to_process), batch_size):
#                 batch = rows_to_process[i:i + batch_size]
#                 execute_values(
#                     cur,
#                     query_str,
#                     batch,
#                     page_size=batch_size
#                 )
#                 conn.commit() # Commit each batch
#                 pbar.update(len(batch))

#         print(f"Processed {len(rows_to_process)} rows in table '{table_name}'.")
#     except Exception as e:
#         print(f"Error during batch operation: {str(e)}")
#         traceback.print_exc()
#         conn.rollback() # Rollback the current transaction on error
#         raise e



def insert_or_update_batch(cur, conn, table_name, df, primary_key_column, batch_size=5000):
    """
    Performs batch UPSERT (INSERT ... ON CONFLICT DO UPDATE) or batch INSERT
    into the specified table.
    """
    rows_inserted = 0
    rows_updated = 0
    rows_deleted = 0
    rows_skipped = 0
    if df.empty:
        print(f"No data to insert into '{table_name}' in this batch.")
        rows_skipped = 0
        return
        

    # Get actual columns from the database and identify SERIAL columns
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
    db_cols_info = cur.fetchall()
    db_columns = [c_info[0] for c_info in db_cols_info]
    serial_cols_in_db = [c_info[0] for c_info in db_cols_info if 'nextval' in c_info[1] or 'serial' in c_info[1].lower() or 'identity' in c_info[1].lower()]

    # Columns from DataFrame that will be explicitly inserted/updated.
    # Exclude DB-managed SERIAL/IDENTITY PKs from the list of columns to provide values for.
    cols_to_insert = [col for col in db_columns if col in df.columns and col not in serial_cols_in_db]

    # Prepare values for batch insertion/update
    rows_to_process = []
    for _, row in df.iterrows():
        processed_row = []
        for col in cols_to_insert:
            # Ensure value is converted to a type psycopg2 can handle
            expected_db_type = next((info[1] for info in db_cols_info if info[0] == col), 'TEXT') # Get actual DB type for conversion
            processed_row.append(convert_to_correct_type(row.get(col), expected_db_type))
        rows_to_process.append(tuple(processed_row))
    


    if primary_key_column and primary_key_column in db_columns: # Ensure PK is actually in DB schema
        # UPSERT scenario:
        # Construct the ON CONFLICT DO UPDATE SET clause
        update_columns = [col for col in cols_to_insert if col != primary_key_column]
        update_set_clause = sql.SQL(', ').join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
            for col in update_columns
        )

        upsert_query = sql.SQL("""
            INSERT INTO {} ({}) VALUES %s
            ON CONFLICT ({}) DO UPDATE SET {}
        """).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, cols_to_insert)),
            sql.Identifier(primary_key_column),
            update_set_clause
        )
        print(f"Using UPSERT strategy for table '{table_name}' on primary key '{primary_key_column}'.")
        query_str = upsert_query.as_string(conn)
    else:
        # Simple INSERT scenario (e.g., for tables with SERIAL PK where no explicit PK is provided from data)
        print(f"Using simple INSERT strategy for table '{table_name}'. No UPSERT possible without a conflict target.")
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, cols_to_insert))
        )
        query_str = insert_query.as_string(conn)

    try:
        with tqdm(total=len(rows_to_process), desc=f"Inserting into {table_name}", unit="row") as pbar:
            for i in range(0, len(rows_to_process), batch_size):
                batch = rows_to_process[i:i + batch_size]
                execute_values(
                    cur,
                    query_str,
                    batch,
                    page_size=batch_size
                )
                conn.commit() # Commit each batch
                rows_inserted += len(batch)
                pbar.update(len(batch))
                
                

        print(f"Processed {len(rows_to_process)} rows in table '{table_name}'.")
    except Exception as e:
        print(f"Error during batch operation: {str(e)}")
        traceback.print_exc()
        conn.rollback() # Rollback the current transaction on error
        raise e
    return rows_inserted, rows_updated, rows_deleted, rows_skipped

# --- Main Function (Updated) ---

def upload_json_to_postgresql(database_name, username, password, json_file_path, user_provided_primary_key=None, host='localhost', port='5432',mask_settings=None, table_name=None):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=database_name,
            user=username,
            password=password,
            host=host,
            port=port,
            connect_timeout=30
        )
        cur = conn.cursor()
        # Set session parameters for performance
        cur.execute("SET synchronous_commit = OFF")
        cur.execute("SET commit_delay = 0")
        cur.execute("SET work_mem = '256MB'")
        cur.execute("SET maintenance_work_mem = '512MB'")

        with open(json_file_path, 'r') as f:
            data = json.load(f)

        json_array = None
        if isinstance(data, list):
            json_array = data
        elif isinstance(data, dict):
            # If it's a dict with a single key pointing to a list or another dict, treat that as the main data
            if len(data) == 1:
                potential_array_or_object = list(data.values())[0]
                if isinstance(potential_array_or_object, list):
                    json_array = potential_array_or_object
                elif isinstance(potential_array_or_object, dict):
                    json_array = [potential_array_or_object] # Wrap single dict in a list
            else:
                raise ValueError("If JSON root is an object, it must contain a single key pointing to the array/object data.")
        else:
            raise ValueError("Invalid JSON format. The root element must be an array or an object containing an array/object.")

        df = flatten_json_data(json_array)


        # Sanitize DataFrame column names
        df.columns = [sanitize_column_name(col) for col in df.columns]
        df = apply_masking(df, mask_settings)
        print("mask_settings=None",df)

        # Determine the table name from the JSON file name
        # table_name = os.path.splitext(os.path.basename(json_file_path))[0]
        # table_name = sanitize_column_name(table_name)
        # If table_name not provided, fall back to JSON filename
        if not table_name:
            table_name = os.path.splitext(os.path.basename(json_file_path))[0]
            table_name = sanitize_column_name(table_name)
        else:
            table_name = sanitize_column_name(table_name)
        



        # --- Primary Key Identification Logic ---
        db_primary_key_column = None
        is_serial_pk = False # Flag to indicate if the PK is a DB-managed SERIAL/IDENTITY

        # 1. Try user-provided primary key
        if user_provided_primary_key:
            sanitized_user_pk = sanitize_column_name(user_provided_primary_key)
            if sanitized_user_pk in df.columns and df[sanitized_user_pk].notna().all() and df[sanitized_user_pk].duplicated().sum() == 0:
                db_primary_key_column = sanitized_user_pk
                print(f"Using user-provided primary key: '{db_primary_key_column}'.")
            else:
                print(f"Warning: User-provided primary key '{user_provided_primary_key}' is invalid (not found, has nulls, or duplicates).")

        # 2. If no valid user-provided PK, try to identify from data
        if db_primary_key_column is None:
            identified_pk = identify_primary_key(df)
            if identified_pk:
                db_primary_key_column = identified_pk
                print(f"Identified primary key from data: '{db_primary_key_column}'.")
            else:
                # 3. Fallback to DB-managed 'id' (SERIAL/IDENTITY)
                db_primary_key_column = 'id'
                is_serial_pk = True
                print("No natural primary key found. Database will manage 'id' as SERIAL primary key.")

        # --- Table Structure Validation and Update ---
        table_exists = validate_table_structure(cur, table_name)

        if not table_exists:
            
            print(f"Creating table '{table_name}'.")
            column_defs = []
            for col in df.columns:
                col_type = determine_sql_data_type(df[col])
                column_defs.append(f'"{col}" {col_type}')

            # Add primary key definition to CREATE TABLE statement
            if is_serial_pk:
                # Add 'id' as GENERATED BY DEFAULT AS IDENTITY for auto-incrementing
                # Ensure it's not already in column_defs if it exists in DF for some reason
                if db_primary_key_column not in [col_def.split(' ')[0].strip('"') for col_def in column_defs]:
                    column_defs.insert(0, f'"{db_primary_key_column}" BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY')
            elif db_primary_key_column and db_primary_key_column in df.columns:
                # If it's a natural PK from the data, append PRIMARY KEY to its definition
                for i, col_def in enumerate(column_defs):
                    if col_def.startswith(f'"{db_primary_key_column}"'):
                        column_defs[i] += ' PRIMARY KEY'
                        break
                else: # Should not happen if db_primary_key_column is from df.columns
                    print(f"Warning: Primary key column '{db_primary_key_column}' not found in initial column definitions for PK application.")

            create_table_query = sql.SQL('CREATE TABLE {} ({})').format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(sql.SQL(col_def) for col_def in column_defs)
            )
            try:
                print(f"Generated CREATE TABLE Query: {create_table_query.as_string(conn)}")
                cur.execute(create_table_query)
                conn.commit()
                if not validate_table_structure(cur, table_name):
                    raise Exception(f"Table '{table_name}' was not created successfully.")
            except Exception as e:
                print(f"Error creating table {table_name}: {str(e)}")
                traceback.print_exc()
                conn.rollback()
                return f"Error: Could not create table {table_name}. {str(e)}"
        else:
            sync_masked_columns(cur, table_name, df)
            print(f"Table '{table_name}' already exists. Updating its structure if necessary.")
            # Update structure (add/drop columns, alter types, ensure PK constraint)
            update_table_structure_for_json(cur, conn, table_name, df, db_primary_key_column)


        # --- Data Insertion/Update (Batch UPSERT/INSERT) ---
        print(f"Starting data insertion into table '{table_name}'.")

        # If it's a serial PK and we are re-uploading a file that should completely replace data,
        # TRUNCATE might be desired before insert_or_update_batch.
        # For now, `insert_or_update_batch` will either UPSERT (if a natural PK exists in DF)
        # or simply INSERT (if PK is SERIAL and not provided in DF).
        # insert_or_update_batch(cur, conn, table_name, df, db_primary_key_column, batch_size=5000)
        rows_inserted, rows_updated, rows_deleted, rows_skipped = insert_or_update_batch(
            cur, conn, table_name, df, db_primary_key_column, batch_size=5000
        )
        # --- Save details in the datasource table ---
        json_save_path = os.path.join('uploads', f"{table_name}.json")
        os.makedirs(os.path.dirname(json_save_path), exist_ok=True)
        # Save the *processed* DataFrame to JSON
        df.to_json(json_save_path, orient='records', indent=4)
        print(f"Processed JSON saved to: {json_save_path}")

        # Create a datasource table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS datasource (
                id SERIAL PRIMARY KEY,
                data_source_name VARCHAR(255) UNIQUE, -- Added UNIQUE constraint
                data_source_path VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit() # Commit DDL for datasource table

        # Check if data source entry already exists to avoid duplicates and update path
        cur.execute("SELECT id FROM datasource WHERE data_source_name = %s;", (table_name,))
        existing_data_source_entry = cur.fetchone()

        if existing_data_source_entry:
            print(f"Data source entry for '{table_name}' already exists. Updating path.")
            cur.execute("""
                UPDATE datasource SET data_source_path = %s WHERE data_source_name = %s;
            """, (json_save_path, table_name))
        else:
            print(f"Adding new data source entry for '{table_name}'.")
            cur.execute("""
                INSERT INTO datasource (data_source_name, data_source_path)
                VALUES (%s, %s);
            """, (table_name, json_save_path))

        conn.commit() # Commit DML for datasource table

        # return "Upload successful"
        # --- Operation Summary ---
        operation_summary = []
        if rows_inserted > 0:
            operation_summary.append("insert")
        if rows_updated > 0:
            operation_summary.append("update")
        if rows_deleted > 0:
            operation_summary.append("delete")
        if rows_skipped > 0:
            operation_summary.append("skip")
        if not operation_summary:
            operation_summary.append("no_change")

        # ✅ Return detailed summary
        return {
            "message": "Upload successful",
            "table_name": table_name,
            "actions_performed": operation_summary,
            "rows_inserted": rows_inserted,
            "rows_deleted": rows_deleted,
            "rows_updated": rows_updated,
            "rows_skipped": rows_skipped
        }
    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
        if conn:
            conn.rollback() # Ensure rollback on any unhandled exception
        return f"Error: {str(e)}"
    finally:
        # Always reset session parameters and close connection
        if cur:
            try:
                cur.execute("RESET synchronous_commit")
                cur.execute("RESET commit_delay")
                cur.execute("RESET work_mem")
                cur.execute("RESET maintenance_work_mem")
            except Exception as e:
                print(f"Error resetting session parameters: {e}")
            finally:
                cur.close()
        if conn:
            conn.close()
            print("Database connection closed.")
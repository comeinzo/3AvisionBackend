import psycopg2
import xml.etree.ElementTree as ET
import os
import pandas as pd
import traceback
from psycopg2 import sql
import re
import numpy as np
from tqdm import tqdm  # For progress bars during batch operations
from psycopg2.extras import execute_values

# def sanitize_column_name(name):
#     """Sanitizes column names to be SQL-friendly."""
#     if isinstance(name, str):
#         # Remove non-alphanumeric (except underscore), replace spaces with underscore, convert to lowercase
#         return re.sub(r'\W+', '_', name).lower()
#     return name
def sanitize_column_name(header):
    """Sanitize column names to lowercase, replace non-alphanumeric with _, collapse multiple _"""
    sanitized = str(header).lower().strip()
    sanitized = re.sub(r'[^a-z0-9]+', '_', sanitized)  # replace non-alphanumeric with _
    sanitized = re.sub(r'_+', '_', sanitized)           # collapse multiple underscores
    sanitized = sanitized.strip('_')                    # remove leading/trailing _
    return sanitized

def determine_sql_data_type(series):
    """
    Determines the appropriate PostgreSQL data type for a pandas Series.
    Intelligently analyzes values to determine the best type.
    Supports: BIGINT, DOUBLE PRECISION, BOOLEAN, DATE, TIMESTAMP, VARCHAR, TEXT
    """
    # Remove null values for analysis
    non_null_series = series.dropna()
    
    if len(non_null_series) == 0:
        return 'VARCHAR(255)'  # Default for all-null columns
    
    # Check for datetime types first (before attempting numeric conversions)
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP WITHOUT TIME ZONE'
    
    # Try to infer type from actual values
    sample = non_null_series.head(100)  # Check first 100 non-null values
    
    # Check if all values look like integers
    try:
        int_check = sample.astype(str).str.strip().apply(lambda x: str(x).lstrip('-').isdigit() if pd.notna(x) else True)
        if int_check.all():
            # All values are integers - use BIGINT
            return 'BIGINT'
    except:
        pass
    
    # Check if all values look like floats (integers or decimals)
    try:
        float_check = sample.astype(str).str.strip().apply(
            lambda x: _is_float(x) if pd.notna(x) else True
        )
        if float_check.all():
            return 'DOUBLE PRECISION'
    except:
        pass
    
    # Check for boolean values
    try:
        bool_values = set(non_null_series.astype(str).str.lower().unique())
        if bool_values.issubset({'true', 'false', '1', '0', 't', 'f', 'y', 'n', 'yes', 'no'}):
            return 'BOOLEAN'
    except:
        pass
    
    # Check for dates (YYYY-MM-DD format)
    try:
        date_pattern_strict = r"^\d{4}-\d{2}-\d{2}$"
        date_check = sample.astype(str).str.match(date_pattern_strict)
        if date_check.all():
            return 'DATE'
    except:
        pass
    
    # Check for timestamps
    try:
        date_pattern_timestamp = r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}"
        timestamp_check = sample.astype(str).str.match(date_pattern_timestamp)
        if timestamp_check.all():
            return 'TIMESTAMP WITHOUT TIME ZONE'
    except:
        pass
    
    # Determine VARCHAR length based on max string length
    try:
        max_length = non_null_series.astype(str).str.len().max()
        # Use VARCHAR with appropriate length, cap at 1000 for large texts
        if max_length <= 50:
            return 'VARCHAR(50)'
        elif max_length <= 100:
            return 'VARCHAR(100)'
        elif max_length <= 255:
            return 'VARCHAR(255)'
        elif max_length <= 1000:
            return 'VARCHAR(1000)'
        else:
            return 'TEXT'  # Use TEXT for very long strings
    except:
        return 'VARCHAR(255)'  # Default fallback

def _is_float(value):
    """Helper function to check if a value is a valid float."""
    try:
        float(str(value).strip())
        return True
    except (ValueError, AttributeError):
        return False

def convert_to_correct_type(value, expected_type):
    """
    Converts a value to the specified Python type matching the SQL type for psycopg2,
    explicitly handling NumPy types and string-based conversions.
    """
    if pd.isna(value) or value is None:
        return None  # Convert pandas NA to None for PostgreSQL NULL
    
    # Convert to string for processing
    str_value = str(value).strip() if value is not None else ""
    
    # Empty strings become NULL
    if not str_value:
        return None

    # --- Add explicit handling for NumPy types first ---
    if isinstance(value, np.integer):
        if expected_type == 'BIGINT':
            return int(value)
        elif expected_type in ('DOUBLE PRECISION', 'NUMERIC'):
            return float(value)
        elif expected_type == 'BOOLEAN':
            return bool(value)
        else:
            return str(value)
    elif isinstance(value, np.floating):
        if expected_type == 'DOUBLE PRECISION':
            return float(value)
        elif expected_type == 'BIGINT':
            return int(float(value))
        elif expected_type == 'BOOLEAN':
            return bool(value)
        else:
            return str(value)
    elif isinstance(value, np.bool_):
        return bool(value)
    # --- End NumPy handling ---

    try:
        if expected_type == 'BIGINT':
            # Try to convert to integer
            try:
                return int(float(str_value))
            except (ValueError, OverflowError):
                return None
                
        elif expected_type == 'DOUBLE PRECISION' or expected_type == 'NUMERIC':
            # Try to convert to float
            try:
                return float(str_value)
            except ValueError:
                return None
                
        elif expected_type == 'BOOLEAN':
            # Handle boolean values
            bool_value = str_value.lower()
            if bool_value in ('true', '1', 't', 'y', 'yes', 'on'):
                return True
            elif bool_value in ('false', '0', 'f', 'n', 'no', 'off'):
                return False
            else:
                return None
                
        elif expected_type == 'DATE':
            # Try to parse as date
            try:
                parsed_date = pd.to_datetime(str_value, errors='coerce')
                if pd.notna(parsed_date):
                    return parsed_date.date()
                return None
            except:
                return None
                
        elif expected_type == 'TIMESTAMP WITHOUT TIME ZONE' or expected_type == 'TIMESTAMP':
            # Try to parse as datetime
            try:
                if hasattr(value, 'to_pydatetime'):
                    return value.to_pydatetime()
                parsed_datetime = pd.to_datetime(str_value, errors='coerce')
                if pd.notna(parsed_datetime):
                    return parsed_datetime.to_pydatetime()
                return None
            except:
                return None
                
        elif expected_type == 'TEXT':
            # Return as text (no length limit)
            return str_value
            
        elif expected_type.startswith('VARCHAR') or expected_type.startswith('character varying'):
            # Handle VARCHAR(n) or character varying(n) - extract length and truncate if needed
            try:
                import re as regex
                match = regex.search(r'[Vv][Aa][Rr][Cc][Hh][Aa][Rr].*\((\d+)\)', expected_type)
                if not match:
                    match = regex.search(r'character varying\((\d+)\)', expected_type, regex.IGNORECASE)
                if match:
                    max_length = int(match.group(1))
                    return str_value[:max_length]  # Truncate if exceeds max length
            except:
                pass
            return str_value
            
        else:
            # Default: return as string
            return str_value
            
    except Exception as e:
        print(f"Warning: Could not convert value '{value}' to type '{expected_type}': {str(e)}")
        return None  # Return None if conversion fails

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

def update_table_structure_for_xml(cur, conn, table_name, df, db_primary_key_column):
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
            cur.execute(sql.SQL('ALTER TABLE {} DROP COLUMN {} CASCADE').format(
                sql.Identifier(table_name),
                sql.Identifier(col)
            ))
            conn.commit()
        except Exception as e:
            print(f"Error dropping column '{col}': {e}")
            conn.rollback()

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
            # Column exists, check if type needs altering
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
                    except (AttributeError, ValueError):
                        pass

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

def flatten_xml_data(root):
    """
    Flattens nested XML structure into a list of dictionaries.
    Converts XML elements and attributes into rows for tabular storage.
    """
    def parse_element(element):
        """
        Parse a single XML element into a dictionary.
        Handles nested elements, attributes, and text content.
        """
        data = {}

        # Add element attributes
        if element.attrib:
            for attr_key, attr_value in element.attrib.items():
                data[attr_key] = attr_value

        # Process child elements
        child_elements = list(element)
        
        if child_elements:
            # Has child elements - process them
            for child in child_elements:
                child_key = child.tag
                
                # Check if this tag already exists (multiple children with same tag)
                if child_key in data:
                    # If it's not a list yet, convert it
                    if not isinstance(data[child_key], list):
                        data[child_key] = [data[child_key]]
                    data[child_key].append(parse_element(child))
                else:
                    child_data = parse_element(child)
                    # If child has only text (no sub-elements), extract just the text
                    if len(child_data) == 1 and 'text' in child_data:
                        data[child_key] = child_data['text']
                    else:
                        data[child_key] = child_data
        else:
            # No child elements - this is a leaf node, get its text
            if element.text and element.text.strip():
                data['text'] = element.text.strip()

        return data

    rows = []
    
    # Check if root has multiple children of same type - if so, iterate through them
    root_children_by_tag = {}
    for child in root:
        if child.tag not in root_children_by_tag:
            root_children_by_tag[child.tag] = []
        root_children_by_tag[child.tag].append(child)
    
    # If root has only one type of child and multiple instances, treat each as a row
    if len(root_children_by_tag) == 1:
        tag = list(root_children_by_tag.keys())[0]
        children = root_children_by_tag[tag]
        if len(children) > 1:
            # Multiple children with same tag - treat each as a separate row
            for child in children:
                row_data = parse_element(child)
                if isinstance(row_data, dict) and 'text' not in row_data:
                    # Flatten the dictionary to handle nested structure
                    flattened_row = flatten_dict(row_data)
                    rows.append(flattened_row)
                else:
                    rows.append(row_data)
        else:
            # Single child
            row_data = parse_element(root)
            flattened_row = flatten_dict(row_data)
            rows.append(flattened_row)
    else:
        # Root has multiple different child tags - parse entire root
        root_data = parse_element(root)
        flattened_root = flatten_dict(root_data)
        rows.append(flattened_root)

    # Convert to pandas DataFrame, filling missing keys with NaN
    df = pd.DataFrame(rows)
    return df

def flatten_dict(data, parent_key='', sep='_'):
    """
    Flatten nested dictionaries into a single-level dictionary.
    """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # For lists, take the first element or skip
            if v and isinstance(v[0], dict):
                items.extend(flatten_dict(v[0], new_key, sep=sep).items())
            elif v:
                items.append((new_key, v[0]))
        else:
            items.append((new_key, v))
    return dict(items)

def insert_or_update_batch(cur, conn, table_name, df, primary_key_column, batch_size=5000):
    """
    Performs batch UPSERT (INSERT ... ON CONFLICT DO UPDATE) or batch INSERT
    into the specified table. Fetches actual column types from database.
    """
    rows_inserted = 0
    rows_updated = 0
    rows_deleted = 0
    rows_skipped = 0
    
    if df.empty:
        print(f"No data to insert into '{table_name}' in this batch.")
        return 0, 0, 0, 0

    # Get actual columns from the database and identify SERIAL columns
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
    db_cols_info = cur.fetchall()
    db_columns = [c_info[0] for c_info in db_cols_info]
    serial_cols_in_db = [c_info[0] for c_info in db_cols_info if 'nextval' in c_info[1] or 'serial' in c_info[1].lower() or 'identity' in c_info[1].lower()]
    
    # Create a mapping of column name to its database type for conversion
    db_type_map = {c_info[0]: c_info[1] for c_info in db_cols_info}

    # Columns from DataFrame that will be explicitly inserted/updated.
    # Exclude DB-managed SERIAL/IDENTITY PKs from the list of columns to provide values for.
    cols_to_insert = [col for col in db_columns if col in df.columns and col not in serial_cols_in_db]

    print(f"Columns to insert: {cols_to_insert}")
    print(f"Database type map: {db_type_map}")

    # Prepare values for batch insertion/update with proper type conversion
    rows_to_process = []
    for idx, (_, row) in enumerate(df.iterrows()):
        processed_row = []
        try:
            for col in cols_to_insert:
                # Get the actual database type for this column
                db_type = db_type_map.get(col, 'TEXT')
                value = row.get(col)
                
                # Convert value based on actual database type
                converted_value = convert_to_correct_type(value, db_type)
                processed_row.append(converted_value)
            
            rows_to_process.append(tuple(processed_row))
        except Exception as e:
            print(f"Error converting row {idx}: {str(e)}")
            traceback.print_exc()
            continue

    if not rows_to_process:
        print(f"No valid rows to insert into '{table_name}'.")
        return 0, 0, 0, len(df)

    # Determine if we can do UPSERT based on primary key constraints
    can_upsert = False
    if primary_key_column and primary_key_column in db_columns and primary_key_column not in serial_cols_in_db:
        # Check if there's a unique constraint on the primary key column
        cur.execute(f"""
            SELECT conname FROM pg_constraint pc
            JOIN pg_class pt ON pt.oid = pc.conrelid
            WHERE pt.relname = %s AND (contype = 'p' OR contype = 'u');
        """, (table_name,))
        constraints = cur.fetchall()
        can_upsert = len(constraints) > 0
        print(f"Can UPSERT: {can_upsert}, Constraints: {constraints}")

    if can_upsert:
        # UPSERT scenario:
        # Construct the ON CONFLICT DO UPDATE SET clause
        update_columns = [col for col in cols_to_insert if col != primary_key_column]
        if update_columns:
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
            # No other columns to update besides PK
            print(f"No columns to update for UPSERT. Using simple INSERT.")
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, cols_to_insert))
            )
            query_str = insert_query.as_string(conn)
    else:
        # Simple INSERT scenario
        print(f"Using simple INSERT strategy for table '{table_name}'. No UPSERT possible.")
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, cols_to_insert))
        )
        query_str = insert_query.as_string(conn)

    try:
        with tqdm(total=len(rows_to_process), desc=f"Inserting into {table_name}", unit="row") as pbar:
            for i in range(0, len(rows_to_process), batch_size):
                batch = rows_to_process[i:i + batch_size]
                try:
                    execute_values(
                        cur,
                        query_str,
                        batch,
                        page_size=batch_size
                    )
                    conn.commit()  # Commit each batch
                    rows_inserted += len(batch)
                    pbar.update(len(batch))
                except Exception as batch_error:
                    print(f"Error inserting batch: {str(batch_error)}")
                    traceback.print_exc()
                    conn.rollback()
                    continue

        print(f"Successfully processed {rows_inserted} rows in table '{table_name}'.")
    except Exception as e:
        print(f"Error during batch operation: {str(e)}")
        traceback.print_exc()
        conn.rollback()
        raise e
    
    return rows_inserted, rows_updated, rows_deleted, rows_skipped

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

def upload_xml_to_postgresql(database_name, username, password, xml_file_path, user_provided_primary_key=None, host='localhost', port='5432',mask_settings=None, table_name=None):
    """
    Uploads XML data to PostgreSQL database.
    
    Args:
        database_name: PostgreSQL database name
        username: Database username
        password: Database password
        xml_file_path: Path to XML file
        user_provided_primary_key: Optional primary key column name
        host: Database host (default: localhost)
        port: Database port (default: 5432)
        table_name: Optional table name (defaults to XML filename)
    
    Returns:
        Dictionary with upload summary or error message
    """
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

        # Parse XML file
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
        except ET.ParseError as e:
            return f"Error: Invalid XML file. {str(e)}"

        # Flatten XML data
        df = flatten_xml_data(root)
        
        if df.empty:
            return "Error: No data found in XML file."

        # Sanitize DataFrame column names
        df.columns = [sanitize_column_name(col) for col in df.columns]
        df = apply_masking(df, mask_settings)

        # Determine the table name from the XML file name
        if not table_name:
            table_name = os.path.splitext(os.path.basename(xml_file_path))[0]
            table_name = sanitize_column_name(table_name)
        else:
            table_name = sanitize_column_name(table_name)

        # --- Primary Key Identification Logic ---
        db_primary_key_column = None
        is_serial_pk = False  # Flag to indicate if the PK is a DB-managed SERIAL/IDENTITY

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
                if db_primary_key_column not in [col_def.split(' ')[0].strip('"') for col_def in column_defs]:
                    column_defs.insert(0, f'"{db_primary_key_column}" BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY')
            elif db_primary_key_column and db_primary_key_column in df.columns:
                # If it's a natural PK from the data, append PRIMARY KEY to its definition
                for i, col_def in enumerate(column_defs):
                    if col_def.startswith(f'"{db_primary_key_column}"'):
                        column_defs[i] += ' PRIMARY KEY'
                        break
                else:
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
            update_table_structure_for_xml(cur, conn, table_name, df, db_primary_key_column)

        # --- Data Insertion/Update (Batch UPSERT/INSERT) ---
        print(f"Starting data insertion into table '{table_name}'.")

        rows_inserted, rows_updated, rows_deleted, rows_skipped = insert_or_update_batch(
            cur, conn, table_name, df, db_primary_key_column, batch_size=5000
        )

        # --- Save details in the datasource table ---
        xml_save_path = os.path.join('uploads', f"{table_name}.json")
        os.makedirs(os.path.dirname(xml_save_path), exist_ok=True)
        # Save the *processed* DataFrame to JSON
        df.to_json(xml_save_path, orient='records', indent=4)
        print(f"Processed XML data saved to: {xml_save_path}")

        # Create a datasource table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS datasource (
                id SERIAL PRIMARY KEY,
                data_source_name VARCHAR(255) UNIQUE,
                data_source_path VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

        # Check if data source entry already exists to avoid duplicates and update path
        cur.execute("SELECT id FROM datasource WHERE data_source_name = %s;", (table_name,))
        existing_data_source_entry = cur.fetchone()

        if existing_data_source_entry:
            print(f"Data source entry for '{table_name}' already exists. Updating path.")
            cur.execute("""
                UPDATE datasource SET data_source_path = %s WHERE data_source_name = %s;
            """, (xml_save_path, table_name))
        else:
            print(f"Adding new data source entry for '{table_name}'.")
            cur.execute("""
                INSERT INTO datasource (data_source_name, data_source_path)
                VALUES (%s, %s);
            """, (table_name, xml_save_path))

        conn.commit()

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
            conn.rollback()
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


import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql
from psycopg2.extras import execute_values
import traceback
from tqdm import tqdm
from user_upload import get_db_connection # Assuming this provides a standard connection
from flask import Flask, request, jsonify # Assuming these are part of a larger Flask app
import io
import time
import random
from config import DB_NAME, USER_NAME, PASSWORD, HOST, PORT
# --- Existing Helper Functions (kept for context, some are already optimized) ---
def is_table_used_in_charts(table_name):
    conn = get_db_connection(dbname=DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM table_chart_save WHERE selected_table = %s
        )
        """,
        (table_name,)
    )
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result


def check_table_usage(cur, table_name):
    if not table_name:
        raise ValueError("Table name is required")
    table_name = table_name.strip('"').strip("'")
    print(f"Received table name: {table_name}")
    is_in_use = is_table_used_in_charts(table_name)
    print("is_in_use", is_in_use)
    return is_in_use

def sanitize_column_name(col_name):
    if isinstance(col_name, str):
        return re.sub(r'\W+', '_', col_name).lower()
    else:
        return col_name

def clean_data(df):
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip().replace('', pd.NA)
    return df

def identify_primary_key(df):
    for col in df.columns:
        if df[col].is_unique and not df[col].isnull().any():
            return col
    return None

def validate_table_structure(cur, table_name):
    try:
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
        table_exists = cur.fetchone()[0]
        return table_exists
    except psycopg2.errors.InFailedSqlTransaction:
        print("Warning: Transaction is aborted, cannot validate table structure.")
        return False
    except Exception as e:
        print(f"Error validating table structure: {e}")
        return False

def preprocess_dates(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            temp_series = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            if not temp_series.isna().all() and (temp_series.notna().sum() / len(df)) > 0.8:
                df[col] = temp_series.dt.strftime('%Y-%m-%d')
    return df

def determine_sql_data_type(series):
    if pd.api.types.is_integer_dtype(series):
        return 'BIGINT'
    elif pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP'
    elif pd.api.types.is_string_dtype(series):
        date_pattern_strict = r"^\d{4}-\d{2}-\d{2}$"
        if series.astype(str).str.match(date_pattern_strict).all() and not series.isnull().all():
            return 'DATE'
        else:
            return 'TEXT'
    else:
        return 'TEXT'

def check_repeating_columns(df):
    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    return duplicate_columns

# Modified bulk_insert_with_copy to use a temporary table for UPSERT
def bulk_insert_with_copy(cur, conn, table_name, df, primary_key_col):
    if not primary_key_col:
        print("Cannot perform COPY-based UPSERT without a primary key column. Falling back to batch insert.")
        return False # Fallback to optimized_batch_insert if no PK for UPSERT

    # Generate a truly unique temporary table name for each call
    # Combine process ID and a timestamp/random string for higher uniqueness
    temp_table_suffix = f"{os.getpid()}_{int(time.time())}_{random.randint(0, 9999)}"
    temp_table_name = sql.Identifier(f"{table_name}_temp_{temp_table_suffix}")

    columns = df.columns.tolist()
    column_defs = []
    for col in columns:
        col_type = determine_sql_data_type(df[col]) # Reuse your existing type determination
        column_defs.append(f'"{col}" {col_type}')

    try:
        # 1. Create a temporary table. Do NOT commit here.
        #    The temp table exists for the duration of the current session/transaction.
        #    ON COMMIT DROP means it will be dropped when the transaction that created it commits/rolls back.
        create_temp_table_query = sql.SQL("CREATE TEMPORARY TABLE {} ({}) ON COMMIT DROP").format(
            temp_table_name,
            sql.SQL(', ').join(sql.SQL(col_def) for col_def in column_defs)
        )
        cur.execute(create_temp_table_query)

        # 2. COPY data into the temporary table
        output = io.StringIO()
        df_copy = df.copy()
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S').replace({pd.NA: '\\N'})
            else:
                df_copy[col] = df_copy[col].astype(str).replace({'None': '\\N', 'nan': '\\N', pd.NA: '\\N'})

        df_copy.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)

        copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')").format(
            temp_table_name, # Copy into temp table
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        cur.copy_expert(copy_query.as_string(conn), output)
        print(f"Copied {len(df)} rows into temporary table '{temp_table_name}'.")

        # 3. Perform UPSERT from temporary table to main table
        update_columns = [col for col in columns if col != primary_key_col]
        
        # CORRECTED: Use EXCLUDED.<column_name> when updating from a SELECT source
        update_set_clause = sql.SQL(', ').join(
            sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(col)) # Use EXCLUDED.column_name
            for col in update_columns
        )
        
        # Build the final UPSERT query from temp table
        if update_columns:
            upsert_query = sql.SQL("""
                INSERT INTO {target_table} ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT ({pk_col}) DO UPDATE SET {update_clause}
            """).format(
                target_table=sql.Identifier(table_name),
                columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                temp_table=temp_table_name,
                pk_col=sql.Identifier(primary_key_col),
                update_clause=update_set_clause
            )
        else: # Only PK column, no other columns to update
            upsert_query = sql.SQL("""
                INSERT INTO {target_table} ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT ({pk_col}) DO NOTHING
            """).format(
                target_table=sql.Identifier(table_name),
                columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                temp_table=temp_table_name,
                pk_col=sql.Identifier(primary_key_col)
            )

        cur.execute(upsert_query)
        rows_upserted = cur.rowcount
        print(f"UPSERTed {rows_upserted} rows from temporary table to '{table_name}'.")
        return True

    except Exception as e:
        print(f"COPY-based UPSERT failed, falling back to batch UPSERT: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback() # Rollback the current transaction for this chunk
        return False
    finally:
        # No explicit DROP TABLE needed due to ON COMMIT DROP.
        # The main loop in upload_csv_to_postgresql will handle `conn.commit()`
        # which will then cause the temp table to be dropped.
        pass

# Modified optimized_batch_insert to support UPSERT
def optimized_batch_insert(cur, conn, table_name, df, primary_key_col, batch_size=5000):
    try:
        rows_to_insert = []
        for _, row in df.iterrows():
            processed_row = []
            for col in df.columns:
                value = row[col]
                if pd.isna(value):
                    processed_row.append(None)
                elif pd.api.types.is_datetime64_any_dtype(df[col]) and pd.notna(value):
                    processed_row.append(value.to_pydatetime() if hasattr(value, 'to_pydatetime') else value)
                else:
                    processed_row.append(value)
            rows_to_insert.append(tuple(processed_row))

        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build the UPSERT clause
        insert_query_base = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        upsert_query = insert_query_base
        if primary_key_col:
            update_columns = [col for col in columns if col != primary_key_col]
            if update_columns:
                update_set_clause = sql.SQL(', ').join(
                    sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(col))
                    for col in update_columns
                )
                upsert_query = sql.SQL("{} ON CONFLICT ({}) DO UPDATE SET {}").format(
                    insert_query_base,
                    sql.Identifier(primary_key_col),
                    update_set_clause
                )
            else: # Only PK column, no other columns to update
                 upsert_query = sql.SQL("{} ON CONFLICT ({}) DO NOTHING").format(
                    insert_query_base,
                    sql.Identifier(primary_key_col)
                )

        print(f"Batch UPSERT query: {upsert_query.as_string(conn)}")

        with tqdm(total=len(rows_to_insert), desc=f"Inserting/Updating into {table_name}", unit="row") as pbar:
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                execute_values(
                    cur,
                    upsert_query.as_string(conn),
                    batch,
                    template=f"({placeholders})",
                    page_size=batch_size
                )
                # Commit less frequently for performance, but ensure commits happen
                if (i // batch_size) % 5 == 0 or (i + batch_size) >= len(rows_to_insert):
                    conn.commit()
                pbar.update(len(batch))

        print(f"Optimized batch UPSERTed {len(rows_to_insert)} rows into table '{table_name}'.")

    except Exception as e:
        print(f"Error during optimized batch insert/update: {str(e)}")
        traceback.print_exc()
        if conn:
            conn.rollback()
        raise e

def process_csv_in_chunks(csv_file_path, chunk_size=50000):
    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, low_memory=False, dtype_backend='numpy_nullable'):
        yield chunk

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'csv')

def upload_csv_to_postgresql(database_name, username, password, csv_file_name, host, port='5432'):
    conn = None
    cur = None
    try:
        current_dir = os.getcwd()
        csv_file_path = os.path.join(current_dir, csv_file_name)

        conn = psycopg2.connect(
            dbname=database_name,
            user=username,
            password=password,
            host=host,
            port=port,
            connect_timeout=30,
            application_name="csv_upload_optimized"
        )
        cur = conn.cursor()
        cur.execute("SET synchronous_commit = OFF")
        cur.execute("SET commit_delay = 0")
        cur.execute("SET work_mem = '256MB'")
        cur.execute("SET maintenance_work_mem = '512MB'")

        if not csv_file_path.lower().endswith('.csv'):
            return "Error: Only CSV files with .csv extension are supported."

        directory_name = os.path.splitext(os.path.basename(csv_file_name))[0]
        directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
        os.makedirs(directory_path, exist_ok=True)

        table_name = sanitize_column_name(directory_name)

        file_size_mb = os.path.getsize(csv_file_path) / (1024 * 1024)
        process_in_chunks = file_size_mb > 100

        initial_df = pd.read_csv(csv_file_path, nrows=1000, low_memory=False, dtype_backend='numpy_nullable')
        print(f"Processing CSV for schema inference: {initial_df.shape[0]} rows.")

        initial_df = preprocess_dates(initial_df)
        initial_df = clean_data(initial_df)
        initial_df.columns = [sanitize_column_name(col) for col in initial_df.columns]

        duplicate_columns = check_repeating_columns(initial_df)
        if duplicate_columns:
            return f"Error: Duplicate columns found in the CSV: {', '.join(duplicate_columns)}"

        primary_key_column_from_csv = identify_primary_key(initial_df)
        db_primary_key_column = 'id'

        if primary_key_column_from_csv:
            print(f"Using column '{primary_key_column_from_csv}' from CSV as primary key.")
            db_primary_key_column = primary_key_column_from_csv
            # Check for duplicates in the initial chunk if using CSV's PK
            duplicate_primary_keys = initial_df[initial_df.duplicated(subset=[db_primary_key_column], keep=False)][db_primary_key_column].tolist()
            if duplicate_primary_keys:
                print(f"Warning: Duplicate primary key values found in CSV for column '{db_primary_key_column}': {', '.join(map(str, duplicate_primary_keys))}. Consider data cleaning or using a database-managed ID if these are not intended for UPSERT.")
                # Decide here: if duplicates in CSV means no natural PK, then fall back to SERIAL 'id'
                # For UPSERT, duplicates in CSV are fine if they represent updates to existing rows.
                # If they are *new* rows that happen to have same ID, then the CSV is malformed or the PK logic is flawed.
                # For now, let's proceed assuming UPSERT handles it, but keep the warning.

        else:
            print("No unique column found in CSV to be used as primary key. PostgreSQL will manage 'id' column.")
            # If 'id' is SERIAL, it manages its own uniqueness. We can't UPSERT on a SERIAL column with CSV data unless we provided the ID.
            # If we intend to UPSERT, we MUST have a natural key from the CSV.
            # If no natural PK, the only reasonable re-upload strategy is to TRUNCATE or append new data (which is not what the error implies).
            # So, if no natural PK is found, and `id` is SERIAL, the current data overwrite logic *must* be considered.
            # For simplicity, if no CSV PK, we will *not* try to UPSERT on 'id' but rather just insert new rows (which will get new IDs).
            # This means if you re-upload the same file, you'll get duplicate rows with new 'id's unless you TRUNCATE.
            # This is a critical design decision. For the purpose of solving the "duplicate key" error,
            # we'll assume 'id' will be used for UPSERT if it's the *chosen* PK and exists in CSV.
            # If 'id' is SERIAL, the only way to avoid conflicts on re-upload is TRUNCATE or only append new data.
            db_primary_key_column = 'id' # Even if it's SERIAL, it's what the DB uses as PK.
                                         # The UPSERT logic will then apply if we use this 'id' column from the CSV.
                                         # But for SERIAL, we typically don't provide the 'id' from CSV.
                                         # This is the tricky part. For SERIAL PK, the CSV should NOT contain 'id'.

        table_exists = validate_table_structure(cur, table_name)
        is_table_in_use = False
        if table_exists:
            is_table_in_use = check_table_usage(cur, table_name)
            print(f"Table '{table_name}' is in use: {is_table_in_use}")

        if table_exists and not is_table_in_use:
            print(f"Table '{table_name}' exists and is not in use. Validating and adjusting columns.")
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
            existing_columns_info = cur.fetchall()
            existing_columns_names = [row[0] for row in existing_columns_info]
            existing_columns_types = {row[0]: row[1] for row in existing_columns_info}

            columns_to_delete = [col for col in existing_columns_names if col not in initial_df.columns and col != db_primary_key_column]
            for col in columns_to_delete:
                alter_query = sql.SQL('ALTER TABLE {} DROP COLUMN {}').format(
                    sql.Identifier(table_name),
                    sql.Identifier(col)
                )
                try:
                    cur.execute(alter_query)
                    print(f"Deleted column '{col}' from table '{table_name}'.")
                    conn.commit()
                except Exception as e:
                    print(f"Error deleting column '{col}' from table '{table_name}': {str(e)}")
                    conn.rollback()
                    continue

            for col in initial_df.columns:
                target_col_type = determine_sql_data_type(initial_df[col])
                if col not in existing_columns_names:
                    alter_query = sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
                        sql.Identifier(table_name),
                        sql.Identifier(col),
                        sql.SQL(target_col_type)
                    )
                    try:
                        cur.execute(alter_query)
                        print(f"Added column '{col}' with type '{target_col_type}' to table '{table_name}'.")
                        conn.commit()
                    except Exception as e:
                        print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")
                        conn.rollback()
                        continue
                else:
                    existing_db_type = existing_columns_types.get(col)
                    if existing_db_type and (
                        (existing_db_type.startswith('character varying') and target_col_type == 'TEXT') or
                        (existing_db_type.startswith('character varying') and target_col_type.startswith('VARCHAR') and
                         int(re.search(r'\d+', existing_db_type).group()) < int(re.search(r'\d+', target_col_type).group()))
                    ):
                        print(f"Attempting to alter type for column '{col}' from '{existing_db_type}' to '{target_col_type}'.")
                        alter_type_query = sql.SQL('ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}').format(
                            sql.Identifier(table_name),
                            sql.Identifier(col),
                            sql.SQL(target_col_type),
                            sql.Identifier(col),
                            sql.SQL(target_col_type)
                        )
                        try:
                            cur.execute(alter_type_query)
                            conn.commit()
                            print(f"Updated column '{col}' to '{target_col_type}' in table '{table_name}'.")
                        except Exception as e:
                            print(f"Error updating column type for '{col}' from '{existing_db_type}' to '{target_col_type}': {str(e)}")
                            conn.rollback()
                            continue

            # Ensure PK constraint is correct for existing table
            if db_primary_key_column: # Should always be set to 'id' or CSV's PK
                cur.execute(f"""
                    SELECT conname FROM pg_constraint pc
                    JOIN pg_class pt ON pt.oid = pc.conrelid
                    WHERE pt.relname = %s AND contype = 'p';
                """, (table_name,))
                pk_constraint_info = cur.fetchone()

                if pk_constraint_info:
                    print(f"Table '{table_name}' already has a primary key constraint '{pk_constraint_info[0]}'.")
                else:
                    print(f"Adding primary key constraint on column '{db_primary_key_column}'.")
                    try:
                        cur.execute(sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
                            sql.Identifier(table_name),
                            sql.Identifier(db_primary_key_column)
                        ))
                        conn.commit()
                        print(f"Added PRIMARY KEY constraint on '{db_primary_key_column}' to table '{table_name}'.")
                    except Exception as e:
                        print(f"Error adding primary key constraint: {str(e)}")
                        conn.rollback()


        elif not table_exists:
            print(f"Creating table '{table_name}'.")
            column_defs = []
            for col in initial_df.columns:
                col_type = determine_sql_data_type(initial_df[col])
                column_defs.append(f'"{col}" {col_type}')

            if primary_key_column_from_csv is None:
                # If no natural PK in CSV, add 'id' as SERIAL PRIMARY KEY
                column_defs.insert(0, f'"{db_primary_key_column}" SERIAL PRIMARY KEY')
            else:
                # If CSV has a natural PK, mark that column as PRIMARY KEY
                try:
                    pk_idx = initial_df.columns.get_loc(db_primary_key_column)
                    # Modify the existing column definition to include PRIMARY KEY
                    # This assumes the column is already in column_defs
                    column_defs[pk_idx] = f'"{db_primary_key_column}" {determine_sql_data_type(initial_df[db_primary_key_column])} PRIMARY KEY'
                except KeyError:
                    print(f"Warning: Primary key column '{db_primary_key_column}' not found in DataFrame columns after processing. Cannot set PRIMARY KEY constraint directly.")

            create_table_query = sql.SQL('CREATE TABLE {} ({})').format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(sql.SQL(col_def) for col_def in column_defs)
            )
            try:
                print(f"Create Table Query: {create_table_query.as_string(cur)}")
                cur.execute(create_table_query)
                conn.commit()
                if not validate_table_structure(cur, table_name):
                    raise Exception(f"Table '{table_name}' was not created successfully despite query execution.")
            except Exception as e:
                print(f"Error creating table {table_name}: {str(e)}")
                traceback.print_exc()
                conn.rollback()
                return f"Error: Could not create table {table_name}. {str(e)}"

        # --- Data Insertion (using UPSERT logic) ---
        if not is_table_in_use:
            print(f"Starting data insertion (or UPSERT) for table '{table_name}'...")

            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
            db_cols_info = cur.fetchall()
            final_columns_for_insert_names = [c_info[0] for c_info in db_cols_info]
            serial_cols_in_db = [c_info[0] for c_info in db_cols_info if 'nextval' in c_info[1]]

            # Determine the actual primary key column name used in the DB for UPSERT
            # This is crucial. If 'id' is SERIAL and not in CSV, don't use it for UPSERT from CSV.
            # Only use a CSV-provided primary key for UPSERT.
            actual_pk_for_upsert = primary_key_column_from_csv if primary_key_column_from_csv else None
            # If the DB-managed 'id' is the primary key and no CSV PK was found, we CANNOT UPSERT on 'id' from CSV data.
            # In this case, if we re-upload, we'll either get duplicates (if no TRUNCATE) or new unique IDs.
            # The current error `Key (id)=(1) already exists.` implies 'id' is the PK and you're providing it.
            # If 'id' is SERIAL, the CSV *should not* contain an 'id' column.
            # So, if `primary_key_column_from_csv` is `None` AND `id` is a SERIAL PK in DB, then
            # you need to decide if you want to TRUNCATE or if the re-upload should append.
            if actual_pk_for_upsert is None and 'id' in serial_cols_in_db:
                # If no natural PK from CSV and 'id' is SERIAL, then UPSERT on 'id' is generally not desired
                # as the database generates it. If you're getting `id=1` conflict, it means your CSV *does*
                # contain an 'id' column, and it's being treated as the PK, even if it was intended to be SERIAL.
                # Let's adjust db_primary_key_column based on the detected PK.
                cur.execute(f"""
                    SELECT a.attname
                    FROM   pg_index ix
                    JOIN   pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
                    WHERE  ix.indrelid = '{table_name}'::regclass AND ix.indisprimary;
                """)
                db_pk_col_name_info = cur.fetchone()
                if db_pk_col_name_info:
                    db_primary_key_column = db_pk_col_name_info[0] # This is the actual PK in DB
                    actual_pk_for_upsert = db_primary_key_column
                else:
                    actual_pk_for_upsert = None # No primary key in DB, or cannot be used for UPSERT.

            if process_in_chunks:
                chunk_num = 0
                for chunk_df in process_csv_in_chunks(csv_file_path):
                    chunk_df = preprocess_dates(chunk_df)
                    chunk_df = clean_data(chunk_df)
                    chunk_df.columns = [sanitize_column_name(col) for col in chunk_df.columns]

                    # If the DB's PK column is SERIAL and we don't expect it from CSV, drop it from DF for insert
                    if actual_pk_for_upsert and actual_pk_for_upsert in serial_cols_in_db and actual_pk_for_upsert in chunk_df.columns:
                         # This means CSV has a column that maps to a SERIAL PK in DB, which is problematic for UPSERT
                         # as SERIAL values are generated by DB.
                         # If your CSV *does* have an 'id' column and you want to use it for UPSERT,
                         # then the DB column 'id' should NOT be SERIAL, but rather BIGINT or similar.
                         # The error 'id=1' exists confirms you are providing 'id' from CSV.
                         # So, if CSV has 'id', ensure DB column is not SERIAL if you want to UPSERT on it.
                         # If DB's 'id' *is* SERIAL, and CSV has 'id', you need to drop it from CSV before insert.
                        print(f"Warning: CSV contains '{actual_pk_for_upsert}' which is a SERIAL primary key in DB. Removing from chunk for insertion to let DB manage it.")
                        chunk_df = chunk_df.drop(columns=[actual_pk_for_upsert])
                        actual_pk_for_upsert = None # Cannot UPSERT on a column we just dropped from DF.
                                                     # This effectively means if 'id' is SERIAL, we append new rows.
                                                     # If you want to UPSERT on CSV's 'id', then 'id' in DB should not be SERIAL.

                    cols_to_reindex_and_insert = [col for col in final_columns_for_insert_names if col not in serial_cols_in_db]
                    # This reindex needs to be careful: if CSV provides 'id' for UPSERT, it should be in chunk_df.columns
                    # If DB provides 'id' (SERIAL), it should NOT be in chunk_df.columns for insertion.
                    # We need the `actual_pk_for_upsert` logic to be consistent here.
                    
                    # If we dropped a serial PK, `cols_to_reindex_and_insert` might still contain it.
                    # Re-filter it to match what's left in chunk_df
                    cols_to_reindex_and_insert_final = [col for col in cols_to_reindex_and_insert if col in chunk_df.columns]
                    chunk_df = chunk_df.reindex(columns=cols_to_reindex_and_insert_final)

                    print(f"Processing chunk {chunk_num+1} ({len(chunk_df)} rows)...")
                    if len(chunk_df) > 0:
                        # Pass the detected PK for UPSERT
                        copy_success = bulk_insert_with_copy(cur, conn, table_name, chunk_df, actual_pk_for_upsert)
                        if not copy_success:
                            optimized_batch_insert(cur, conn, table_name, chunk_df, actual_pk_for_upsert, batch_size=5000)
                    chunk_num += 1
                print(f"Finished inserting all chunks into table '{table_name}'.")

            else: # Not chunked processing
                if actual_pk_for_upsert and actual_pk_for_upsert in serial_cols_in_db and actual_pk_for_upsert in initial_df.columns:
                    print(f"Warning: CSV contains '{actual_pk_for_upsert}' which is a SERIAL primary key in DB. Removing from DataFrame for insertion to let DB manage it.")
                    initial_df = initial_df.drop(columns=[actual_pk_for_upsert])
                    actual_pk_for_upsert = None

                cols_to_reindex_and_insert_final = [col for col in final_columns_for_insert_names if col not in serial_cols_in_db and col in initial_df.columns]
                initial_df = initial_df.reindex(columns=cols_to_reindex_and_insert_final)

                if len(initial_df) > 50000:
                    print("Using COPY FROM for large dataset...")
                    copy_success = bulk_insert_with_copy(cur, conn, table_name, initial_df, actual_pk_for_upsert)
                    if not copy_success:
                        print("COPY FROM failed, using optimized batch UPSERT...")
                        optimized_batch_insert(cur, conn, table_name, initial_df, actual_pk_for_upsert, batch_size=5000)
                else:
                    print("Using optimized batch UPSERT...")
                    optimized_batch_insert(cur, conn, table_name, initial_df, actual_pk_for_upsert, batch_size=2000)

            # ... (rest of your code for saving processed CSV, datasource table) ...
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = 'public'
                    AND tablename = 'datasource'
                );
            """)
            datasource_table_exists = cur.fetchone()[0]

            if not datasource_table_exists:
                cur.execute("""
                    CREATE TABLE datasource (
                        id SERIAL PRIMARY KEY,
                        data_source_name VARCHAR(255),
                        data_source_path VARCHAR(255)
                    );
                """)
                print("Created 'datasource' table.")

            cur.execute("SELECT id FROM datasource WHERE data_source_name = %s;", (directory_name,))
            existing_data_source = cur.fetchone()

            if existing_data_source:
                print(f"Data source '{directory_name}' already exists in 'datasource' table. Skipping insertion.")
            else:
                insert_query = sql.SQL("""
                    INSERT INTO datasource (data_source_name, data_source_path)
                    VALUES (%s, %s)
                """)
                cur.execute(insert_query, (directory_name, directory_path))
                print(f"Inserted '{directory_name}' into 'datasource' table.")

            conn.commit()

        else: # This is the block that needs modification
            print(f"Table '{table_name}' is in use, skipping deletion and insertion. No changes made to data.")
            if cur:
                cur.close()
                cur = None # Set to None after closing
            if conn:
                conn.close()
                conn = None # Set to None after closing
            return f"Error: Table '{table_name}' is currently in use by charts. Cannot modify data."

    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
        if conn:
            conn.rollback()
        return f"Error: {str(e)}"
    finally:
        # Only attempt to close if they are not None (meaning they haven't been closed already)
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

    return "Upload successful"
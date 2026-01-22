import psycopg2
from psycopg2 import sql
from flask import jsonify, request
from config import DB_NAME,USER_NAME,PASSWORD,HOST,PORT
from bar_chart import fetch_data_for_duel ,fetch_data_tree,fetchText_data,fetch_data_for_duel_bar
from histogram_utils import generate_histogram_details,handle_column_data_types
from viewChart.viewChart import get_db_connection_view, fetch_chart_data,filter_chart_data,fetch_ai_saved_chart_data
import psycopg2
import pandas as pd
import re  
import ast
import json
from decimal import Decimal
import numpy as np
import paramiko
import socket
import threading
import pyodbc
from statsmodels.tsa.seasonal import seasonal_decompose
from dashboard_save.extensions import socketio
import select
from datetime import datetime

def create_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, 
            user=USER_NAME, 
            password=PASSWORD, 
            host=HOST, 
            port=PORT
        )
        return conn
    except Exception as e:
        print(f"Error creating connection to the database: {e}")
        return None
# def add_wallpaper_column(conn):
#     alter_table_query = """
#     ALTER TABLE table_dashboard
#     ADD COLUMN IF NOT EXISTS wallpaper_id TEXT;
#     """
#     with conn.cursor() as cur:
#         cur.execute(alter_table_query)
#         conn.commit()

# Function to create the table if it doesn't exist


# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     and_mask = pd.Series(True, index=df.index)
#     or_masks = []

#     for col, filter_data in filter_options.items():
#         if col not in df.columns:
#             continue

#         # Normalize filter data
#         if isinstance(filter_data, dict):
#             values = filter_data.get("values", [])
#             operator = filter_data.get("operator", "AND").upper()
#         else:
#             values = filter_data
#             operator = "AND"

#         if not values:
#             continue

#         is_date_col = (
#             pd.api.types.is_datetime64_any_dtype(df[col])
#             or "date" in col.lower()
#         )

#         # Build column mask
#         if is_date_col:
#             temp_dates = pd.to_datetime(df[col], errors="coerce")
#             sample_val = str(values[0])

#             if sample_val.isdigit():  # YEAR
#                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])

#             elif sample_val.startswith("Q"):  # QUARTER
#                 col_mask = temp_dates.dt.quarter.isin(
#                     [int(v.replace("Q", "")) for v in values]
#                 )

#             else:  # MONTH NAME
#                 col_mask = temp_dates.dt.month_name().isin(
#                     [v.strip().capitalize() for v in values]
#                 )
#         else:
#             df[col] = df[col].astype(str).str.strip()
#             values = [str(v).strip() for v in values]
#             col_mask = df[col].isin(values)

#         # Apply operator
#         if operator == "OR":
#             or_masks.append(col_mask)
#         else:  # AND
#             and_mask &= col_mask

#     # Combine AND & OR
#     if or_masks:
#         or_mask = or_masks[0]
#         for m in or_masks[1:]:
#             or_mask |= m
#         return df[and_mask & or_mask]

#     return df[and_mask]
# def apply_and_or_filters(df, filter_options):
#     if not filter_options or not isinstance(filter_options, dict):
#         return df

#     and_mask = pd.Series(True, index=df.index)
#     or_mask = pd.Series(False, index=df.index)

#     has_and = False
#     has_or = False

#     for col, filter_data in filter_options.items():
#         if col not in df.columns:
#             continue

#         # Normalize
#         if isinstance(filter_data, dict):
#             values = filter_data.get("values", [])
#             operator = filter_data.get("operator", "AND").upper()
#         else:
#             values = filter_data
#             operator = "AND"

#         if not values:
#             continue

#         # Date handling
#         is_date_col = (
#             pd.api.types.is_datetime64_any_dtype(df[col])
#             or "date" in col.lower()
#         )

#         if is_date_col:
#             temp_dates = pd.to_datetime(df[col], errors="coerce")
#             sample_val = str(values[0])

#             if sample_val.isdigit():  # YEAR
#                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])

#             elif sample_val.startswith("Q"):  # QUARTER
#                 col_mask = temp_dates.dt.quarter.isin(
#                     [int(v.replace("Q", "")) for v in values]
#                 )

#             else:  # DATE STRING
#                 col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
#         else:
#             df[col] = df[col].astype(str).str.strip()
#             values = [str(v).strip() for v in values]
#             col_mask = df[col].isin(values)

#         # 🔥 Apply operator
#         if operator == "OR":
#             or_mask |= col_mask
#             has_or = True
#         else:
#             and_mask &= col_mask
#             has_and = True

#     # 🔥 FINAL DECISION LOGIC
#     if has_and and has_or:
#         return df[and_mask & or_mask]
#     elif has_or:
#         return df[or_mask]          # ✅ FIX: pure OR
#     else:
#         return df[and_mask]

def apply_and_or_filters(df: pd.DataFrame, filter_options: dict) -> pd.DataFrame:
    """
    Apply AND/OR filters on a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to filter.
        filter_options (dict): Dictionary of filters.
            Example:
            {
                'country': ['Albania', 'India'],                  # AND group
                'region': {'values': ['Asia'], 'operator': 'OR'}, # OR group
                'product': {'values': ['Debit card'], 'operator': 'AND'}
            }

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if not filter_options or not isinstance(filter_options, dict):
        return df

    print("filter_options:", filter_options)

    # Initialize masks
    and_mask = pd.Series(True, index=df.index)
    or_mask = pd.Series(False, index=df.index)

    # Detect if ANY OR exists
    has_or = any(
        isinstance(v, dict) and v.get("operator", "").upper() == "OR"
        for v in filter_options.values()
    )

    for col, filter_data in filter_options.items():
        if col not in df.columns:
            continue

        # Normalize filter_data
        if isinstance(filter_data, dict):
            values = filter_data.get("values", [])
            operator = filter_data.get("operator", "AND").upper()
        else:
            values = filter_data
            operator = "AND"

        if not values:
            continue

        # Build column mask
        if pd.api.types.is_datetime64_any_dtype(df[col]) or "date" in col.lower():
            temp_dates = pd.to_datetime(df[col], errors="coerce")
            sample_val = str(values[0])

            if sample_val.isdigit():
                col_mask = temp_dates.dt.year.isin([int(v) for v in values])
            elif sample_val.startswith("Q"):
                col_mask = temp_dates.dt.quarter.isin(
                    [int(v.replace("Q", "")) for v in values]
                )
            else:
                col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
        else:
            col_mask = df[col].astype(str).isin(map(str, values))

        # ✅ AUTO GROUPING: OR group if any OR exists and column is not 'country'
        if has_or and operator in ("AND", "OR") and col != "country":
            or_mask |= col_mask
        else:
            and_mask &= col_mask

    # ✅ FINAL MASK APPLICATION
    if has_or:
        df_filtered = df[or_mask & and_mask]
    else:
        df_filtered = df[and_mask]

    print("df after filter:", df_filtered)
    return df_filtered


def insert_combined_chart_details(conn, combined_chart_details):
    try:
        # Alter table if any relevant column is VARCHAR(255)
        alter_columns_if_needed(conn)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM table_dashboard")
        last_dashboard_id = cursor.fetchone()[0] or 0
        new_dashboard_id = last_dashboard_id + 1
        insert_query = """
        INSERT INTO table_dashboard 
        (id,user_id, company_name, file_name, chart_ids, position, chart_size, chart_type, chart_Xaxis, chart_Yaxis, chart_aggregate, filterdata, clicked_category, heading, chartcolor, droppableBgColor, opacity, image_ids,project_name)
        VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        """

        
        cursor.execute(insert_query, (
            new_dashboard_id,
            combined_chart_details['user_id'],
            combined_chart_details['company_name'],
            combined_chart_details['file_name'],
            combined_chart_details['chart_ids'],
            str(combined_chart_details['positions']),
            str(combined_chart_details['sizes']),
            str(combined_chart_details['chart_types']),
            str(combined_chart_details['chart_Xaxes']),
            str(combined_chart_details['chart_Yaxes']),
            str(combined_chart_details['chart_aggregates']),
            str(combined_chart_details['filterdata']),
            combined_chart_details['clicked_category'],
            combined_chart_details['heading'],
            json.dumps(combined_chart_details['chartcolor']),
            combined_chart_details['droppableBgColor'],
            combined_chart_details['opacities'],
            json.dumps(combined_chart_details['image_ids']),
            combined_chart_details['project_name']
        ))
        conn.commit()
        cursor.close()
        print("Insert successful.")
    except Exception as e:
        print(f"Error inserting combined chart details: {e}")

def alter_columns_if_needed(conn):
    columns_to_check = [
        'chart_ids', 'position', 'chart_size', 'chart_type',
        'chart_Xaxis', 'chart_Yaxis', 'chart_aggregate',
        'heading', 'chartcolor', 'droppableBgColor',
        'opacity', 'image_ids'
    ]
    
    try:
        cursor = conn.cursor()
        for column in columns_to_check:
            cursor.execute("""
                SELECT character_maximum_length, data_type
                FROM information_schema.columns
                WHERE table_name = 'table_dashboard' AND column_name = %s
            """, (column,))
            result = cursor.fetchone()
            if result:
                char_length, data_type = result
                if data_type == 'character varying' and char_length == 255:
                    print(f"Altering column '{column}' to TEXT...")
                    cursor.execute(f"ALTER TABLE table_dashboard ALTER COLUMN {column} TYPE TEXT")
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error altering columns: {e}")


import json


def get_db_connection(dbname=DB_NAME):
    conn = psycopg2.connect(
        dbname=dbname,
        # user="postgres",
        # password="jaTHU@12",
        # host="localhost",
        # port="5432"
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    return conn

def get_company_db_connection(company_name):

    # This is where you define the connection string
    conn = psycopg2.connect(
        dbname=company_name,  # Ensure this is the correct company database
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    return conn 

def get_dashboard_names(user_id, database_name):
    # Step 1: Get employees reporting to the given user_id from the company database.
    conn_company = get_company_db_connection(database_name)
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
                    #      SELECT employee_id FROM employee_list WHERE reporting_id = %s 
                    # """, (user_id,))
                    cursor.execute("""
                        WITH RECURSIVE subordinates AS (
                            SELECT employee_id, reporting_id,employee_name
                            FROM employee_list
                            WHERE reporting_id = %s

                            UNION

                            SELECT e.employee_id, e.reporting_id,e.employee_name
                            FROM employee_list e
                            INNER JOIN subordinates s ON e.reporting_id = s.employee_id
                        )
                        SELECT employee_id FROM subordinates
                        UNION
                        SELECT %s;
                    """, (user_id,user_id))
                    reporting_employees = [row[0] for row in cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"Error fetching reporting employees: {e}")
        finally:
            conn_company.close()

    # Include the user's own employee_id for fetching their charts.
    # Convert all IDs to integers for consistent data type handling.
    all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]

    # Step 2: Fetch dashboard names for these employees from the datasource database.
    conn_datasource = get_db_connection(DB_NAME)
    dashboard_names = {}

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                # Create placeholders for the IN clause
                placeholders = ', '.join(['%s'] * len(all_employee_ids))
                query = f"""
                    SELECT user_id, file_name FROM table_dashboard
                    WHERE user_id IN ({placeholders}) and company_name = %s
                """
                # cursor.execute(query, tuple(all_employee_ids))
                cursor.execute(query, tuple(map(str, all_employee_ids))+ (database_name,))
                dashboards = cursor.fetchall()

                # Organize dashboards by user_id
                for uid, file_name in dashboards:
                    if uid not in dashboard_names:
                        dashboard_names[uid] = []
                    dashboard_names[uid].append(file_name)
        except psycopg2.Error as e:
            print(f"Error fetching dashboard details: {e}")
        finally:
            conn_datasource.close()

    return dashboard_names

    
# def fetch_project_names(user_id, database_name):
#     conn_company = get_company_db_connection(database_name)
#     all_employee_ids = []

#     if conn_company:
#         try:
#             with conn_company.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT column_name FROM information_schema.columns
#                     WHERE table_name='employee_list' AND column_name='reporting_id'
#                 """)
#                 column_exists = cursor.fetchone()

#                 if column_exists:
#                     cursor.execute("""
#                         WITH RECURSIVE subordinates AS (
#                             SELECT employee_id, reporting_id
#                             FROM employee_list
#                             WHERE reporting_id = %s

#                             UNION

#                             SELECT e.employee_id, e.reporting_id
#                             FROM employee_list e
#                             INNER JOIN subordinates s ON e.reporting_id = s.employee_id
#                         )
#                         SELECT employee_id FROM subordinates
#                         UNION
#                         SELECT %s;
#                     """, (user_id, user_id))
#                     reporting_employees = [row[0] for row in cursor.fetchall()]
#                     all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]
#                 else:
#                     all_employee_ids = [int(user_id)] # If no reporting_id, just include user_id
#         except psycopg2.Error as e:
#             print(f"Error fetching reporting employees for project names: {e}")
#         finally:
#             conn_company.close()

#     conn_datasource = get_db_connection(DB_NAME)
#     project_names = []

#     if conn_datasource and all_employee_ids:
#         try:
#             with conn_datasource.cursor() as cursor:
#                 placeholders = ', '.join(['%s'] * len(all_employee_ids))
#                 query = f"""
#                     SELECT DISTINCT project_name FROM table_dashboard
#                     WHERE user_id IN ({placeholders}) AND company_name = %s;
#                 """
#                 cursor.execute(query, tuple(map(str, all_employee_ids)) + (database_name,))
#                 project_names = [row[0] for row in cursor.fetchall()]
#         except psycopg2.Error as e:
#             print(f"Error fetching project names: {e}")
#         finally:
#             conn_datasource.close()

#     return project_names


def fetch_project_names(user_id, database_name):
    conn_datasource = get_db_connection(DB_NAME)
    project_names = []

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                # Query for user_id and company_name, sorted by the latest entry first (LIFO)
                query = """
                    SELECT project_name 
                    FROM table_dashboard
                    WHERE user_id = %s AND company_name = %s
                    GROUP BY project_name
                    ORDER BY MAX(id) DESC;
                """
                # passed user_id and database_name (which maps to company_name)
                cursor.execute(query, (str(user_id), database_name))
                project_names = [row[0] for row in cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"Error fetching project names: {e}")
        finally:
            conn_datasource.close()

    return project_names



# def get_dashboard_names(user_id, database_name, project_name=None):
#     # Step 1: Get employees reporting to the given user_id from the company database.
#     conn_company = get_company_db_connection(database_name)
#     reporting_employees = []

#     if conn_company:
#         try:
#             with conn_company.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT column_name FROM information_schema.columns
#                     WHERE table_name='employee_list' AND column_name='reporting_id'
#                 """)
#                 column_exists = cursor.fetchone()

#                 if column_exists:
#                     cursor.execute("""
#                         WITH RECURSIVE subordinates AS (
#                             SELECT employee_id, reporting_id
#                             FROM employee_list
#                             WHERE reporting_id = %s

#                             UNION

#                             SELECT e.employee_id, e.reporting_id
#                             FROM employee_list e
#                             INNER JOIN subordinates s ON e.reporting_id = s.employee_id
#                         )
#                         SELECT employee_id FROM subordinates
#                         UNION
#                         SELECT %s;
#                     """, (user_id, user_id))
#                     reporting_employees = [row[0] for row in cursor.fetchall()]
#                 else:
#                     reporting_employees = [] # If no reporting_id, only user_id will be considered below

#         except psycopg2.Error as e:
#             print(f"Error fetching reporting employees: {e}")
#         finally:
#             conn_company.close()

#     # Include the user's own employee_id for fetching their charts.
#     all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]

#     # Step 2: Fetch dashboard names for these employees from the datasource database.
#     conn_datasource = get_db_connection(DB_NAME)
#     dashboard_names = {}

#     if conn_datasource:
#         try:
#             with conn_datasource.cursor() as cursor:
#                 placeholders = ', '.join(['%s'] * len(all_employee_ids))
#                 # query = f"""
#                 #     SELECT user_id, file_name FROM table_dashboard
#                 #     WHERE user_id IN ({placeholders}) AND company_name = %s AND project_name = %s 
#                 # """
#                 # params = tuple(map(str, all_employee_ids)) + (database_name, project_name)
                
#                 # if project_name: # Add project_name filter if provided
#                 #     query += " AND project_name = %s"
#                 #     params += (project_name,)

#                 if project_name:
#                     query = f"""
#                         SELECT user_id, file_name
#                         FROM table_dashboard
#                         WHERE user_id IN ({placeholders})
#                         AND company_name = %s
#                         AND project_name = %s
#                         ORDER BY updated_at DESC;
#                     """
#                     params = tuple(map(str, all_employee_ids)) + (database_name, project_name)

#                 else:
#                     query = f"""
#                         SELECT user_id, file_name
#                         FROM table_dashboard
#                         WHERE user_id IN ({placeholders})
#                         AND company_name = %s
#                         ORDER BY updated_at DESC;
#                     """
#                     params = tuple(map(str, all_employee_ids)) + (database_name,)
#                 cursor.execute(query, params)
#                 dashboards = cursor.fetchall()
#                 print("dashboards",dashboards)

#                 # for uid, file_name in dashboards:
#                 #     if uid not in dashboard_names:
#                 #         dashboard_names[uid] = []
#                 #     dashboard_names[uid].append(file_name)
#         except psycopg2.Error as e:
#             print(f"Error fetching dashboard details: {e}")
#         finally:
#             conn_datasource.close()

#     return dashboards


def get_dashboard_names(user_id, database_name, project_name=None):
    # We only need one connection now, as we aren't looking up subordinates
    conn_datasource = get_db_connection(DB_NAME)
    dashboards = []

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                # Step 1: Construct the base query
                # We switch from "IN (...) " to " = %s " for a single user
                query = """
                    SELECT user_id, file_name
                    FROM table_dashboard
                    WHERE user_id = %s 
                    AND company_name = %s
                """
                params = [user_id, database_name]

                # Step 2: Add optional filter for project_name
                if project_name:
                    query += " AND project_name = %s"
                    params.append(project_name)

                # Step 3: Add ordering
                query += " ORDER BY updated_at DESC;"

                # Step 4: Execute
                cursor.execute(query, tuple(params))
                dashboards = cursor.fetchall()
                
                print("dashboards", dashboards)

        except psycopg2.Error as e:
            print(f"Error fetching dashboard details: {e}")
        finally:
            conn_datasource.close()

    return dashboards



def get_Edit_dashboard_names(user_id, database_name):
    """
    Fetch dashboards created only by the given user_id in the 'datasource' database,
    filtered by the specified company (database_name).
    """
    dashboard_names = {}

    # Step: Fetch dashboard names only for the current user from the datasource database.
    conn_datasource = get_db_connection(DB_NAME)

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                cursor.execute("""
                    SELECT user_id, file_name 
                    FROM table_dashboard 
                    WHERE user_id = %s AND company_name = %s ORDER BY updated_at DESC
                """, (str(user_id), database_name))

                dashboards = cursor.fetchall()

                # Organize dashboards by user_id
                for uid, file_name in dashboards:
                    if uid not in dashboard_names:
                        dashboard_names[uid] = []
                    dashboard_names[uid].append(file_name)

        except psycopg2.Error as e:
            print(f"Error fetching dashboard details: {e}")
        finally:
            conn_datasource.close()

    return dashboard_names





def fetch_external_db_connection(database_name,selected_user):
    try:
        print("company_name",database_name)
        # Connect to local PostgreSQL to get external database connection details
        conn = psycopg2.connect(
           dbname=database_name,  # Ensure this is the correct company database
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
        cursor.execute(query, (selected_user,))
        connection_details = cursor.fetchone()
        conn.close()
        return connection_details
    except Exception as e:
        print(f"Error fetching connection details: {e}")
        return None





def apply_calculations(dataframe, calculationData, x_axis, y_axis):
    def replace_column(match):
        col_name = match.group(1)
        if col_name in dataframe.columns:
            return f"dataframe['{col_name}']"
        else:
            raise ValueError(f"Column {col_name} not found in DataFrame.")

    if not calculationData or not isinstance(calculationData, list):
        return dataframe

    for calc_entry in calculationData:
        calc_formula = calc_entry.get('calculation', '').strip()
        new_col_name = calc_entry.get('columnName', '').strip()
        replace_col_name = calc_entry.get('replaceColumn', new_col_name)

        if not calc_formula or not new_col_name:
            continue  # Skip incomplete entries

        # Apply only if involved in axes
        if new_col_name not in (x_axis or []) and new_col_name not in (y_axis or []):
            continue

        print("Processing formula:", calc_formula)
        try:
            if new_col_name in y_axis:
                print("Y-axis involved:", new_col_name)
            if new_col_name in x_axis:
                print("X-axis involved:", new_col_name)

            # Handle different calculation formulas
            if calc_formula.startswith("if"):
                # match = re.match(
                #     r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$",
                #     calc_formula, re.IGNORECASE
                # )
                match = (
                    re.match(
                        r"if\s*\(\s*(.+?)\s*\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$",
                        calc_formula.strip(),
                        re.IGNORECASE
                    )
                    or
                    re.match(
                        r"if\s*\(\s*(.+?)\s*,\s*'?(.*?)'?\s*,\s*'?(.*?)'?\s*\)$",
                        calc_formula.strip(),
                        re.IGNORECASE
                    )
                )

                if not match:
                    raise ValueError("Invalid IF format")
                condition_expr, then_val, else_val = match.groups()
                condition_expr_python = re.sub(r'\[(.*?)\]', replace_column, condition_expr)
                dataframe[new_col_name] = np.where(
                    eval(condition_expr_python),
                    then_val.strip("'\""),
                    else_val.strip("'\"")
                )

            elif calc_formula.startswith("switch"):
                switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calc_formula, re.IGNORECASE)
                if not switch_match:
                    raise ValueError("Invalid SWITCH format")
                col_name, rest = switch_match.groups()
                if col_name not in dataframe.columns:
                    raise ValueError(f"Column '{col_name}' not found")
                cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
                default_match = re.search(r'default\s*,\s*["\']?(.*?)["\']?$', rest, re.IGNORECASE)
                default_val = default_match.group(1) if default_match else None
                dataframe[new_col_name] = dataframe[col_name].map(dict(cases)).fillna(default_val)
            elif calc_formula.lower().startswith("round"):
                # Match formula: round(<expression>, <decimals>)
                match = re.match(r'round\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)', calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError(
                        "Invalid ROUND format. Use round([col], decimals) or round([col1]/[col2], decimals)"
                    )

                expr, decimals = match.groups()
                decimals = int(decimals)

                # Replace [column] with numeric dataframe references
                def replace_column(match):
                    col_name = match.group(1)
                    if col_name not in dataframe.columns:
                        raise ValueError(f"Missing column: {col_name}")
                    # Convert to numeric
                    dataframe[col_name] = pd.to_numeric(dataframe[col_name], errors='coerce')
                    return f"dataframe['{col_name}']"

                # Replace [col] references in the expression
                expr_python = re.sub(r'\[([^\]]+)\]', replace_column, expr)

                # Handle division by zero safely
                expr_python = re.sub(
                    r"dataframe\['([^']+)'\]\s*/\s*dataframe\['([^']+)'\]",
                    r"np.divide(dataframe['\1'], dataframe['\2'].replace(0, np.nan))",
                    expr_python
                )

                # Evaluate expression safely and round
                try:
                    dataframe[new_col_name] = np.round(eval(expr_python), decimals)
                except Exception as e:
                    print(f"Error evaluating ROUND formula: {e}")
                    dataframe[new_col_name] = np.nan

            

            elif calc_formula.startswith("iferror"):
                match = re.match(r"iferror\s*\((.+?)\s*,\s*(.+?)\)", calc_formula, re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid IFERROR format")
                expr, fallback = match.groups()
                expr_python = re.sub(r'\[(.*?)\]', replace_column, expr)
                try:
                    dataframe[new_col_name] = eval(expr_python)
                    dataframe[new_col_name] = dataframe[new_col_name].fillna(fallback)
                except:
                    dataframe[new_col_name] = fallback

            elif calc_formula.startswith("calculate"):
                match = re.match(
                    r"calculate\s*\(\s*(sum|avg|count|max|min)\s*\(\s*\[([^\]]+)\]\)\s*,\s*\[([^\]]+)\]\s*=\s*['\"](.*?)['\"]\s*\)",
                    calc_formula, re.IGNORECASE
                )
                if not match:
                    raise ValueError("Invalid CALCULATE format")
                agg, value_col, filter_col, filter_val = match.groups()
                df_filtered = dataframe[dataframe[filter_col] == filter_val]
                result_val = {
                    "sum": df_filtered[value_col].astype(float).sum(),
                    "avg": df_filtered[value_col].astype(float).mean(),
                    "count": df_filtered[value_col].count(),
                    "max": df_filtered[value_col].astype(float).max(),
                    "min": df_filtered[value_col].astype(float).min(),
                }[agg]
                dataframe[new_col_name] = result_val

            elif calc_formula.startswith(("maxx", "minx")):
                match = re.match(r"(maxx|minx)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE)
                func, col = match.groups()
                dataframe[new_col_name] = dataframe[col].max() if func.lower() == "maxx" else dataframe[col].min()

            elif calc_formula.startswith("abs"):
                col = re.match(r"abs\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                dataframe[new_col_name] = dataframe[col].abs()

            elif calc_formula.startswith("len"):
                match = re.match(r"len\s*\(\s*(?:\[(.*?)\]|\"(.*?)\")\s*\)", calc_formula, re.IGNORECASE)
                col_name = match.group(1) or match.group(2)
                dataframe[new_col_name] = dataframe[col_name].astype(str).str.len()

            elif calc_formula.startswith("lower"):
                col = re.match(r"lower\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                dataframe[new_col_name] = dataframe[col].astype(str).str.lower()

            elif calc_formula.startswith("upper"):
                col = re.match(r"upper\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                dataframe[new_col_name] = dataframe[col].astype(str).str.upper()

            elif calc_formula.startswith("concat"):
                parts = re.split(r",(?![^\[]*\])", re.match(r"concat\s*\((.+)\)", calc_formula, re.IGNORECASE).group(1))
                concat_parts = []
                for part in parts:
                    part = part.strip()
                    if part.startswith("[") and part.endswith("]"):
                        col = part[1:-1]
                        concat_parts.append(dataframe[col].astype(str))
                    else:
                        concat_parts.append(part.strip('"').strip("'"))
                from functools import reduce
                dataframe[new_col_name] = reduce(lambda x, y: x + y, concat_parts)

            elif re.match(r"(year|month|day)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE):
                func, col = re.match(r"(year|month|day)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).groups()
                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")
                dataframe[new_col_name] = getattr(dataframe[col].dt, func.lower())

            elif calc_formula.startswith("isnull"):
                match = re.match(r"isnull\s*\(\s*\[([^\]]+)\]\s*,\s*['\"]?(.*?)['\"]?\s*\)", calc_formula, re.IGNORECASE)
                col, fallback = match.groups()
                dataframe[new_col_name] = dataframe[col].fillna(fallback)

            elif re.match(r"(?:\[([^\]]+)\]|\"([^\"]+)\")\s+in\s*\((.*?)\)", calc_formula, re.IGNORECASE):
                match = re.match(r"(?:\[([^\]]+)\]|\"([^\"]+)\")\s+in\s*\((.*?)\)", calc_formula, re.IGNORECASE)
                col = match.group(1) or match.group(2)
                values = [v.strip().strip('"').strip("'") for v in match.group(3).split(",")]
                dataframe[new_col_name] = dataframe[col].isin(values)

            elif calc_formula.startswith("datediff"):
                end_col, start_col = re.match(r"datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).groups()
                dataframe[end_col] = pd.to_datetime(dataframe[end_col], errors="coerce")
                dataframe[start_col] = pd.to_datetime(dataframe[start_col], errors="coerce")
                dataframe[new_col_name] = (dataframe[end_col] - dataframe[start_col]).dt.days

            elif calc_formula.startswith("today()"):
                dataframe[new_col_name] = pd.Timestamp.today().normalize()

            elif calc_formula.startswith("now()"):
                dataframe[new_col_name] = pd.Timestamp.now()

            elif calc_formula.startswith("dateadd"):
                col, interval, unit = re.match(
                    r"dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*['\"](day|month|year)['\"]\)", calc_formula, re.IGNORECASE
                ).groups()
                interval = int(interval)
                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")
                if unit == "day":
                    dataframe[new_col_name] = dataframe[col] + pd.to_timedelta(interval, unit="d")
                elif unit == "month":
                    dataframe[new_col_name] = dataframe[col] + pd.DateOffset(months=interval)
                elif unit == "year":
                    dataframe[new_col_name] = dataframe[col] + pd.DateOffset(years=interval)

            elif calc_formula.startswith("formatdate"):
                col, fmt = re.match(r"formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*['\"](.+?)['\"]\)", calc_formula, re.IGNORECASE).groups()
                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")
                # Map format string
                fmt_mapped = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
                dataframe[new_col_name] = dataframe[col].dt.strftime(fmt_mapped)

            elif calc_formula.startswith("replace"):
                col, old, new = re.match(
                    r"replace\s*\(\s*\[([^\]]+)\]\s*,\s*['\"](.*?)['\"]\s*,\s*['\"](.*?)['\"]\)", calc_formula, re.IGNORECASE
                ).groups()
                dataframe[new_col_name] = dataframe[col].astype(str).str.replace(old, new, regex=False)

            elif calc_formula.startswith("trim"):
                col = re.match(r"trim\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                dataframe[new_col_name] = dataframe[col].astype(str).str.strip()
            elif calc_formula.lower().startswith(("sum", "avg", "min", "max")):
                match = re.match(
                    r'(sum|avg|min|max)\s*\(\s*\[([^\]]+)\]\s*\)',
                    calc_formula,
                    re.IGNORECASE
                )
                if not match:
                    raise ValueError("Invalid aggregation format.")

                agg_func, col = match.groups()
                agg_func = agg_func.lower()

                # DO NOT create calculated column
                # Just mark aggregation intent
                aggregation = agg_func
                y_base_column = col

                print(f"Detected aggregation: {aggregation}({col})")

                # IMPORTANT: skip dataframe eval
                skip_calculated_column = True

            else:
                # calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
                # print("Evaluating math formula:", calc_formula_python)
                # Replace column references [col] → temp_df['col']
                calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)

                # Handle COUNT
                calc_formula_python = re.sub(
                    r'count\s*\(\s*(temp_df\[.*?\])\s*\)',
                    r'\1.count()',
                    calc_formula_python,
                    flags=re.IGNORECASE
                )

                # Handle SUM
                calc_formula_python = re.sub(
                    r'sum\s*\(\s*(temp_df\[.*?\])\s*\)',
                    r'\1.sum()',
                    calc_formula_python,
                    flags=re.IGNORECASE
                )

                # Handle AVG
                calc_formula_python = re.sub(
                    r'avg\s*\(\s*(temp_df\[.*?\])\s*\)',
                    r'\1.mean()',
                    calc_formula_python,
                    flags=re.IGNORECASE
                )

                print("Evaluating math formula:", calc_formula_python)
                temp_df[new_col_name] = eval(calc_formula_python)

            print(f"✅ New column '{new_col_name}' created.")

            # else:
            #     # Default fallback: eval with replaced columns
            #     calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
            #     dataframe[new_col_name] = eval(calc_formula_python)

            print(f"✅ Created new column: {new_col_name}")

            # Update axes with new column name if replaced
            if y_axis:
                y_axis = [new_col_name if col == replace_col_name else col for col in y_axis]
            if x_axis:
                x_axis = [new_col_name if col == replace_col_name else col for col in x_axis]
            if skip_calculated_column:
                y_axis = [y_base_column if col == new_col_name else col for col in y_axis]
            

        except Exception as e:
            print(f"Error processing formula '{calc_formula}': {e}")

    return dataframe


def extract_filter_from_category(category_text):
    """
    Handles:
    - region Asia
    - {"region Asia"}
    - {"region":"Asia"}
    - ["region Asia"]
    """

    if not category_text:
        return {}

    # Normalize to string
    category_text = str(category_text).strip()

    # Remove wrapping {} or []
    if (category_text.startswith("{") and category_text.endswith("}")) or \
       (category_text.startswith("[") and category_text.endswith("]")):
        category_text = category_text[1:-1].strip()

    # Remove quotes
    category_text = category_text.replace('"', '').replace("'", "").strip()

    # Handle JSON-style key:value
    if ":" in category_text:
        key, value = category_text.split(":", 1)
    else:
        parts = category_text.split(None, 1)
        if len(parts) != 2:
            return {}
        key, value = parts

    return {
        key.lower().strip(): [value.lower().strip()]
    }


def is_restricted_role(role):
    return role in ["viewer", "report viewer"]


# def apply_employee_category_filter(chart_filters, employee_category_filter):
#     """
#     Restrict ONLY employee category column using intersection
#     """
#     if not employee_category_filter:
#         return chart_filters

#     for col, emp_vals in employee_category_filter.items():
#         if col in chart_filters:
#             chart_filters[col] = list(
#                 set(chart_filters[col]) & set(emp_vals)
#             )
#         else:
#             chart_filters[col] = emp_vals

#     return chart_filters
# def apply_employee_category_filter(chart_filters, employee_category_filter):
#     """
#     Restrict ONLY employee category column using intersection.
#     Case-insensitive comparison is applied.
#     """
#     if not employee_category_filter:
#         return chart_filters

#     for col, emp_vals in employee_category_filter.items():
#         if col in chart_filters:
#             # Keep only values that match (case-insensitive)
#             emp_vals_lower = {str(v).lower() for v in emp_vals}
#             chart_filters[col] = [
#                 v for v in chart_filters[col] 
#                 if str(v).lower() in emp_vals_lower
#             ]
#         else:
#             chart_filters[col] = emp_vals

#     return chart_filters


# def apply_employee_category_filter(chart_filters, employee_category_filter, data_columns=None):
#     """
#     Restrict ONLY employee category column using intersection.
#     Case-insensitive comparison is applied.

#     data_columns: Optional dict of actual column values from the table, e.g.
#     {'region': ['Asia', 'Europe', 'ASIA', 'North America']}
#     """
#     if not employee_category_filter:
#         return chart_filters

#     for col, emp_vals in employee_category_filter.items():
#         emp_vals_lower = {str(v).lower() for v in emp_vals}

#         # If chart_filters already has values for this column, intersect them case-insensitively
#         if col in chart_filters:
#             chart_filters[col] = [
#                 v for v in chart_filters[col]
#                 if str(v).lower() in emp_vals_lower
#             ]
#         # If chart_filters does not have values, take them from data_columns or employee_category_filter
#         else:
#             if data_columns and col in data_columns:
#                 chart_filters[col] = [
#                     v for v in data_columns[col] if str(v).lower() in emp_vals_lower
#                 ]
#             else:
#                 # fallback to lowercase filter values
#                 chart_filters[col] = list(emp_vals_lower)

#     return chart_filters
# def expand_filters_with_actual_values(df, chart_filters, employee_category_filter):
#     """
#     Replace employee_category_filter values with actual values from the dataframe
#     Case-insensitive matching.
    
#     df: pandas DataFrame containing actual column values
#     chart_filters: dict, existing chart filters
#     employee_category_filter: dict, e.g., {'region': ['asia']}
#     """
#     chart_filters_clean = chart_filters.copy()
    
#     for col, filter_vals in employee_category_filter.items():
#         if col not in df.columns:
#             continue  # skip if column not in dataframe
        
#         # Get all unique values in the column (actual values)
#         actual_vals = df[col].dropna().unique()
#         actual_vals_lower = {str(v).lower(): v for v in actual_vals}  # map lowercase → actual
        
#         # Match filter values case-insensitively
#         matched_vals = [actual_vals_lower[v.lower()] for v in filter_vals if v.lower() in actual_vals_lower]
        
#         # Override/add to chart filters
#         chart_filters_clean[col] = matched_vals
    
#     return chart_filters_clean
# def expand_filters_with_actual_values(df, chart_filters, employee_category_filter):
#     chart_filters_clean = chart_filters.copy()

#     # 🔹 Normalize employee_category_filter
#     if isinstance(employee_category_filter, list):
#         normalized_filter = {}
#         for item in employee_category_filter:
#             for k, v in item.items():
#                 normalized_filter.setdefault(k, []).append(v)
#         employee_category_filter = normalized_filter

#     for col, filter_vals in employee_category_filter.items():
#         if col not in df.columns:
#             continue

#         actual_vals = df[col].dropna().unique()
#         actual_vals_lower = {str(v).lower(): v for v in actual_vals}

#         matched_vals = [
#             actual_vals_lower[v.lower()]
#             for v in filter_vals
#             if v.lower() in actual_vals_lower
#         ]

#         chart_filters_clean[col] = matched_vals

#     return chart_filters_clean

# def expand_filters_with_actual_values(df, chart_filters, employee_category_filter):
#     chart_filters_clean = chart_filters.copy()

#     # 🔹 Normalize employee_category_filter (with operator)
#     if isinstance(employee_category_filter, list):
#         normalized_filter = {}

#         for item in employee_category_filter:
#             col = item.get("key")
#             val = item.get("value")
#             op  = item.get("operator", "AND").upper()

#             if col and val:
#                 normalized_filter.setdefault(col, {
#                     "values": [],
#                     "operator": op
#                 })
#                 normalized_filter[col]["values"].append(val)

#         employee_category_filter = normalized_filter

#     # 🔹 Apply filters
#     for col, filter_obj in employee_category_filter.items():
#         if col not in df.columns:
#             continue

#         filter_vals = filter_obj.get("values", [])
#         operator    = filter_obj.get("operator", "AND")

#         actual_vals = df[col].dropna().unique()
#         actual_vals_lower = {str(v).lower(): v for v in actual_vals}

#         matched_vals = [
#             actual_vals_lower[v.lower()]
#             for v in filter_vals
#             if v.lower() in actual_vals_lower
#         ]

#         # 🔹 Preserve operator in chart_filters_clean
#         chart_filters_clean[col] = {
#             "values": matched_vals,
#             "operator": operator
#         }

# #     return chart_filters_clean
# def expand_filters_with_actual_values(df, chart_filters, employee_category_filter):
#     chart_filters_clean = chart_filters.copy()

#     # 🔹 Normalize employee_category_filter (with operator)
#     if isinstance(employee_category_filter, list):
#         normalized_filter = {}

#         for item in employee_category_filter:
#             col = item.get("key")
#             val = item.get("value")
#             op  = item.get("operator", "AND").upper()

#             if col and val:
#                 normalized_filter.setdefault(col, {
#                     "values": [],
#                     "operator": op
#                 })
#                 normalized_filter[col]["values"].append(val)

#         employee_category_filter = normalized_filter

#     # 🔹 Apply filters
#     for col, filter_obj in employee_category_filter.items():
#         if col not in df.columns:
#             continue

#         filter_vals = filter_obj.get("values", [])
#         operator    = filter_obj.get("operator", "AND")

#         actual_vals = df[col].dropna().unique()
#         actual_vals_lower = {str(v).lower(): v for v in actual_vals}

#         matched_vals = []
#         for v in filter_vals:
#             key = str(v).lower()
#             if key in actual_vals_lower:
#                 matched_vals.append(actual_vals_lower[key])
#             else:
#                 matched_vals.append(v)  # 🔹 KEEP user value

#         chart_filters_clean[col] = {
#             "values": matched_vals,
#             "operator": operator
#         }

#     return chart_filters_clean
# def expand_filters_with_actual_values(df, chart_filters, employee_category_filter):
#     chart_filters_clean = {}

#     # 🔹 STEP 1: Normalize chart_filters (base filters)
#     for col, val in chart_filters.items():
#         if isinstance(val, dict):
#             chart_filters_clean[col] = {
#                 "values": val.get("values", []),
#                 "operator": val.get("operator", "AND").upper()
#             }
#         else:
#             chart_filters_clean[col] = {
#                 "values": val,
#                 "operator": "AND"
#             }

#     # 🔹 STEP 2: Normalize employee_category_filter
#     if isinstance(employee_category_filter, list):
#         normalized_filter = {}

#         for item in employee_category_filter:
#             col = item.get("key")
#             val = item.get("value")
#             op  = item.get("operator", "AND").upper()

#             if col and val:
#                 normalized_filter.setdefault(col, {
#                     "values": [],
#                     "operator": op
#                 })
#                 normalized_filter[col]["values"].append(val)

#         employee_category_filter = normalized_filter

#     # 🔹 STEP 3: Merge employee_category_filter into chart_filters_clean
#     for col, filter_obj in employee_category_filter.items():
#         if col not in df.columns:
#             continue

#         filter_vals = filter_obj.get("values", [])
#         operator    = filter_obj.get("operator", "AND").upper()

#         actual_vals = df[col].dropna().unique()
#         actual_vals_lower = {str(v).lower(): v for v in actual_vals}

#         matched_vals = []
#         for v in filter_vals:
#             key = str(v).lower()
#             matched_vals.append(actual_vals_lower.get(key, v))

#         chart_filters_clean[col] = {
#             "values": matched_vals,
#             "operator": operator
#         }

#     return chart_filters_clean
from collections import OrderedDict

def expand_filters_with_actual_values(df, chart_filters, employee_category_filter):
    chart_filters_clean = {}

    # 🔹 STEP 1: Normalize chart_filters (ALWAYS add operator)
    for col, val in chart_filters.items():
        if isinstance(val, dict):
            chart_filters_clean[col] = {
                "values": val.get("values", []),
                "operator": val.get("operator", "AND").upper()
            }
        else:
            chart_filters_clean[col] = {
                "values": val,
                "operator": "AND"
            }

    # 🔹 STEP 2: Normalize employee_category_filter
    if isinstance(employee_category_filter, list):
        normalized_filter = {}

        for item in employee_category_filter:
            col = item.get("key")
            val = item.get("value")
            op  = item.get("operator", "AND").upper()

            if col and val:
                normalized_filter.setdefault(col, {
                    "values": [],
                    "operator": op
                })
                normalized_filter[col]["values"].append(val)

        employee_category_filter = normalized_filter

    # 🔹 STEP 3: Merge employee_category_filter WITH actual value matching
    for col, filter_obj in employee_category_filter.items():
        if col not in df.columns:
            continue

        filter_vals = filter_obj.get("values", [])
        operator    = filter_obj.get("operator", "AND").upper()

        actual_vals = df[col].dropna().unique()
        actual_vals_lower = {str(v).lower(): v for v in actual_vals}

        matched_vals = []
        for v in filter_vals:
            key = str(v).lower()
            matched_vals.append(actual_vals_lower.get(key, v))

        chart_filters_clean[col] = {
            "values": matched_vals,
            "operator": operator
        }

    # 🔹 STEP 4: Reorder → employee_category_filter FIRST
    ordered_filters = OrderedDict()

    for col in employee_category_filter.keys():
        if col in chart_filters_clean:
            ordered_filters[col] = chart_filters_clean[col]

    for col in chart_filters_clean.keys():
        if col not in ordered_filters:
            ordered_filters[col] = chart_filters_clean[col]

    chart_filters_clean = ordered_filters

    # ✅ FINAL DEBUG (single, correct print)
    print("FINAL employee_category_filter:", employee_category_filter)
    print("FINAL chart_filters_clean:", chart_filters_clean)

    return chart_filters_clean



def apply_employee_category_filter(chart_filters, employee_category_filter, data_columns=None):
    """
    Restrict ONLY employee category column using intersection.
    Expands filter to include all case variations present in data_columns.

    data_columns: Optional dict of actual column values from the table, e.g.
    {'region': ['Asia', 'Europe', 'ASIA', 'North America']}
    """
    if not employee_category_filter:
        return chart_filters

    for col, emp_vals in employee_category_filter.items():
        emp_vals_lower = {str(v).lower() for v in emp_vals}

        # If we have actual column data, get all values matching case-insensitively
        if data_columns and col in data_columns:
            chart_filters[col] = [
                val for val in data_columns[col] 
                if str(val).lower() in emp_vals_lower
            ]
        else:
            # fallback: use original employee filter, preserving original cases
            chart_filters[col] = [v for v in emp_vals]
            print("chartfiltere",chart_filters)

    return chart_filters
DB_CONFIG_TEMPLATE = {
    'user': USER_NAME,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT
}

# --- GLOBAL VARIABLES ---
active_listeners = {}
listener_lock = threading.Lock()

# --- REAL-TIME HELPER FUNCTIONS ---

def ensure_trigger_exists(db_name, table_name):
    """Creates the PostgreSQL trigger if missing."""
    print(f"🔍 Ensuring trigger exists for {table_name} in DB: {db_name}")
    try:
        conn = psycopg2.connect(dbname=db_name, **DB_CONFIG_TEMPLATE)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # 1. Generic Notification Function
        cursor.execute("""
            CREATE OR REPLACE FUNCTION notify_chart_update() RETURNS TRIGGER AS $$
            BEGIN
                PERFORM pg_notify('chart_update', TG_TABLE_NAME);
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # 2. Check for Trigger
        trigger_name = f"trg_notify_{table_name}"
        cursor.execute("SELECT 1 FROM pg_trigger WHERE tgname = %s", (trigger_name,))
        
        if not cursor.fetchone():
            print(f"🛠️ Creating trigger for {table_name}")
            # NOTE: Removed quotes around table_name to handle case sensitivity automatically
            cursor.execute(f"""
                CREATE TRIGGER "{trigger_name}"
                AFTER INSERT OR UPDATE OR DELETE ON {table_name}
                FOR EACH ROW EXECUTE FUNCTION notify_chart_update();
            """)
            print(f"✅ Trigger created for {table_name}")
        
        cursor.close(); conn.close()
    except Exception as e:
        print(f"⚠️ Trigger Error: {e}")




def get_dashboards_for_table(table_name, database_name_param):
    """
    Finds all dashboards (charts) that rely on a specific table in a specific database.
    Returns a list of dashboard config objects so we can re-run data fetching.
    """
    conn = create_connection() # Connects to MASTER DB
    if not conn:
        return []
    
    dashboards_to_update = []
    try:
        cursor = conn.cursor()
        # Find charts using this table AND this database
        cursor.execute("SELECT id FROM table_chart_save WHERE selected_table = %s AND database_name = %s", (table_name, database_name_param))
        chart_ids = [row[0] for row in cursor.fetchall()]
        
        if not chart_ids:
            return []

        # database columns from insert: 
        # id, chart_ids, position, chart_type, filterdata, chartcolor, droppableBgColor, opacity, image_ids, file_name, company_name
        
        cursor.execute("""
            SELECT id, chart_ids, position, filterdata, chartcolor, droppableBgColor, opacity, image_ids, 
                   chart_type, user_id, file_name, company_name, filterdata 
            FROM table_dashboard
        """)
        all_dashboards = cursor.fetchall()
        
        for row in all_dashboards:
            (dash_id, dash_chart_ids_str, positions, filterdata, chartcolor, droppableBgColor, 
             opacity, image_ids, dashboard_chart_type, user_id, file_name, company_name, dashboard_filter_raw) = row

            if not dash_chart_ids_str: continue
            
            try:
                curr_ids = list(map(int, re.findall(r'\d+', dash_chart_ids_str)))
            except:
                continue
            
            affected_chart_ids = set(curr_ids).intersection(chart_ids)
            
            if affected_chart_ids:
                dashboards_to_update.append({
                    "dashboard_id": dash_id,
                    "chart_ids": curr_ids, # Keep full list for index mapping
                    "positions": positions,
                    "filter_options": filterdata,
                    "areacolour": chartcolor,
                    "droppableBgColor": droppableBgColor,
                    "opacity": opacity,
                    "image_ids": image_ids,
                    "chart_type": dashboard_chart_type,
                    "user_id": user_id,
                    "file_name": file_name,
                    "company_name": company_name,
                    "dashboard_Filter": dashboard_filter_raw,
                    "view_mode": "view",
                    
                    # Store which IDs are actually affected for filtering
                    "affected_ids_set": affected_chart_ids
                })
                
        cursor.close()
        conn.close()
        return dashboards_to_update
        
    except Exception as e:
        print(f"Error finding dashboards for table {table_name}: {e}")
        return []

def filter_list_by_indices(data_list, indices):
    """Helper to filter a list (or string rep of list) by keeping only specific indices."""
    if not data_list: return []
    
    # Handle string format if necessary (though simple lists are expected here usually)
    is_str = isinstance(data_list, str)
    if is_str:
        try:
            parsed = ast.literal_eval(data_list)
            if not isinstance(parsed, list): parsed = []
        except:
             # Try regex or simple split if AST fails
             parsed = []
             
        data_list = parsed

    if not isinstance(data_list, list): return []
    
    return [data_list[i] for i in indices if i < len(data_list)]

def background_db_listener(db_name):
    """Background thread that listens for DB updates."""
    try:
        conn = psycopg2.connect(dbname=db_name, **DB_CONFIG_TEMPLATE)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("LISTEN chart_update;")
        print(f"✅ [Listener] Started listening to DB: {db_name}")

        while True:
            # Wait 5s for notification
            if select.select([conn], [], [], 5) != ([], [], []):
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    changed_table = notify.payload
                    
                    # ⚠️ Silent unless we find a matching active dashboard
                    # print(f"🔔 TRIGGER FIRED! DB: {db_name} | Table: {changed_table}")
                    
                    # 1. Find affected dashboards (Pass DB Name!)
                    affected_dashboards = get_dashboards_for_table(changed_table, db_name)
                    
                    if not affected_dashboards:
                        # Log only if verbose or debugging specific table issues
                        # print(f"ℹ️ Table {changed_table} updated, but no dashboards use it.")
                        continue

                    # Pre-check: Are ANY of these dashboards active?
                    active_update_count = 0
                    
                    for dash in affected_dashboards:
                         try:
                             # Room 2: Name based (user_id_dashboard_name) match URL structure roughly
                             # URL: /Dashboard_data/<user_id>,<name>/...
                             # We'll use a standardized room name: "dashboard_updates_<user_id>_<file_name>"
                             room_name_str = f"dashboard_{dash['user_id']}_{dash['file_name']}"
                             
                             # ----------------------------------------------------
                             # ACTIVE USER CHECK (Optimization)
                             # ----------------------------------------------------
                             try:
                                 import socketio as sio_lib
                                 namespace = '/'
                                 rooms = socketio.server.manager.rooms
                                 
                                 current_room_participants = set()
                                 if isinstance(rooms, dict):
                                     ns_rooms = rooms.get(namespace, {})
                                     current_room_participants = ns_rooms.get(room_name_str, set())
                                 
                                 if not current_room_participants:
                                     # SILENT SKIP
                                     continue
                                 
                                 # ONLY NOW do we announce the trigger
                                 print(f"🔔 TRIGGER FIRED! DB: {db_name} | Table: {changed_table}")
                                 print(f"👀 ACTIVE USERS DETECTED in {room_name_str}: {len(current_room_participants)}")
                                 active_update_count += 1
                             except Exception as check_err:
                                 print(f"⚠️ Could not check room participants ({check_err}). Proceeding anyway.")
                             # ----------------------------------------------------

                             print(f"🔄 Calculating Data for Dashboard: {dash.get('file_name')} (ID: {dash['dashboard_id']})")
                             
                             # 2. Filter input lists to ONLY include affected charts
                             full_chart_ids = dash['chart_ids']
                             affected_set = dash['affected_ids_set']
                             
                             # Find indices of affected charts
                             indices_to_keep = [i for i, cid in enumerate(full_chart_ids) if cid in affected_set]
                             
                             if not indices_to_keep: continue
                             
                             # Prepare filtered inputs
                             def safe_filter(val, idxs):
                                 try:
                                     lst = val
                                     if isinstance(lst, str):
                                         lst = lst.strip()
                                         if lst.startswith("[") or lst.startswith("{"):
                                             try: import json; lst = json.loads(lst)
                                             except: lst = ast.literal_eval(lst)
                                     
                                     if isinstance(lst, list):
                                          return [lst[i] for i in idxs if i < len(lst)]
                                     return []
                                 except:
                                     return []

                             # Filter everything
                             filtered_chart_ids = [full_chart_ids[i] for i in indices_to_keep]
                             filtered_positions = safe_filter(dash['positions'], indices_to_keep)
                             filtered_filters = safe_filter(dash['filter_options'], indices_to_keep)
                             filtered_areacolour = safe_filter(dash['areacolour'], indices_to_keep)
                             filtered_opacity = safe_filter(dash['opacity'], indices_to_keep)
                             filtered_types = safe_filter(dash['chart_type'], indices_to_keep)
                             
                             # 3. Calculate new data (filtered)
                             # Only calling this IF users are active
                             # 3. Calculate new data (filtered)
                             # Only calling this IF users are active
                             new_data = get_dashboard_view_chart_data(
                                 chart_ids=filtered_chart_ids,
                                 positions=filtered_positions,
                                 filter_options=filtered_filters,
                                 areacolour=filtered_areacolour,
                                 droppableBgColor=dash['droppableBgColor'], 
                                 opacity=filtered_opacity,
                                 image_ids=dash['image_ids'], 
                                 chart_type=filtered_types,
                                 dashboard_Filter=dash['dashboard_Filter'],
                                 view_mode=dash['view_mode'],
                                 company_name=dash['company_name'],
                                 employee_id=dash['user_id']
                             )
                             
                             print(f"🔥 DATA FETCHED for {dash['file_name']}! Count: {len(new_data) if new_data else 0}")
                             print(f"📡 EMITTING 'dashboard_update' to Room: '{room_name_str}'")
                             
                             raw_payload = {
                                 'dashboard_id': dash['dashboard_id'],
                                 'dashboard_name': dash['file_name'],
                                 'user_id': dash['user_id'],
                                 'table': changed_table,
                                 'data': new_data,
                                 'message': 'Data refreshed',
                                 'timestamp': str(datetime.now())
                             }

                             # --- SERIALIZATION HELPER ---
                             def clean_for_json(obj):
                                 if isinstance(obj, (datetime, pd.Timestamp)):
                                     return obj.isoformat()
                                 if isinstance(obj, Decimal):
                                     return float(obj)  
                                 if isinstance(obj, (np.integer, np.int64)):
                                     return int(obj)
                                 if isinstance(obj, (np.floating, np.float64)):
                                     return float(obj)
                                 if isinstance(obj, np.ndarray):
                                     return obj.tolist()
                                 if isinstance(obj, dict):
                                     return {k: clean_for_json(v) for k, v in obj.items()}
                                 if isinstance(obj, list):
                                     return [clean_for_json(i) for i in obj]
                                 if isinstance(obj, tuple):
                                     return [clean_for_json(i) for i in obj]
                                 return obj

                             try:
                                 payload = clean_for_json(raw_payload)
                                 socketio.emit('dashboard_update', payload, room=room_name_str)
                                 print(f"📢 Emitted to room: {room_name_str}")
                             except Exception as serialization_err:
                                 print(f"❌ JSON SERIALIZATION FAILED: {serialization_err}")
                             
                         except Exception as inner_e:
                             print(f"❌ Error updating dashboard {dash.get('dashboard_id')}: {inner_e}")
                             import traceback
                             traceback.print_exc()
                    
    except Exception as e:
        print(f"❌ Listener died for {db_name}: {e}")
        with listener_lock:
            if db_name in active_listeners: del active_listeners[db_name]

def start_dynamic_listener(db_name):
    """Starts the listener thread if not already running."""
    with listener_lock:
        if db_name not in active_listeners or not active_listeners[db_name].is_alive():
            print(f"🚀 Spawning new listener thread for DB: {db_name}")
            t = threading.Thread(target=background_db_listener, args=(db_name,))
            t.daemon = True
            t.start()
            active_listeners[db_name] = t



def get_dashboard_view_chart_data(chart_ids,positions,filter_options,areacolour,droppableBgColor,opacity,image_ids,chart_type,dashboard_Filter,view_mode,company_name,employee_id,temp_filters=None,logged_user_role=None):
    conn = create_connection()  # Initial connection to your main database
    
    # Backup original dashboard_Filter to prevent loop contamination
    original_dashboard_filter_arg = dashboard_Filter
    print("Chart areacolour:", areacolour)
    print("Chart opacity received:", opacity)
    print("positions",positions)
    if conn:
        try:
            print("chart_ids",chart_ids)
            # if isinstance(chart_ids, str):
            #     chart_ids = list(map(int, re.findall(r'\d+', chart_ids)))                
            #     print("chart_ids",chart_ids)
            if isinstance(chart_ids, str):
                chart_ids = list(map(int, re.findall(r'\d+', chart_ids)))
            elif isinstance(chart_ids, set):
                chart_ids = sorted(chart_ids)  # Or throw error if you need strict order


            if isinstance(positions, str):
                positions = ast.literal_eval(positions)  # If positions are passed as a string, convert it to list of dicts
             # Ensure filter_options is a list
            if isinstance(filter_options, str):
                filter_options = ast.literal_eval(filter_options)
            
            if isinstance(areacolour, str):
                # areacolour = [color.strip() for color in re.findall(r'#(?:[0-9a-fA-F]{6})', areacolour)]  # Extract color hex codes into a list
                areacolour = [color.strip() for color in re.findall(r'#[0-9a-fA-F]{3,6}', areacolour)]
            print("_areacolour:", areacolour)
            # chart_opacities = {}
            # if isinstance(opacity, str):
            #     # Remove curly braces and split by comma, then convert to float
            #     clean_opacity_str = opacity.strip('{}')
            #     opacity_values = [float(o.strip()) for o in clean_opacity_str.split(',')]
            #     for idx, chart_id in enumerate(chart_ids):
            #         if idx < len(opacity_values):
            #             chart_opacities[chart_id] = opacity_values[idx]
            #         else:
            #             chart_opacities[chart_id] = 1.0 # Default if not enough opacity values
            chart_opacities = {}

            try:
                if isinstance(opacity, str):
                    if opacity.strip().startswith("["):
                        # JSON-style list string (e.g. "[0.3, 1, 0.4]")
                        opacity_values = json.loads(opacity)
                    else:
                        # PostgreSQL-style array string (e.g. "{0.3,1,0.4}")
                        clean_opacity_str = opacity.strip('{}')
                        opacity_values = [float(o.strip()) for o in clean_opacity_str.split(',')]
                elif isinstance(opacity, list):
                    opacity_values = opacity
                else:
                    raise ValueError("Unsupported opacity format")

                for idx, chart_id in enumerate(chart_ids):
                    chart_opacities[chart_id] = opacity_values[idx] if idx < len(opacity_values) else 1.0
            except Exception as e:
                print("Error processing opacity values:", e)
                chart_opacities = {chart_id: 1.0 for chart_id in chart_ids}  # fallback to 1.0


            print("Processed chart_opacities:", chart_opacities)
          

            import json
            try:
                # Step 1: Normalize chart_type into a list
                if isinstance(chart_type, str):
                    chart_type_str = chart_type.strip()

                    if chart_type_str.startswith("[") and chart_type_str.endswith("]"):
                        try:
                            # Try parsing as JSON (e.g., '["bar", "line"]')
                            chart_type_values = json.loads(chart_type_str)
                        except json.JSONDecodeError:
                            # Fallback to literal_eval for Python-style strings (e.g., "['bar', 'line']")
                            chart_type_values = ast.literal_eval(chart_type_str)

                    elif chart_type_str.startswith("{") and chart_type_str.endswith("}"):
                        # Handle PostgreSQL array-style string (e.g., "{bar,line}")
                        clean_str = chart_type_str.strip('{}')
                        chart_type_values = [s.strip().strip('"') for s in clean_str.split(',') if s.strip()]
                    else:
                        # Single value string
                        chart_type_values = [chart_type_str]

                elif isinstance(chart_type, list):
                    chart_type_values = chart_type

                else:
                    raise ValueError("Unsupported chart_type format")

                # Step 2: Map values to chart_ids
                chart_type_value = {
                    chart_id: chart_type_values[idx] if idx < len(chart_type_values) else None
                    for idx, chart_id in enumerate(chart_ids)
                }

                print("chart_type_value:", chart_type_value)

            except Exception as e:
                print("Error processing chart_type values:", e)
                chart_type_value = {chart_id: None for chart_id in chart_ids}
                print("chart_type_value:", chart_type_value)
            chart_positions = {chart_id: positions[idx] if idx < len(positions) else None for idx, chart_id in enumerate(chart_ids)}
            chart_filters = {chart_id: filter_options[idx] if idx < len(filter_options) else None for idx, chart_id in enumerate(chart_ids)}
            # areacolour={chart_id:areacolour[idx] if idx < len(positions) else None for idx, chart_id in enumerate(chart_ids)}
            # print("Chart Filters:", chart_filters)
            chart_areacolour = {}
            
            for idx, chart_id in enumerate(chart_ids):
                
                if idx < len(areacolour):
                    chart_areacolour[chart_id] = areacolour[idx]
                else:
                    chart_areacolour[chart_id] = None  # Or some default color
          

            print("chart_positions", chart_positions)
            print("Chart Filters:", chart_filters)

            user_role = None
            employee_category_filters = []

            try:
                company_conn = get_company_db_connection(company_name)
                emp_cur = company_conn.cursor()

                # 🔹 Fetch user role
                emp_cur.execute("""
                    SELECT r.role_name
                    FROM employee_list e
                    JOIN role r ON e.role_id = r.role_id::text
                    WHERE e.employee_id = %s
                    LIMIT 1
                """, (employee_id,))

                role_row = emp_cur.fetchone()
                if role_row:
                    user_role = role_row[0].lower().strip()

                # 🔹 Fetch categories WITH operator
                emp_cur.execute("""
                    SELECT
                        c.category_name,
                        ucm.category_value,
                        COALESCE(ucm.operator, 'AND')
                    FROM user_category_mapping ucm
                    JOIN category c ON c.category_id = ucm.category_id
                    WHERE ucm.user_id = %s
                    ORDER BY ucm.id ASC
                """, (employee_id,))

                category_rows = emp_cur.fetchall()
                print("category_rows", category_rows)

                # ✅ CORRECT VARIABLE
                employee_category_filter = [
                    {
                        "key": key,
                        "value": value,
                        "operator": op
                    }
                    for key, value, op in category_rows
                    if key and value
                ]

                emp_cur.close()
                company_conn.close()

                print("user_role:", user_role)
                print("employee_category_filters:", employee_category_filter)

            except Exception as e:
                print("Failed to fetch role/category:", e)




            print("User role:", user_role)
            print("Employee category filter:", employee_category_filter)
            print("Processed chart_areacolour:", chart_areacolour)
            for chart_id, position in chart_positions.items():
                if not isinstance(position, dict) or 'x' not in position or 'y' not in position:
                    print(f"Invalid position for chart_id {chart_id}: {position}")
                    return []
            # --- DATAFRAME CACHE (Optimization for Real-Time) ---
            dataframe_cache = {} 
            # ----------------------------------------------------

            sorted_chart_ids = sorted(chart_ids, key=lambda x: (chart_positions.get(x, {'x': 0, 'y': 0})['x'], chart_positions.get(x, {'x': 0, 'y': 0})['y']))
            chart_data_list = []
            print("chart_data_list",chart_data_list)
            for chart_id in sorted_chart_ids:
                # Reset dashboard_Filter for this iteration to avoid contamination
                dashboard_Filter = original_dashboard_filter_arg
                
                cursor = conn.cursor()
                cursor.execute("SELECT id, database_name, selected_table, x_axis, y_axis, aggregate, chart_type, filter_options, chart_heading, chart_color, selectedUser,xfontsize,fontstyle,categorycolor,valuecolor,yfontsize,headingColor,ClickedTool,Bgcolour,OptimizationData,calculationdata,selectedFrequency,chart_name,user_id,xAxisTitle, yAxisTitle  FROM table_chart_save WHERE id = %s", (chart_id,))
            #                 cursor.execute("""
            #     SELECT id, database_name, selected_table, x_axis, y_axis, aggregate, chart_type,
            #            filter_options, chart_heading, chart_color, selectedUser, xfontsize,
            #            fontstyle, categorycolor, valuecolor, yfontsize, headingColor,
            #            ClickedTool, Bgcolour, OptimizationData
            #     FROM table_chart_save
            #     ORDER BY id DESC
            #     LIMIT 10
            # """)
                
                chart_data = cursor.fetchone()
                cursor.close()
                print("chart_data",chart_data)
                
                if chart_data:
                    # Extract chart data
                    database_name = chart_data[1]
                    table_name = chart_data[2]
                    

                    # --- TEMP FILTER OVERRIDE ---
                    if temp_filters:
                        # Construct a dynamic filter object for this table
                        dashboard_Filter = {
                            'table_name': table_name,
                            'filters': temp_filters
                        }
                        print(f"🔄 [TempFilter] Applying override for {table_name}: {dashboard_Filter}")
                    # ----------------------------

                    # --- ✅ REAL-TIME SETUP (DO NOT REMOVE) ---
                    # This starts the background thread that listens for DB changes.
                    try:
                        # Only ensure trigger/listener ONCE per DB/Table to avoid overhead in loop? 
                        # Actually 'ensure_trigger_exists' handles if checks efficiently, but we can optimize if needed.
                        pass # Kept logic below as is for now
                        # print(f"🚀 Initializing Real-Time Listener for DB: {database_name}...")
                        # ensure_trigger_exists(database_name, table_name)
                        # start_dynamic_listener(database_name)
                    except Exception as e:
                        print(f"⚠️ Real-time setup warning: {e}")
                    # ------------------------------------------
                    x_axis = chart_data[3]
                    y_axis = chart_data[4]  # Assuming y_axis is a list
                    aggregate = chart_data[5]
                    aggregation = chart_data[5]
                    chart_type = chart_data[6]
                    # chart_type = chart_type_value .get(chart_id)

                    chart_heading = chart_data[8]
                    chart_color = chart_data[9]  # Assuming chart_color is a list
                    selected_user = chart_data[10]  # Extract the selectedUser field
                    xfontsize = chart_data[11]
                    fontstyle = chart_data[12]
                    categorycolor = chart_data[13]
                    valuecolor = chart_data[14]
                    yfontsize = chart_data[15]
                    headingColor=chart_data[16]
                    ClickedTool=chart_data[17]
                    OptimizationData=chart_data[19]
                    calculationData=chart_data[20]
                    areacolour = chart_areacolour.get(chart_id)
                    filter_options = chart_filters.get(chart_id, {})
                    final_opacity = chart_opacities.get(chart_id, 1.0) # Default to 1.0 if not found in the passed opacities
                    selectedFrequency=chart_data[21]
                    chart_name=chart_data[22]
                    user_id=chart_data[23]
                    xAxisTitle=chart_data[24]
                    yAxisTitle =chart_data[25]
                    agg_value = chart_data[5]  # aggregate from DB
                    print("agg_value0", agg_value)



                    # --- ✅ REAL-TIME SETUP BLOCK ---
                    try:
                        # 1. Create trigger (Fixes "update not detected" issue)
                        ensure_trigger_exists(database_name, table_name)
                        
                        # 2. Start listener thread
                        start_dynamic_listener(database_name)
                        
                    except Exception as e:
                        print(f"⚠️ Real-time setup warning: {e}")
                    # --------------------------------


                    # Clean agg_value from quotes
                    # if isinstance(agg_value, str):
                    #     agg_value = agg_value.replace('"', '').replace("'", '').strip().lower()
                    #     print("agg_value1", agg_value)

                    if chart_type in ["singleValueChart", "meterGauge"]:
                        if isinstance(agg_value, str):
                            agg_value = agg_value.replace('"', '').replace("'", '').strip().lower()
                            print("agg_value1", agg_value)

                    # Ensure y_axis is list
                    current_y_axis = None
                    if isinstance(y_axis, str):
                        try:
                            y_axis = json.loads(y_axis)
                        except:
                            y_axis = []
                    if isinstance(y_axis, list) and y_axis:
                        current_y_axis = y_axis[0]

                    aggregate = None

                    # CASE 1: Simple direct aggregation string
                    if isinstance(agg_value, str) and agg_value.lower() in ["minimum","maximum","sum", "count", "avg", "mean", "min", "max","average","distinct count"]:
                        aggregate = agg_value.lower()
                        print("✔ Using direct string aggregate:", aggregate)

                    else:
                        # CASE 2: JSON list or incorrect stored string
                        if isinstance(agg_value, str):
                            try:
                                agg_value = json.loads(agg_value)
                            except:
                                agg_value = []

                        # CASE 3: Find match based on yAxis
                        if isinstance(agg_value, list):
                            aggregate = next(
                                (item.get('aggregation') for item in agg_value if item.get('yAxis') == current_y_axis),
                                None
                            )

                    # Fallback to first aggregation in list if still none
                    if not aggregate and isinstance(agg_value, list) and agg_value:
                        aggregate = agg_value[0].get('aggregation')

                    # Absolute final fallback → default to SUM
                    if not aggregate:
                        aggregate = "sum"

                    print("✔ Final Aggregate Used:", aggregate)
                    
                    print("Chart OptimizationData:", OptimizationData)
                    print("final_opacity",final_opacity)
                    
                                        # -----------------------------------------------
                    # 🟦 APPLY DASHBOARD FILTER IF TABLE NAME MATCHES
                    # -----------------------------------------------
                    if view_mode == "edit":
                        print("View mode is 'edit' → Skipping dashboard filters.")
                    else:
                        print("dashboard_Filter",dashboard_Filter)
                        # Normalize dashboard_Filter into dict
                        if dashboard_Filter is None:
                            dashboard_Filter = {} # Initialize to an empty dictionary
                        
                        if isinstance(dashboard_Filter, list):
                            # The user reported it coming as a list ['{"region": ...}', ...]
                            # If it's a list, it might be a list of filter strings?
                            # For now, if it's a list, we'll try to use the first item or default to empty dict
                            # to avoid the crash.
                            print("⚠️ dashboard_Filter is a LIST. Attempting to parse first item...")
                            try:
                                if len(dashboard_Filter) > 0:
                                    item = dashboard_Filter[0]
                                    if isinstance(item, str):
                                        dashboard_Filter = json.loads(item.replace("'", '"'))
                                    elif isinstance(item, dict):
                                        dashboard_Filter = item
                                    else:
                                        dashboard_Filter = {}
                                else:
                                    dashboard_Filter = {}
                            except Exception as e:
                                print(f"⚠️ Failed to parse dashboard_Filter list: {e}")
                                dashboard_Filter = {}

                        if isinstance(dashboard_Filter, str):
                            try:
                                dashboard_Filter = json.loads(dashboard_Filter.replace("'", '"'))
                            except Exception:
                                try:
                                    dashboard_Filter = ast.literal_eval(dashboard_Filter)
                                except:
                                    dashboard_Filter = {}

                        print("Normalized Dashboard Filter:", dashboard_Filter)
                        
                        # Ensure it's a dict before calling .get()
                        if not isinstance(dashboard_Filter, dict):
                            print(f"⚠️ dashboard_Filter is still not a dict ({type(dashboard_Filter)}). Resetting to empty.")
                            dashboard_Filter = {}

                        dashboard_table = dashboard_Filter.get("table_name")
                        dashboard_filters_list = dashboard_Filter.get("filters", [])

                        # Normalize dashboard filters into dict {column: values}
                        dashboard_filters = {}
                        for item in dashboard_filters_list:
                            if isinstance(item, dict):
                                dashboard_filters.update(item)

                        print("Dashboard Filters:", dashboard_filters)

                        # Only apply dashboard filters when table name matches
                        if dashboard_table and dashboard_table == table_name:

                            print(f"Applying dashboard filters to chart {chart_id} (table matched: {table_name})")

                            # Parse chart filter_options (string → dict)
                            chart_filters_clean = {}
                            if isinstance(filter_options, str):
                                try:
                                    chart_filters_clean = json.loads(filter_options)
                                except:
                                    chart_filters_clean = ast.literal_eval(filter_options)
                            elif isinstance(filter_options, dict):
                                chart_filters_clean = filter_options

                            # Merge dashboard filters into chart filters
                            # for col, val_list in dashboard_filters.items():
                            #     if col not in chart_filters_clean:
                            #         chart_filters_clean[col] = val_list   # Add new filter
                            #     else:
                            #         # Merge without duplicates
                            #         existing = set(chart_filters_clean[col])
                            #         new_vals = set(val_list)
                            #         chart_filters_clean[col] = list(existing | new_vals)
                            # Suggested Override Logic (Replacing the 'else' block)
                            for col, val_list in dashboard_filters.items():
                                # If the column is not in the chart filters, add it (same as before)
                                if col not in chart_filters_clean:
                                    chart_filters_clean[col] = val_list
                                else:
                                    # === CHANGE THIS SECTION ===
                                    # If the column IS in the chart filters, OVERRIDE it with the dashboard's filter values.
                                    chart_filters_clean[col] = val_list
                                    # The previous 'existing = set(chart_filters_clean[col]) | new_vals' logic is removed.
                                    # ===========================

                            # Replace old filter options
                            filter_options = chart_filters_clean

                            print("Merged filter_options (with override):", filter_options)

                        #]


                    # END Dashboard Filter Merge
                    # ------------------------------------------------
                                        
                    # Determine the aggregation function
                    aggregate_py = {
                        'count': 'count',
                        'sum': 'sum',
                        'average': 'mean',
                        'minimum': 'min',
                        'maximum': 'max',
                        'distinct count': 'nunique',
                    }.get(aggregate, 'sum')  # Default to 'sum' if no match

                    
                    
                    if not selected_user or selected_user.lower() == 'null':
                        print("🟢 Using default local database connection...")
                        connection = get_db_connection_view(database_name)
                        masterdatabasecon = create_connection()
                        print("✅ Local database connection established successfully!")

                    else:
                        print(f"🟡 Using external database connection for user: {selected_user}")
                        connection_details = fetch_external_db_connection(database_name, selected_user)

                        if not connection_details:
                            raise Exception(f"❌ Unable to fetch external database connection details for user '{selected_user}'")
                        use_ssh = bool(connection_details[8])
                        db_type = str(connection_details[2]).upper()
                        db_details = {
                            "name": connection_details[1],
                            "dbType": connection_details[2],
                            "host": connection_details[3],
                            "user": connection_details[4],
                            "password": connection_details[5],
                            "port": int(connection_details[6]),
                            "database": connection_details[7],
                            "use_ssh": use_ssh,
                            "ssh_host": connection_details[9] if use_ssh else None,
                            "ssh_port": int(connection_details[10] or 22) if use_ssh else None,
                            "ssh_username": connection_details[11] if use_ssh else None,
                            "ssh_key_path": connection_details[12] if use_ssh else None,
                            # "use_ssh": connection_details[8],
                            # "ssh_host": connection_details[9],
                            # "ssh_port": int(connection_details[10]),
                            # "ssh_username": connection_details[11],
                            # "ssh_key_path": connection_details[12],
                        }

                        print(f"🔹 External DB Connection Details: {db_details}")

                        # Initialize SSH variables
                        ssh_client = None
                        local_sock = None
                        stop_event = threading.Event()
                        tunnel_thread = None

                        # ✅ Establish SSH Tunnel if required
                        if db_details["use_ssh"]:
                            print("🔐 Establishing SSH tunnel manually (Paramiko)...")
                            private_key = paramiko.RSAKey.from_private_key_file(db_details["ssh_key_path"])

                            ssh_client = paramiko.SSHClient()
                            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                            ssh_client.connect(
                                db_details["ssh_host"],
                                username=db_details["ssh_username"],
                                pkey=private_key,
                                port=db_details["ssh_port"],
                                timeout=10
                            )

                            # Find free local port
                            local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            local_sock.bind(('127.0.0.1', 0))
                            local_port = local_sock.getsockname()[1]
                            local_sock.listen(1)
                            print(f"✅ Local forwarder listening on 127.0.0.1:{local_port}")

                            transport = ssh_client.get_transport()

                            def pipe(src, dst):
                                try:
                                    while True:
                                        data = src.recv(1024)
                                        if not data:
                                            break
                                        dst.sendall(data)
                                except Exception:
                                    pass
                                finally:
                                    src.close()
                                    dst.close()

                            def forward_tunnel():
                                while not stop_event.is_set():
                                    try:
                                        client_sock, _ = local_sock.accept()
                                        chan = transport.open_channel(
                                            "direct-tcpip",
                                            ("127.0.0.1", 5432),
                                            client_sock.getsockname()
                                        )
                                        if chan is None:
                                            client_sock.close()
                                            continue
                                        threading.Thread(target=pipe, args=(client_sock, chan), daemon=True).start()
                                        threading.Thread(target=pipe, args=(chan, client_sock), daemon=True).start()
                                    except Exception as e:
                                        print(f"❌ Channel open failed: {e}")

                            tunnel_thread = threading.Thread(target=forward_tunnel, daemon=True)
                            tunnel_thread.start()

                            host = "127.0.0.1"
                            port = local_port
                        else:
                            host = db_details["host"]
                            port = db_details["port"]

                        print(f"🧩 Connecting to external PostgreSQL at {host}:{port} ...")

                        # connection = psycopg2.connect(
                        #     dbname=db_details["database"],
                        #     user=db_details["user"],
                        #     password=db_details["password"],
                        #     host=host,
                        #     port=port
                        # )
                        if db_type == 'MSSQL':
                            print(f"🧩 Connecting to external MSSQL at {host}:{port} ...")

                            connection = pyodbc.connect(
                                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                                f"SERVER={host},{port};"
                                f"DATABASE={db_details['database']};"
                                f"UID={db_details['user']};"
                                f"PWD={db_details['password']};"
                                "TrustServerCertificate=yes;"
                            )

                            print("✅ External MSSQL connection established successfully!")
                            

                        else:
                            print(f"🧩 Connecting to external PostgreSQL at {host}:{port} ...")

                            connection = psycopg2.connect(
                                dbname=db_details["database"],
                                user=db_details["user"],
                                password=db_details["password"],
                                host=host,
                                port=port,
                            )

                        print("✅ External PostgreSQL connection established successfully!")

                        print('External Connection established:', connection)
                    
                                        # -----------------------------------------------
                    # 🟦 APPLY DASHBOARD FILTER IF TABLE NAME MATCHES
                    # -----------------------------------------------
                    if view_mode == "edit":
                        print("View mode is 'edit' → Skipping dashboard filters.")
                    else:
                        print("dashboard_Filter",dashboard_Filter)
                        # Normalize dashboard_Filter into dict
                        if dashboard_Filter is None:
                            dashboard_Filter = {} # Initialize to an empty dictionary
                        
                        if isinstance(dashboard_Filter, list):
                            # The user reported it coming as a list ['{"region": ...}', ...]
                            # If it's a list, it might be a list of filter strings?
                            # For now, if it's a list, we'll try to use the first item or default to empty dict
                            # to avoid the crash.
                            print("⚠️ dashboard_Filter is a LIST. Attempting to parse first item...")
                            try:
                                if len(dashboard_Filter) > 0:
                                    item = dashboard_Filter[0]
                                    if isinstance(item, str):
                                        dashboard_Filter = json.loads(item.replace("'", '"'))
                                    elif isinstance(item, dict):
                                        dashboard_Filter = item
                                    else:
                                        dashboard_Filter = {}
                                else:
                                    dashboard_Filter = {}
                            except Exception as e:
                                print(f"⚠️ Failed to parse dashboard_Filter list: {e}")
                                dashboard_Filter = {}

                        if isinstance(dashboard_Filter, str):
                            try:
                                dashboard_Filter = json.loads(dashboard_Filter.replace("'", '"'))
                            except Exception:
                                try:
                                    dashboard_Filter = ast.literal_eval(dashboard_Filter)
                                except:
                                    dashboard_Filter = {}

                        print("Normalized Dashboard Filter:", dashboard_Filter)
                        
                        # Ensure it's a dict before calling .get()
                        if not isinstance(dashboard_Filter, dict):
                            print(f"⚠️ dashboard_Filter is still not a dict ({type(dashboard_Filter)}). Resetting to empty.")
                            dashboard_Filter = {}

                        dashboard_table = dashboard_Filter.get("table_name")
                        dashboard_filters_list = dashboard_Filter.get("filters", [])

                        # Normalize dashboard filters into dict {column: values}
                        dashboard_filters = {}
                        for item in dashboard_filters_list:
                            if isinstance(item, dict):
                                dashboard_filters.update(item)

                        print("Dashboard Filters:", dashboard_filters)
                        # -------------------------------
                        # ✅ ALWAYS INITIALIZE chart_filters_clean
                        # -------------------------------
                        chart_filters_clean = {}

                        if isinstance(filter_options, str):
                            try:
                                chart_filters_clean = json.loads(filter_options)
                            except:
                                chart_filters_clean = ast.literal_eval(filter_options)
                        elif isinstance(filter_options, dict):
                            chart_filters_clean = filter_options.copy()


                        # Only apply dashboard filters when table name matches
                        if dashboard_table and dashboard_table == table_name:

                            print(f"Applying dashboard filters to chart {chart_id} (table matched: {table_name})")

                            # Parse chart filter_options (string → dict)
                            # chart_filters_clean = {}
                            # if isinstance(filter_options, str):
                            #     try:
                            #         chart_filters_clean = json.loads(filter_options)
                            #     except:
                            #         chart_filters_clean = ast.literal_eval(filter_options)
                            # elif isinstance(filter_options, dict):
                            #     chart_filters_clean = filter_options

                            # Merge dashboard filters into chart filters
                            # for col, val_list in dashboard_filters.items():
                            #     if col not in chart_filters_clean:
                            #         chart_filters_clean[col] = val_list   # Add new filter
                            #     else:
                            #         # Merge without duplicates
                            #         existing = set(chart_filters_clean[col])
                            #         new_vals = set(val_list)
                            #         chart_filters_clean[col] = list(existing | new_vals)
                            # Suggested Override Logic (Replacing the 'else' block)
                            for col, val_list in dashboard_filters.items():
                                # If the column is not in the chart filters, add it (same as before)
                                if col not in chart_filters_clean:
                                    chart_filters_clean[col] = val_list
                                else:
                                    # === CHANGE THIS SECTION ===
                                    # If the column IS in the chart filters, OVERRIDE it with the dashboard's filter values.
                                    chart_filters_clean[col] = val_list
                                    # The previous 'existing = set(chart_filters_clean[col]) | new_vals' logic is removed.
                                    # ===========================
                        if temp_filters:
                            print("Applying TEMP filters:", temp_filters)
                            for col, val in temp_filters.items():
                                chart_filters_clean[col] = val  # OVERRIDE
                        print("logged_user_role",user_role)
                        if is_restricted_role(user_role):
                            print("Restricted role detected → applying employee category restriction")
                            df = fetch_chart_data(connection, table_name)
                            print("chart_filters_clean,employee_category_filter,df",df,chart_filters_clean,employee_category_filter)

                            chart_filters_clean = expand_filters_with_actual_values(df,
                                        chart_filters_clean,
                                        employee_category_filter
                                        
                            )
                            print("chart_filters_clean",chart_filters_clean)
                        else:
                            print("Non-restricted role → skipping employee category filter")
                            # Replace old filter options
                        filter_options = chart_filters_clean

                        print("Merged filter_options (with override):", filter_options)

                        #]


                    # END Dashboard Filter Merge
                    # ------------------------------------------------
                                        
                    # Determine the aggregation function
                    aggregate_py = {
                        'count': 'count',
                        'sum': 'sum',
                        'average': 'mean',
                        'minimum': 'min',
                        'maximum': 'max'
                    }.get(aggregate, 'sum')  # Default to 'sum' if no match

                    
                    
                    
                    

                    # # -----------------------------------------------
                    # # 🟦 APPLY DASHBOARD FILTER IF TABLE NAME MATCHES
                    # # -----------------------------------------------
                    # if view_mode == "edit":
                    #     print("View mode is 'edit' → Skipping dashboard filters.")
                    # else:
                    #     print("dashboard_Filter",dashboard_Filter)
                    #     # Normalize dashboard_Filter into dict
                    #     if dashboard_Filter is None:
                    #         dashboard_Filter = {} # Initialize to an empty dictionary
                    #     if isinstance(dashboard_Filter, str):
                    #         try:
                    #             dashboard_Filter = json.loads(dashboard_Filter.replace("'", '"'))
                    #         except Exception:
                    #             dashboard_Filter = ast.literal_eval(dashboard_Filter)

                    #     print("Normalized Dashboard Filter:", dashboard_Filter)

                    #     dashboard_table = dashboard_Filter.get("table_name")
                    #     dashboard_filters_list = dashboard_Filter.get("filters", [])

                    #     # Normalize dashboard filters into dict {column: values}
                    #     dashboard_filters = {}
                    #     for item in dashboard_filters_list:
                    #         if isinstance(item, dict):
                    #             dashboard_filters.update(item)

                    #     print("Dashboard Filters:", dashboard_filters)
                    #     chart_filters_clean = {}
                    #     if isinstance(filter_options, str):
                    #         try:
                    #             chart_filters_clean = json.loads(filter_options)
                    #         except:
                    #             chart_filters_clean = ast.literal_eval(filter_options)
                    #     elif isinstance(filter_options, dict):
                    #         chart_filters_clean = filter_options

                    #     # Only apply dashboard filters when table name matches
                    #     if dashboard_table and dashboard_table == table_name:

                    #         print(f"Applying dashboard filters to chart {chart_id} (table matched: {table_name})")

                    #         # Parse chart filter_options (string → dict)
                    #         # chart_filters_clean = {}
                    #         # if isinstance(filter_options, str):
                    #         #     try:
                    #         #         chart_filters_clean = json.loads(filter_options)
                    #         #     except:
                    #         #         chart_filters_clean = ast.literal_eval(filter_options)
                    #         # elif isinstance(filter_options, dict):
                    #         #     chart_filters_clean = filter_options

                    #         # Merge dashboard filters into chart filters
                    #         # for col, val_list in dashboard_filters.items():
                    #         #     if col not in chart_filters_clean:
                    #         #         chart_filters_clean[col] = val_list   # Add new filter
                    #         #     else:
                    #         #         # Merge without duplicates
                    #         #         existing = set(chart_filters_clean[col])
                    #         #         new_vals = set(val_list)
                    #         #         chart_filters_clean[col] = list(existing | new_vals)
                    #         # Suggested Override Logic (Replacing the 'else' block)
                    #         for col, val_list in dashboard_filters.items():
                    #             # If the column is not in the chart filters, add it (same as before)
                    #             if col not in chart_filters_clean:
                    #                 chart_filters_clean[col] = val_list
                    #             else:
                    #                 # === CHANGE THIS SECTION ===
                    #                 # If the column IS in the chart filters, OVERRIDE it with the dashboard's filter values.
                    #                 chart_filters_clean[col] = val_list
                    #                 # The previous 'existing = set(chart_filters_clean[col]) | new_vals' logic is removed.
                    #                 # ===========================
                    #                 # ------------------------------------
                    #         # 🔐 APPLY EMPLOYEE CATEGORY FILTER
                    #         # ONLY FOR viewer / report viewer
                    #         # ------------------------------------
                    #     if is_restricted_role(user_role):
                    #         print("Restricted role detected → applying employee category restriction")
                    #         df = fetch_chart_data(connection, table_name)

                    #         chart_filters_clean = expand_filters_with_actual_values(df,
                    #                     chart_filters_clean,
                    #                     employee_category_filter
                                        
                    #         )
                    #         print("chart_filters_clean",chart_filters_clean)
                    #     else:
                    #         print("Non-restricted role → skipping employee category filter")

                    #         # Replace old filter options
                    #     filter_options = chart_filters_clean

                    #     print("Merged filter_options (with override):", filter_options)

                    #     #]


                    # # END Dashboard Filter Merge
                    # # ------------------------------------------------
                                        
                    # # Determine the aggregation function
                    # aggregate_py = {
                    #     'count': 'count',
                    #     'sum': 'sum',
                    #     'average': 'mean',
                    #     'minimum': 'min',
                    #     'maximum': 'max'
                    # }.get(aggregate, 'sum')  # Default to 'sum' if no match




                    # Check if selectedUser is NULL
                    # if selected_user is None:
                    #     # Use the default local connection if selectedUser is NULL
                    #     connection = get_db_connection_view(database_name)
                    #     masterdatabasecon=create_connection()
                    #     print('Using local database connection')

                    # else:
                    #     # Use external connection if selectedUser is provided
                    #     connection = fetch_external_db_connection(database_name, selected_user)
                    #     host = connection[3]
                    #     dbname = connection[7]
                    #     user = connection[4]
                    #     password = connection[5]

                    #     # Create a new psycopg2 connection using the details from the tuple
                    #     connection = psycopg2.connect(
                    #         dbname=dbname,
                    #         user=user,
                    #         password=password,
                    #         host=host
                    #     )
                    
                    if chart_type == "wordCloud":
                        if len(y_axis) == 0:
                            x_axis_columns_str = ', '.join(x_axis)
                            print("x_axis_columns_str:", x_axis_columns_str)
                            query = f"""
                                SELECT word, COUNT(*) AS word_count
                                FROM (
                                    SELECT regexp_split_to_table({x_axis_columns_str}, '\\s+') AS word
                                    FROM {table_name}
                                ) AS words
                                GROUP BY word
                                ORDER BY word_count DESC;
                            """
                            print("WordCloud SQL Query:", query)

                            try:
                                cursor = connection.cursor()
                                cursor.execute(query)
                                data = cursor.fetchall()
                                cursor.close()
                                print("wordcloulddata",data)
                                if data:
                                    categories = [row[0] for row in data]  # Words
                                    values = [row[1] for row in data]     # Counts

                                    chart_data_list.append({
                                        "chart_id": chart_id,
                                        "categories": categories,
                                        "values": values,
                                        "chart_type": chart_type,
                                        "chart_heading": chart_heading,
                                        "positions": chart_positions.get(chart_id),
                                        "xfontsize": xfontsize,
                                        "fontstyle" :fontstyle,
                                        "categorycolor" :categorycolor,
                                        "valuecolor" :valuecolor,
                                        "yfontsize" :yfontsize,
                                        "headingColor":headingColor,
                                        "filter_options":filter_options,
                                        "ClickedTool":ClickedTool,
                                        "Bgcolour":areacolour,
                                        "table_name":table_name,
                                        "database_name": database_name,
                                        "opacity":final_opacity,
                                        "chart_name":chart_name,
                                        "user_id": user_id,
                                        
                                        

                                    
                                    })
                                    continue
                                else:
                                    print("No data returned for WordCloud query")
                            except Exception as e:
                                print("Error executing WordCloud query:", e)
                                chart_data_list.append({
                                    "error": f"WordCloud query failed: {str(e)}"
                                })
                    elif chart_type == "timeSeriesDecomposition":
                        try:
                            # selectedFrequency = data.get("selectedFrequency", "monthly").lower()
                            print("Selected Frequency:", selectedFrequency)

                            time_column = x_axis[0]
                            value_column = y_axis[0]

                            # Fetch data
                            df = fetch_data_for_ts_decomposition(
                                table_name, [time_column], filter_options, [value_column],
                                None, database_name, selected_user, calculationData
                            )

                            if df.empty:
                                return jsonify({"error": "No data available for time series decomposition after filtering."}), 400

                            # Clean and prepare data
                            df.dropna(subset=[time_column], inplace=True)
                            df[time_column] = pd.to_datetime(df[time_column], errors='coerce')
                            df.set_index(time_column, inplace=True)
                            df[value_column] = pd.to_numeric(df[value_column], errors='coerce')
                            df.dropna(subset=[value_column], inplace=True)

                            # Frequency mapping
                            freq_map = {
                                "daily": "D",
                                "monthly": "MS",
                                "yearly": "YS"
                            }
                            time_series_frequency = freq_map.get(selectedFrequency, "MS")
                            print("Resampling frequency:", time_series_frequency)

                            # Aggregation
                            agg_func_map = {
                                "sum": "sum",
                                "average": "mean",
                                "mean": "mean",
                                "count": "count",
                                "distinct count": "nunique",
                                "minimum": "min",
                                "min": "min",
                                "maximum": "max",
                                "max": "max",
                                "median": "median",
                                "variance": "var"
                            }
                            agg_func = agg_func_map.get(aggregate.lower())
                            if not agg_func:
                                return jsonify({"error": f"Unsupported aggregation type: {aggregate}"}), 400

                            ts_data = getattr(df[value_column].resample(time_series_frequency), agg_func)()
                            ts_data = ts_data.ffill().bfill()

                            # Determine seasonal period
                            if time_series_frequency == 'D':
                                period = 7
                            elif time_series_frequency == 'W':
                                period = 52
                            elif time_series_frequency == 'Q':
                                period = 4
                            else:  # MS or YS
                                period = min(4, len(ts_data) // 2)

                            if len(ts_data) < 2 * period:
                                return jsonify({
                                    "error": f"Not enough data for decomposition. At least {2 * period} points needed."
                                }), 400

                           
                            decomposition = seasonal_decompose(ts_data, model='additive', period=period)

                            trend = decomposition.trend.dropna().tolist()
                            seasonal = decomposition.seasonal.dropna().tolist()
                            residual = decomposition.resid.dropna().tolist()
                            observed = decomposition.observed.dropna().tolist()
                            dates = decomposition.observed.dropna().index.strftime('%Y-%m-%d').tolist()
                            

                            # Append results to chart data list
                            chart_data_list.append({
                                "categories": dates,
                                "values": observed,
                                "chart_id": chart_id,
                                "chart_type": chart_type,
                                "chart_color": chart_color,
                                "x_axis": x_axis,
                                "y_axis": y_axis,
                                "aggregate": aggregation,
                                "positions": chart_positions.get(chart_id),
                                "xfontsize": xfontsize,
                                "fontstyle": fontstyle,
                                "categorycolor": categorycolor,
                                "valuecolor": valuecolor,
                                "yfontsize": yfontsize,
                                "chart_heading": chart_heading,
                                "headingColor": headingColor,
                                "table_name": table_name,
                                "database_name": database_name,
                                "filter_options": filter_options,
                                "ClickedTool": ClickedTool,
                                "Bgcolour": areacolour,
                                "OptimizationData": OptimizationData if 'OptimizationData' in locals() or 'OptimizationData' in globals() else None,
                                "opacity": final_opacity,
                                "calculationData": calculationData,
                                "selectedFrequency":selectedFrequency,
                                "chart_name": (user_id, chart_name),
                                "user_id": user_id 
                            })

                        except Exception as e:
                            return jsonify({"error": f"An unexpected error occurred during time series decomposition: {str(e)}"}), 500
                        continue
                    # Handle singleValueChart type separately
                    elif chart_type == "singleValueChart":
                        print("sv")
                        print("aggregate5",aggregate)
                        aggregate_py = {
                            'count': 'count',
                            'sum': 'sum',
                            'average': 'avg',
                            'minimum': 'min',
                            'maximum': 'max',
                            'distinct count': 'distinct count'
                        }.get(aggregate, 'sum') 
                        
                        single_value_result = fetchText_data(database_name, table_name, x_axis[0], aggregate_py,selected_user,filter_options)
                        print("Single Value Result for Chart ID", chart_id, ":", single_value_result)
                        # Append single value chart data
                        chart_data_list.append({
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_heading": chart_heading,
                            "values": single_value_result,
                            "positions": chart_positions.get(chart_id),
                            "xfontsize": xfontsize,
                            "fontstyle" :fontstyle,
                            "categorycolor" :categorycolor,
                            "valuecolor" :valuecolor,
                            "yfontsize" :yfontsize,
                            "headingColor":headingColor, 
                            "filter_options":filter_options ,
                            "x_axis": x_axis,
                            "y_axis": y_axis,   
                            "ClickedTool":ClickedTool, 
                            "Bgcolour":areacolour,
                            "table_name":table_name,
                            "database_name": database_name,
                            "opacity":final_opacity,
                            "chart_name": (user_id, chart_name),
                            "user_id": user_id  
                        })
                        continue  # Skip further processing for this chart ID
                    elif chart_type == "meterGauge":
                        print("meterGauge")
                        print("aggregate5",aggregate)
                        aggregate_py = {
                            'count': 'count',
                            'sum': 'sum',
                            'average': 'avg',
                            'minimum': 'min',
                            'maximum': 'max',
                            'distinct count': 'distinct count'
                        }.get(aggregate, 'sum') 
                        single_value_result = fetchText_data(database_name, table_name, x_axis[0], aggregate_py,selected_user,filter_options)
                        print("Single Value Result for Chart ID", chart_id, ":", single_value_result)
                        # Append single value chart data
                        chart_data_list.append({
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_heading": chart_heading,
                            "values": single_value_result,
                            "positions": chart_positions.get(chart_id),
                            "xfontsize": xfontsize,
                            "fontstyle" :fontstyle,
                            "categorycolor" :categorycolor,
                            "valuecolor" :valuecolor,
                            "yfontsize" :yfontsize,
                            "headingColor":headingColor, 
                            "filter_options":filter_options ,
                            "x_axis": x_axis,
                            "y_axis": y_axis,   
                            "ClickedTool":ClickedTool, 
                            "Bgcolour":areacolour,
                            "chart_color": chart_color,
                            "table_name":table_name,
                            "database_name": database_name,
                            "opacity":final_opacity,
                            "chart_name": (user_id, chart_name),
                            "user_id": user_id  
                        })
                        continue  # Skip further processing for this chart ID
                    # Proceed with category and value generation for non-singleValueChart types
                    
                    # ----------------------------------------------------
                    # OPTIMIZED DATA FETCHING (CACHE LOOKUP)
                    # ----------------------------------------------------
                    # Check if we already fetched this table in this batch
                    if table_name in dataframe_cache:
                        print(f"⚡ CACHE HIT: Reusing dataframe for {table_name}")
                        dataframe = dataframe_cache[table_name].copy(deep=True) # Deep copy to prevent cross-contamination
                    else:
                        print(f"🐢 CACHE MISS: Fetching DB for {table_name}")
                        dataframe = fetch_chart_data(connection, table_name)
                        # Store a clean copy in cache
                        dataframe_cache[table_name] = dataframe.copy(deep=True)
                    # ----------------------------------------------------
                    
                    print("Chart ID", chart_id)
                    skip_calculated_column = False
                    y_base_column = None
                 
                    if calculationData and isinstance(calculationData, list):
                        for calc_entry in calculationData:
                            calc_formula = calc_entry.get('calculation', '').strip()
                            new_col_name = calc_entry.get('columnName', '').strip()
                            replace_col_name = calc_entry.get('replaceColumn', new_col_name)

                            if not calc_formula or not new_col_name:
                                continue  # Skip incomplete entries

                            # Apply only if the column is involved in x or y axis
                            if new_col_name not in (x_axis or []) and new_col_name not in (y_axis or []):
                                continue

                            print("calc_formula",calc_formula)
                            # if new_col_name in y_axis:
                            
                            if new_col_name in y_axis:
                                print("new_col_namey", new_col_name)
                            if new_col_name in x_axis:
                                print("new_col_nameX", new_col_name)
                            def replace_column(match):
                                col_name = match.group(1)
                                if col_name in dataframe.columns:
                                    return f"dataframe['{col_name}']"
                                else:
                                    raise ValueError(f"Column {col_name} not found in DataFrame.")

                            if calc_formula.startswith("if"):
                                match = (
                                    re.match(
                                        r"if\s*\(\s*(.+?)\s*\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$",
                                        calc_formula.strip(),
                                        re.IGNORECASE
                                    )
                                    or
                                    re.match(
                                        r"if\s*\(\s*(.+?)\s*,\s*'?(.*?)'?\s*,\s*'?(.*?)'?\s*\)$",
                                        calc_formula.strip(),
                                        re.IGNORECASE
                                    )
                                )

                                # match = re.match(r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$", calc_formula.strip(), re.IGNORECASE)
                                if not match:
                                    raise ValueError("Invalid IF format")
                                condition_expr, then_val, else_val = match.groups()
                                condition_expr_python = re.sub(r'\[(.*?)\]', replace_column, condition_expr)
                                dataframe[new_col_name] = np.where(eval(condition_expr_python), then_val.strip("'\""), else_val.strip("'\""))
                            elif calc_formula.lower().startswith("round"):
                                # Match formula: round(<expression>, <decimals>)
                                match = re.match(r'round\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)', calc_formula, re.IGNORECASE)
                                if not match:
                                    raise ValueError(
                                        "Invalid ROUND format. Use round([col], decimals) or round([col1]/[col2], decimals)"
                                    )

                                expr, decimals = match.groups()
                                decimals = int(decimals)

                                # Replace [column] with numeric dataframe references
                                def replace_column(match):
                                    col_name = match.group(1)
                                    if col_name not in dataframe.columns:
                                        raise ValueError(f"Missing column: {col_name}")
                                    # Convert to numeric
                                    dataframe[col_name] = pd.to_numeric(dataframe[col_name], errors='coerce')
                                    return f"dataframe['{col_name}']"

                                # Replace [col] references in the expression
                                expr_python = re.sub(r'\[([^\]]+)\]', replace_column, expr)

                                # Handle division by zero safely
                                expr_python = re.sub(
                                    r"dataframe\['([^']+)'\]\s*/\s*dataframe\['([^']+)'\]",
                                    r"np.divide(dataframe['\1'], dataframe['\2'].replace(0, np.nan))",
                                    expr_python
                                )

                                # Evaluate expression safely and round
                                try:
                                    dataframe[new_col_name] = np.round(eval(expr_python), decimals)
                                except Exception as e:
                                    print(f"Error evaluating ROUND formula: {e}")
                                    dataframe[new_col_name] = np.nan


                            elif calc_formula.startswith("switch"):
                                switch_match = re.match(r"switch\s*\(\s*\[([^\]]+)\](.*?)\)", calc_formula, re.IGNORECASE)
                                if not switch_match:
                                    raise ValueError("Invalid SWITCH format")
                                col_name, rest = switch_match.groups()
                                if col_name not in dataframe.columns:
                                    raise ValueError(f"Column '{col_name}' not found")
                                cases = re.findall(r'"(.*?)"\s*,\s*"(.*?)"', rest)
                                default_match = re.search(r'default\s*,\s*["\']?(.*?)["\']?$', rest, re.IGNORECASE)
                                default_val = default_match.group(1) if default_match else None
                                dataframe[new_col_name] = dataframe[col_name].map(dict(cases)).fillna(default_val)

                            elif calc_formula.startswith("iferror"):
                                match = re.match(r"iferror\s*\((.+?)\s*,\s*(.+?)\)", calc_formula, re.IGNORECASE)
                                if not match:
                                    raise ValueError("Invalid IFERROR format")
                                expr, fallback = match.groups()
                                expr_python = re.sub(r'\[(.*?)\]', replace_column, expr)
                                try:
                                    dataframe[new_col_name] = eval(expr_python)
                                    dataframe[new_col_name] = dataframe[new_col_name].fillna(fallback)
                                except:
                                    dataframe[new_col_name] = fallback

                            elif calc_formula.startswith("calculate"):
                                match = re.match(r"calculate\s*\(\s*(sum|avg|count|max|min)\s*\(\s*\[([^\]]+)\]\)\s*,\s*\[([^\]]+)\]\s*=\s*['\"](.*?)['\"]\s*\)", calc_formula, re.IGNORECASE)
                                if not match:
                                    raise ValueError("Invalid CALCULATE format")
                                agg, value_col, filter_col, filter_val = match.groups()
                                df_filtered = dataframe[dataframe[filter_col] == filter_val]
                                result_val = {
                                    "sum": df_filtered[value_col].astype(float).sum(),
                                    "avg": df_filtered[value_col].astype(float).mean(),
                                    "count": df_filtered[value_col].count(),
                                    "distinct count": df_filtered[value_col].nunique(),
                                    "max": df_filtered[value_col].astype(float).max(),
                                    "min": df_filtered[value_col].astype(float).min(),
                                }[agg]
                                dataframe[new_col_name] = result_val

                            elif calc_formula.startswith(("maxx", "minx")):
                                match = re.match(r"(maxx|minx)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE)
                                func, col = match.groups()
                                dataframe[new_col_name] = dataframe[col].max() if func.lower() == "maxx" else dataframe[col].min()

                            elif calc_formula.startswith("abs"):
                                col = re.match(r"abs\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                                dataframe[new_col_name] = dataframe[col].abs()

                            elif calc_formula.startswith("len"):
                                col = re.match(r"len\s*\(\s*(?:\[([^\]]+)\]|\"([^\"]+)\")\s*\)", calc_formula, re.IGNORECASE).groups()
                                dataframe[new_col_name] = dataframe[col[0] or col[1]].astype(str).str.len()

                            elif calc_formula.startswith("lower"):
                                col = re.match(r"lower\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                                dataframe[new_col_name] = dataframe[col].astype(str).str.lower()

                            elif calc_formula.startswith("upper"):
                                col = re.match(r"upper\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                                dataframe[new_col_name] = dataframe[col].astype(str).str.upper()

                            elif calc_formula.startswith("concat"):
                                parts = re.split(r",(?![^\[]*\])", re.match(r"concat\s*\((.+)\)", calc_formula, re.IGNORECASE).group(1))
                                concat_parts = []
                                for part in parts:
                                    part = part.strip()
                                    if part.startswith("[") and part.endswith("]"):
                                        col = part[1:-1]
                                        concat_parts.append(dataframe[col].astype(str))
                                    else:
                                        concat_parts.append(part.strip('"').strip("'"))
                                from functools import reduce
                                dataframe[new_col_name] = reduce(lambda x, y: x + y, [p if isinstance(p, pd.Series) else pd.Series([p]*len(dataframe)) for p in concat_parts])

                            elif re.match(r"(year|month|day)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE):
                                func, col = re.match(r"(year|month|day)\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).groups()
                                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")
                                dataframe[new_col_name] = getattr(dataframe[col].dt, func.lower())

                            elif calc_formula.startswith("isnull"):
                                col, fallback = re.match(r"isnull\s*\(\s*\[([^\]]+)\]\s*,\s*['\"]?(.*?)['\"]?\s*\)", calc_formula, re.IGNORECASE).groups()
                                dataframe[new_col_name] = dataframe[col].fillna(fallback)

                            elif re.match(r"(?:\[([^\]]+)\]|\"([^\"]+)\")\s+in\s*\((.*?)\)", calc_formula, re.IGNORECASE):
                                match = re.match(r"(?:\[([^\]]+)\]|\"([^\"]+)\")\s+in\s*\((.*?)\)", calc_formula, re.IGNORECASE)
                                col = match.group(1) or match.group(2)
                                values = [v.strip().strip('"').strip("'") for v in match.group(3).split(",")]
                                dataframe[new_col_name] = dataframe[col].isin(values)

                            elif calc_formula.startswith("datediff"):
                                end_col, start_col = re.match(r"datediff\s*\(\s*\[([^\]]+)\]\s*,\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).groups()
                                dataframe[end_col] = pd.to_datetime(dataframe[end_col], errors="coerce")
                                dataframe[start_col] = pd.to_datetime(dataframe[start_col], errors="coerce")
                                dataframe[new_col_name] = (dataframe[end_col] - dataframe[start_col]).dt.days

                            elif calc_formula.startswith("today()"):
                                dataframe[new_col_name] = pd.Timestamp.today().normalize()

                            elif calc_formula.startswith("now()"):
                                dataframe[new_col_name] = pd.Timestamp.now()

                            elif calc_formula.startswith("dateadd"):
                                col, interval, unit = re.match(r"dateadd\s*\(\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*['\"](day|month|year)['\"]\)", calc_formula, re.IGNORECASE).groups()
                                interval = int(interval)
                                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")
                                if unit == "day":
                                    dataframe[new_col_name] = dataframe[col] + pd.to_timedelta(interval, unit="d")
                                elif unit == "month":
                                    dataframe[new_col_name] = dataframe[col] + pd.DateOffset(months=interval)
                                elif unit == "year":
                                    dataframe[new_col_name] = dataframe[col] + pd.DateOffset(years=interval)

                            elif calc_formula.startswith("formatdate"):
                                col, fmt = re.match(r"formatdate\s*\(\s*\[([^\]]+)\]\s*,\s*['\"](.+?)['\"]\)", calc_formula, re.IGNORECASE).groups()
                                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")
                                fmt_mapped = fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
                                dataframe[new_col_name] = dataframe[col].dt.strftime(fmt_mapped)

                            elif calc_formula.startswith("replace"):
                                col, old, new = re.match(r"replace\s*\(\s*\[([^\]]+)\]\s*,\s*['\"](.*?)['\"]\s*,\s*['\"](.*?)['\"]\)", calc_formula, re.IGNORECASE).groups()
                                dataframe[new_col_name] = dataframe[col].astype(str).str.replace(old, new, regex=False)

                            elif calc_formula.startswith("trim"):
                                col = re.match(r"trim\s*\(\s*\[([^\]]+)\]\)", calc_formula, re.IGNORECASE).group(1)
                                dataframe[new_col_name] = dataframe[col].astype(str).str.strip()

                            # else:
                            #     calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
                            #     dataframe[new_col_name] = eval(calc_formula_python)
                            elif calc_formula.lower().startswith(("sum", "avg", "min", "max")):
                                match = re.match(
                                    r'(sum|avg|min|max)\s*\(\s*\[([^\]]+)\]\s*\)',
                                    calc_formula,
                                    re.IGNORECASE
                                )
                                if not match:
                                    raise ValueError("Invalid aggregation format.")

                                agg_func, col = match.groups()
                                agg_func = agg_func.lower()

                                # DO NOT create calculated column
                                # Just mark aggregation intent
                                aggregation = agg_func
                                y_base_column = col

                                print(f"Detected aggregation: {aggregation}({col})")

                                # IMPORTANT: skip dataframe eval
                                skip_calculated_column = True

                            else:
                                # calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
                                # print("Evaluating math formula:", calc_formula_python)
                                # Replace column references [col] → temp_df['col']
                                calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)

                                # Handle COUNT
                                calc_formula_python = re.sub(
                                    r'count\s*\(\s*(temp_df\[.*?\])\s*\)',
                                    r'\1.count()',
                                    calc_formula_python,
                                    flags=re.IGNORECASE
                                )

                                # Handle DISTINCT COUNT
                                calc_formula_python = re.sub(
                                    r'distinct count\s*\(\s*(temp_df\[.*?\])\s*\)',
                                    r'\1.nunique()',
                                    calc_formula_python,
                                    flags=re.IGNORECASE
                                )

                                # Handle SUM
                                calc_formula_python = re.sub(
                                    r'sum\s*\(\s*(temp_df\[.*?\])\s*\)',
                                    r'\1.sum()',
                                    calc_formula_python,
                                    flags=re.IGNORECASE
                                )

                                # Handle AVG
                                calc_formula_python = re.sub(
                                    r'avg\s*\(\s*(temp_df\[.*?\])\s*\)',
                                    r'\1.mean()',
                                    calc_formula_python,
                                    flags=re.IGNORECASE
                                )

                                print("Evaluating math formula:", calc_formula_python)
                                temp_df[new_col_name] = eval(calc_formula_python)

                            print(f"✅ New column '{new_col_name}' created.")


                            # Replace in axes
                            if y_axis:
                                y_axis = [new_col_name if col == replace_col_name else col for col in y_axis]
                            if x_axis:
                                x_axis = [new_col_name if col == replace_col_name else col for col in x_axis]
                            if skip_calculated_column:
                                y_axis = [y_base_column if col == new_col_name else col for col in y_axis]

                    for axis in y_axis:
                        try:
                
                            dataframe[axis] = pd.to_datetime(dataframe[axis], errors='raise', format='%H:%M:%S')
                            dataframe[axis] = dataframe[axis].dt.hour * 60 + dataframe[axis].dt.minute + dataframe[axis].dt.second / 60
                            print(f"Converted Time to Minutes for {axis}: ", dataframe[axis].head())
                        except ValueError:
                            dataframe[axis] = pd.to_numeric(dataframe[axis], errors='coerce')
                    # Check if the aggregation type is count
                    # if aggregate_py == 'count' and chart_type not in ["duealbarChart", "duealChart","treeHierarchy","Butterfly","tablechart"]:
                    #     print("COUNT AGGREGATION")
                    #     print("Aggregate is count", aggregate_py)
                    #     print("X-Axis:", x_axis)
                    #     df=fetch_chart_data(connection, table_name)
                    #     df = apply_calculations(df, calculationData, x_axis, y_axis)
                    #     print("dataframe---------",dataframe.head(5))
                    #     grouped_df = df.groupby(x_axis[0]).size().reset_index(name="count")


                    #     # print("dataframe---------",df.head(5))
                    #     # grouped_df = df.groupby(x_axis[0]).size().reset_index(name="count")
                    #     print("grouped_df---------",grouped_df)
                    #     print("Grouped DataFrame (count):", grouped_df.head())
                    #     categories = grouped_df[x_axis[0]].tolist()
                    #     values = grouped_df["count"].tolist()
                    #     filtered_categories = []
                    #     filtered_values = []
                    #     for category, value in zip(categories, values):
                    #         if category in filter_options:
                    #             filtered_categories.append(category)
                    #             filtered_values.append(value)
                    #     print("Filtered Categories:", filtered_categories)
                    #     print("Filtered Values:", filtered_values)
                    #     chart_data_list.append({
                    #         "categories": filtered_categories,
                    #         "values": filtered_values,
                    #         "chart_id": chart_id,
                    #         "chart_type": chart_type,
                    #         "chart_color": chart_color,
                    #         "x_axis": x_axis,
                    #         "y_axis": y_axis,
                    #         "aggregate": aggregate,
                    #         "positions": chart_positions.get(chart_id),
                    #         "xfontsize": xfontsize,
                    #         "fontstyle" :fontstyle,
                    #         "categorycolor" :categorycolor,
                    #         "valuecolor" :valuecolor,
                    #         "yfontsize" :yfontsize,
                    #         "chart_heading":chart_heading,
                    #         "headingColor":headingColor,
                    #         "filter_options":filter_options, 
                    #         "ClickedTool":ClickedTool,
                    #         "Bgcolour":areacolour,
                    #         "table_name":table_name,  
                    #         "opacity":final_opacity ,
                    #         "calculationData":calculationData ,
                    #         "chart_name":chart_name     
                    #     })
                    if aggregate_py == 'count' and chart_type not in ["duealbarChart", "duealChart","treeHierarchy","Butterfly","tablechart","stackedbar"]:
                        print("COUNT AGGREGATION")
                        print("Aggregate is count", aggregate_py)
                        print("table_name:", table_name)
                        print("y_axis:", y_axis)
                        print("filter_options:", filter_options)
                        print("X-Axis:", x_axis)
                        print("chart type",chart_type)
                        
                        if chart_type is None:
                            chart_typec = chart_data[6]  # Fallback to database value
                            print(f"WARNING: chart_type was None for chart_id {chart_id}, using database value: {chart_type}")
                        # print("chart_type:", chart_typec)                 
                        # Start with fresh data - don't rely on the potentially modified dataframe
                        df = fetch_chart_data(connection, table_name)
                        print("Original df rows before any processing:", len(df))
                        print("Original df columns:", list(df.columns))
                        print("Original df sample:", df.head(3))
                        
                        # Apply calculations first if needed
                        if calculationData:
                            df = apply_calculations(df, calculationData, x_axis, y_axis)
                            print("After calculations:", len(df))
                        
                        # Debug filter_options type and content
                        print("DEBUG: type of filter_options:", type(filter_options))
                        print("DEBUG: filter_options value:", filter_options)
                        print("DEBUG: filter_options bool check:", bool(filter_options))
                        print("DEBUG: isinstance dict check:", isinstance(filter_options, dict))
                        
                        # Parse filter_options if it's a string
                        if isinstance(filter_options, str):
                            try:
                                import json
                                filter_options = json.loads(filter_options)
                                print("DEBUG: Parsed filter_options from string:", filter_options)
                            except json.JSONDecodeError as e:
                                print("DEBUG: Failed to parse filter_options as JSON:", e)
                                filter_options = {}
                        df = apply_and_or_filters(df, filter_options)
                        # Now apply the filters BEFORE grouping
                        # if filter_options and isinstance(filter_options, dict):
                        #     print("Applying filters to df...")
                        #     print("Filter options received:", filter_options)
                            
                        #     for column, valid_values in filter_options.items():
                        #         print(f"\n--- Processing filter for column: '{column}' ---")
                        #         print(f"Filter values: {valid_values}")
                                
                        #         if column in df.columns:
                        #             print(f"Column '{column}' found in dataframe")
                        #             print(f"Unique values in '{column}' before filter (first 20):", sorted(df[column].unique())[:20])
                        #             print(f"Data type of column '{column}':", df[column].dtype)
                        #             print(f"Rows before filtering '{column}': {len(df)}")
                                    
                        #             if valid_values:  # Check if valid_values is not empty
                        #                 # Convert both dataframe values and filter values to strings for consistent comparison
                        #                 df[column] = df[column].astype(str).str.strip()
                        #                 valid_values_str = [str(v).strip() for v in valid_values]
                                        
                        #                 print(f"Converted filter values to strings: {valid_values_str}")
                                        
                        #                 # Show some sample values to compare
                        #                 sample_df_values = df[column].unique()[:10]
                        #                 print(f"Sample df values (as strings): {sample_df_values}")
                                        
                        #                 # Check which filter values actually exist in the data
                        #                 existing_values = []
                        #                 for fv in valid_values_str:
                        #                     if fv in df[column].values:
                        #                         existing_values.append(fv)
                        #                     else:
                        #                         print(f"WARNING: Filter value '{fv}' not found in column '{column}'")
                                        
                        #                 print(f"Filter values that exist in data: {existing_values}")
                                        
                        #                 if existing_values:
                        #                     # Apply the filter
                        #                     df = df[df[column].isin(valid_values_str)]
                        #                     print(f"After filtering '{column}': {len(df)} rows remaining")
                        #                     print(f"Unique values in '{column}' after filter:", sorted(df[column].unique()))
                        #                 else:
                        #                     print(f"ERROR: None of the filter values for '{column}' exist in the data!")
                        #                     print(f"Available values in '{column}': {sorted(df[column].unique())}")
                        #             else:
                        #                 print(f"WARNING: No valid values provided for column '{column}'")
                        #         else:
                        #             print(f"ERROR: Column '{column}' not found in dataframe")
                        #             print(f"Available columns: {list(df.columns)}")
                        # for column, filter_def in filter_options.items():
                        #     print(f"\n--- Processing filter for column: '{column}' ---")
                        #     print("Filter definition:", filter_def)

                        #     if column not in df.columns:
                        #         print(f"ERROR: Column '{column}' not found")
                        #         continue

                        #     # ✅ Extract correctly
                        #     values = filter_def.get("values", [])
                        #     operator = filter_def.get("operator", "AND").upper()

                        #     print("Extracted values:", values)
                        #     print("Operator:", operator)

                        #     if not values:
                        #         print(f"WARNING: No values provided for {column}")
                        #         continue

                        #     # Normalize
                        #     df[column] = df[column].astype(str).str.strip()
                        #     values = [str(v).strip() for v in values]

                        #     # Check existence
                        #     existing_values = [v for v in values if v in df[column].values]

                        #     if not existing_values:
                        #         print(f"ERROR: None of the filter values exist for '{column}'")
                        #         print("Available values:", df[column].unique())
                        #         continue

                        #     # ✅ Apply filter
                        #     df = df[df[column].isin(existing_values)]

                        #     print(f"Rows after filtering '{column}':", len(df))
                        # print(f"\n=== FINAL FILTERING RESULTS ===")
                        # print(f"Final filtered df rows: {len(df)}")
                        
                        if len(df) > 0:
                            print(f"Sample of filtered data:")
                            print(df.head(3))
                            
                            # Group the filtered dataframe
                            # =========================================================
                            #                 DATE GRANULARITY FOR COUNT
                            # =========================================================
                            dateGranularity = selectedFrequency

                            # Parse granularity JSON if string
                            if isinstance(dateGranularity, str):
                                try:
                                    import json
                                    dateGranularity = json.loads(dateGranularity)
                                except:
                                    dateGranularity = {}

                            if dateGranularity and isinstance(dateGranularity, dict):
                                for date_col, granularity in dateGranularity.items():
                                    if date_col in df.columns and date_col in x_axis:
                                        print(f"[COUNT] Applying granularity: {date_col} -> {granularity}")

                                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                                        g = granularity.lower()

                                        granularity_col = f"{date_col}_{g}"

                                        if g == "year":
                                            df[granularity_col] = df[date_col].dt.year.astype(str)

                                        elif g == "quarter":
                                            df[granularity_col] = "Q" + df[date_col].dt.quarter.astype(str)

                                        elif g == "month":
                                            df[granularity_col] = df[date_col].dt.month_name()

                                        elif g == "week":
                                            df[granularity_col] = "Week " + df[date_col].dt.isocalendar().week.astype(str)

                                        elif g == "day":
                                            df[granularity_col] = df[date_col].dt.strftime("%Y-%m-%d")

                                        else:
                                            print(f"Unsupported granularity: {granularity}")
                                            continue

                                        # Replace x_axis column with granularity column
                                        x_axis = [granularity_col if c == date_col else c for c in x_axis]

                                        print(f"[COUNT] Created granularity column → {granularity_col}")
                                        print(df[[date_col, granularity_col]].head())

                            # =========================================================
                            #                 GROUPING AFTER GRANULARITY
                            # =========================================================
                            print(f"\nGrouping by: {x_axis[0]}")
                            # grouped_df = df.groupby(x_axis[0]).size().reset_index(name="count")
                            # grouped_df = df.groupby(x_axis[0])[y_axis[0]].nunique().reset_index(name="count")
                            if y_axis and aggregate_py == "count":
                                # COUNT = number of rows
                                grouped_df = (
                                    df.groupby(x_axis[0])
                                    .size()
                                    .reset_index(name="count")
                                )
                            else:
                                grouped_df = (
                                    df.groupby(x_axis[0])[y_axis[0]]
                                    .nunique()
                                    .reset_index(name="count")
                                )


                            
                            
                            # print("Grouped DataFrame after filtering:", grouped_df)
                            
                            # # Extract final categories and values
                            # categories = grouped_df[x_axis[0]].tolist()
                            # values = grouped_df["count"].tolist()
                            # Group the filtered dataframe


                            print("Grouped DataFrame after filtering:", grouped_df)

                            # Apply optimization filters (top10, bottom10, both5) if specified
                            if 'OptimizationData' in locals() or 'OptimizationData' in globals():
                                if OptimizationData == 'top10':
                                    # Sort descending and get top 10
                                    grouped_df = grouped_df.sort_values(by="count", ascending=False).head(10)
                                    print("Applied top10 optimization filter")
                                elif OptimizationData == 'bottom10':
                                    # Sort ascending and get bottom 10
                                    grouped_df = grouped_df.sort_values(by="count", ascending=True).head(10)
                                    print("Applied bottom10 optimization filter")
                                elif OptimizationData == 'both5':
                                    # Get top 5 and bottom 5
                                    top_df = grouped_df.sort_values(by="count", ascending=False).head(5)
                                    bottom_df = grouped_df.sort_values(by="count", ascending=True).head(5)
                                    grouped_df = pd.concat([top_df, bottom_df])
                                    print("Applied both5 optimization filter")

                            # Extract final categories and values
                            categories = grouped_df[x_axis[0]].tolist()
                            values = grouped_df["count"].tolist()


                            
                            print(f"Final Categories: {categories}")
                            print(f"Final Values: {values}")
                        else:
                            print("ERROR: No data remaining after filtering!")
                            categories = []
                            values = []
                        
                        chart_data_list.append({
                            "categories": categories,
                            "values": values,
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_color": chart_color,
                            "x_axis": x_axis,
                            "y_axis": y_axis,
                            "aggregate": aggregation,
                            "positions": chart_positions.get(chart_id),
                            "xfontsize": xfontsize,
                            "fontstyle": fontstyle,
                            "categorycolor": categorycolor,
                            "valuecolor": valuecolor,
                            "yfontsize": yfontsize,
                            "chart_heading": chart_heading,
                            "headingColor": headingColor,
                            "filter_options": filter_options, 
                            "ClickedTool": ClickedTool,
                            "Bgcolour": areacolour,
                            "table_name": table_name,
                            "database_name": database_name, 
                            "opacity": final_opacity,
                            "calculationData": calculationData,
                            "chart_name": (user_id, chart_name),
                            "user_id": user_id,
                            "xAxisTitle": xAxisTitle,
                            "yAxisTitle" :yAxisTitle   
                        })
                        continue 



                        # continue  # Skip further processing for this chart ID
                    if chart_type == "tablechart":
                        print("Tree hierarchy chart detected")
                        print("Tree hierarchy chart detected")

                        print("tableName====================", table_name)
                        print("x_axis====================", x_axis)
                        print("filter_options====================", filter_options)
                        print("y_axis====================", y_axis)
                        print("aggregate====================", aggregate)
                        print("databaseName====================", database_name)
                        print("selectedUser====================", selected_user)
                        if isinstance(filter_options, str):
                            try:
                                filter_options = json.loads(filter_options)  # Convert JSON string to dict
                            except json.JSONDecodeError:
                                raise ValueError("Invalid JSON format for filter_options")
                        data = fetch_data_tree(table_name, x_axis, filter_options, y_axis, aggregate, database_name,selected_user,calculationData)
                        categories = data.get("categories", [])
                        values = data.get("values", [])
                        print("categories",categories)
                        print("values",values)
                        chart_data_list.append({
                                "categories": categories,
                                "values": values,
                                "chart_id": chart_id,
                                "chart_type": chart_type,
                                "chart_color": chart_color,
                                "x_axis": x_axis,
                                "y_axis": y_axis,
                                "aggregate": aggregation,
                                "positions": chart_positions.get(chart_id),
                                "xfontsize": xfontsize,
                                "fontstyle" :fontstyle,
                                "categorycolor" :categorycolor,
                                "valuecolor" :valuecolor,
                                "yfontsize" :yfontsize,
                                "chart_heading":chart_heading,
                                "headingColor":headingColor,
                                "filter_options":filter_options,
                                "ClickedTool":ClickedTool ,
                                "Bgcolour":areacolour , 
                                "table_name":table_name,
                                "database_name": database_name,
                                "opacity":final_opacity ,
                                "calculationData":calculationData,
                                 "chart_name": (user_id, chart_name),
                                "user_id": user_id    
                            })
                        continue 
                    if chart_type == "treeHierarchy":
                        print("Tree hierarchy chart detected")
                        print("Tree hierarchy chart detected")

                        print("tableName====================", table_name)
                        print("x_axis====================", x_axis)
                        print("filter_options====================", filter_options)
                        print("y_axis====================", y_axis)
                        print("aggregate====================", aggregate)
                        print("databaseName====================", database_name)
                        print("selectedUser====================", selected_user)
                        if isinstance(filter_options, str):
                            try:
                                filter_options = json.loads(filter_options)  # Convert JSON string to dict
                            except json.JSONDecodeError:
                                raise ValueError("Invalid JSON format for filter_options")
                        data = fetch_data_tree(table_name, x_axis, filter_options, y_axis, aggregate, database_name,selected_user,calculationData)
                        categories = data.get("categories", [])
                        values = data.get("values", [])
                        print("categories",categories)
                        print("values",values)
                        if 'OptimizationData' in locals() or 'OptimizationData' in globals():
                            try:
                                # Create DataFrame for easy sorting
                                df = pd.DataFrame(categories)
                                df['value'] = values

                                if OptimizationData == 'top10':
                                    df = df.sort_values(by='value', ascending=False).head(10)
                                elif OptimizationData == 'bottom10':
                                    df = df.sort_values(by='value', ascending=True).head(10)
                                elif OptimizationData == 'both5':
                                    top_df = df.sort_values(by='value', ascending=False).head(5)
                                    bottom_df = df.sort_values(by='value', ascending=True).head(5)
                                    df = pd.concat([top_df, bottom_df])

                                # Convert back to list after filtering
                                values = df['value'].tolist()
                                categories = df.drop(columns=['value']).to_dict(orient='records')
                            except Exception as e:
                                print("Optimization filtering failed:", str(e))
                        chart_data_list.append({
                                "categories": categories,
                                "values": values,
                                "chart_id": chart_id,
                                "chart_type": chart_type,
                                "chart_color": chart_color,
                                "x_axis": x_axis,
                                "y_axis": y_axis,
                                "aggregate": aggregation,
                                "positions": chart_positions.get(chart_id),
                                "xfontsize": xfontsize,
                                "fontstyle" :fontstyle,
                                "categorycolor" :categorycolor,
                                "valuecolor" :valuecolor,
                                "yfontsize" :yfontsize,
                                "chart_heading":chart_heading,
                                "headingColor":headingColor,
                                "filter_options":filter_options,
                                "ClickedTool":ClickedTool ,
                                "Bgcolour":areacolour , 
                                "table_name":table_name,
                                "database_name": database_name,
                                "opacity":final_opacity,
                                "calculationData":calculationData ,
                                "chart_name": (user_id, chart_name),
                                "user_id": user_id    
                            })
                        continue 
                    if chart_type == "duealChart" :
                            print("Dual AXIS Chart")
                            print("calculationData", calculationData)

                            # Parse filter options
                            if isinstance(filter_options, str):
                                try:
                                    filter_options = json.loads(filter_options)
                                except json.JSONDecodeError:
                                    raise ValueError("Invalid JSON format for filter_options")

                            # Parse calculationData if needed
                            if isinstance(calculationData, str):
                                try:
                                    calculationData = json.loads(calculationData)
                                except json.JSONDecodeError:
                                    raise ValueError("Invalid JSON format for calculationData")

                            print("Fetching data...")
                            # data = fetch_data_for_duel(table_name, x_axis, filter_options, y_axis, aggregate, database_name, selected_user,calculationData)
                            data = fetch_data_for_duel(table_name, x_axis, filter_options, y_axis, agg_value, database_name, selected_user,calculationData,dateGranularity=selectedFrequency)
                            print(f"Data fetched for dual chart: {data}")
                            # --- Optimization Filtering ---
                            if 'OptimizationData' in locals() or 'OptimizationData' in globals():
                                df = pd.DataFrame(data, columns=[x_axis[0], 'series1', 'series2'])
                                
                                if OptimizationData == 'top10':
                                    df = df.sort_values(by='series1', ascending=False).head(10)
                                elif OptimizationData == 'bottom10':
                                    df = df.sort_values(by='series1', ascending=True).head(10)
                                elif OptimizationData == 'both5':
                                    top_df = df.sort_values(by='series1', ascending=False).head(5)
                                    bottom_df = df.sort_values(by='series1', ascending=True).head(5)
                                    df = pd.concat([top_df, bottom_df])

                                # Convert filtered df back to list of tuples
                                data = df.values.tolist()
                            # Add a check to see the length of rows
                            chart_data_list.append({
                                "categories": [row[0] for row in data],
                                    "series1":[row[1] for row in data],
                                    "series2": [row[2] for row in data],
                                    "chart_id": chart_id,
                                    "chart_type": chart_type,
                                    "chart_color": chart_color,
                                    "x_axis": x_axis,
                                    "y_axis": y_axis,
                                    "aggregate": aggregation,
                                    "positions": chart_positions.get(chart_id),
                                    "xfontsize": xfontsize,
                                    "fontstyle" :fontstyle,
                                    "categorycolor" :categorycolor,
                                    "valuecolor" :valuecolor,
                                    "yfontsize" :yfontsize,
                                    "chart_heading":chart_heading,
                                    "headingColor":headingColor,
                                    "filter_options":filter_options,
                                    "ClickedTool":ClickedTool,
                                    "Bgcolour":areacolour,
                                    "table_name":table_name,
                                    "database_name": database_name,
                                    "opacity":final_opacity,
                                    "calculationData":calculationData,
                                    "chart_name": (user_id, chart_name),
                                    "user_id": user_id,
                                    "xAxisTitle": xAxisTitle,
                                    "yAxisTitle" :yAxisTitle        
                            })
                    elif chart_type == "Butterfly":
                            print("Butterfly Chart")
                            if isinstance(filter_options, str):
                                try:
                                    filter_options = json.loads(filter_options)  # Convert JSON string to dict
                                except json.JSONDecodeError:
                                    raise ValueError("Invalid JSON format for filter_options")
                            data = fetch_data_for_duel(table_name, x_axis, filter_options, y_axis, aggregate, database_name, selected_user,calculationData,dateGranularity=selectedFrequency)
                           # --- Optimization Filtering ---
                            if 'OptimizationData' in locals() or 'OptimizationData' in globals():
                                df = pd.DataFrame(data, columns=[x_axis[0], 'series1', 'series2'])
                                
                                if OptimizationData == 'top10':
                                    df = df.sort_values(by='series1', ascending=False).head(10)
                                elif OptimizationData == 'bottom10':
                                    df = df.sort_values(by='series1', ascending=True).head(10)
                                elif OptimizationData == 'both5':
                                    top_df = df.sort_values(by='series1', ascending=False).head(5)
                                    bottom_df = df.sort_values(by='series1', ascending=True).head(5)
                                    df = pd.concat([top_df, bottom_df])

                                # Convert filtered df back to list of tuples
                                data = df.values.tolist()
                            chart_data_list.append({
                                "categories": [row[0] for row in data],
                                    "series1":[row[1] for row in data],
                                    "series2": [row[2] for row in data],
                                    "chart_id": chart_id,
                                    "chart_type":chart_type,
                                    "chart_color": chart_color,
                                    "x_axis": x_axis,
                                    "y_axis": y_axis,
                                    "aggregate": aggregation,
                                    "positions": chart_positions.get(chart_id),
                                    "xfontsize": xfontsize,
                                    "fontstyle" :fontstyle,
                                    "categorycolor" :categorycolor,
                                    "valuecolor" :valuecolor,
                                    "yfontsize" :yfontsize,
                                    "chart_heading":chart_heading,
                                    "headingColor":headingColor,
                                    "filter_options":filter_options,
                                    "ClickedTool":ClickedTool,
                                    "Bgcolour":areacolour,
                                    "table_name":table_name,
                                    "database_name": database_name,
                                    "opacity":final_opacity,
                                    "calculationData":calculationData,
                                    "chart_name": (user_id, chart_name),
                                    "user_id": user_id,
                                    "xAxisTitle": xAxisTitle,
                                    "yAxisTitle" :yAxisTitle        
                            })
                    elif chart_type in ["duealbarChart", "stackedbar"]:
                        print("duealbarChart")
                        # filter_options = json.loads(filter_options)
                        print('calculationData',calculationData)
                        if isinstance(filter_options, str):
                                try:
                                    filter_options = json.loads(filter_options)  # Convert JSON string to dict
                                except json.JSONDecodeError:
                                    raise ValueError("Invalid JSON format for filter_options")
                        
                        datass = fetch_data_for_duel_bar(table_name, x_axis, filter_options, y_axis, aggregate, database_name,selected_user,calculationData,dateGranularity=selectedFrequency)
                        print("datass",datass)
                    # --- Optimization Filtering ---
                        if 'OptimizationData' in locals() or 'OptimizationData' in globals():
                            df = pd.DataFrame(datass, columns=[x_axis[0], 'series1', 'series2'])
                            
                            if OptimizationData == 'top10':
                                df = df.sort_values(by='series1', ascending=False).head(10)
                            elif OptimizationData == 'bottom10':
                                df = df.sort_values(by='series1', ascending=True).head(10)
                            elif OptimizationData == 'both5':
                                top_df = df.sort_values(by='series1', ascending=False).head(5)
                                bottom_df = df.sort_values(by='series1', ascending=True).head(5)
                                df = pd.concat([top_df, bottom_df])

                                # Convert filtered df back to list of tuples
                            datass = df.values.tolist()
                       
                        chart_data_list.append({
                            "categories": [row[0] for row in datass],
                                "series1":[row[1] for row in datass],
                                "series2": [row[2] for row in datass],
                                "chart_id": chart_id,
                                "chart_type": chart_type,
                                "chart_color": chart_color,
                                "x_axis": x_axis,
                                "y_axis": y_axis,
                                "aggregate": aggregation,
                                "positions": chart_positions.get(chart_id),
                                "xfontsize": xfontsize,
                                "fontstyle" :fontstyle,
                                "categorycolor" :categorycolor,
                                "valuecolor" :valuecolor,
                                "yfontsize" :yfontsize,
                                "chart_heading":chart_heading,
                                "headingColor":headingColor,
                                "filter_options":filter_options,
                                "ClickedTool":ClickedTool,
                                "Bgcolour":areacolour,
                                "table_name":table_name,
                                "database_name": database_name,
                                "opacity":final_opacity,
                                "calculationData":calculationData ,
                                "chart_name": (user_id, chart_name),
                                "user_id": user_id,
                                "xAxisTitle": xAxisTitle,
                                "yAxisTitle" :yAxisTitle       
                        })

                    elif chart_type == "sampleAitestChart":
                        try:
                            # Fetch chart data
                            df = fetch_chart_data(connection, table_name)
                            print("Chart ID", chart_id)
                            print("//////////",df.head(5))
                            
                            # Handle column data types (conversion and cleaning)
                            df, numeric_columns, text_columns = handle_column_data_types(df)

                            # Generate histogram details
                            histogram_details = generate_histogram_details(df)
                            connection.close()
                            chart_data_list.append({
                             "histogram_details": histogram_details,  
                             "chart_type": chart_data[6]
                        })
                        except Exception as e:
                            print("Error while processing chart:", e)
                            return jsonify({"error": "An error occurred while generating the chart."}), 500
                    elif chart_type == "AiCharts":
                        try:
                            # Fetch chart data
                            df=fetch_ai_saved_chart_data(masterdatabasecon, tableName="table_chart_save",chart_id=chart_id)
                            print("Chart ID", chart_id)
                            print("df:",df)
                            connection.close()
                            chart_data_list.append({
                             "histogram_details": df,  
                             "chart_type": chart_data[6]
                        })
                        except Exception as e:
                            print("Error while processing chart:", e)
                            return jsonify({"error": "An error occurred while generating the chart."}), 500
                 
                   
                    else:
                        # x axis 1 and y axis 1
                        # x axis 1 and y axis 1
                       # =========================================================
                        #     DATE GRANULARITY (NORMAL AGG) — SAME AS COUNT VERSION
                        # =========================================================
                        if isinstance(filter_options, str):
                            try:
                                filter_options = json.loads(filter_options)
                            except:
                                filter_options = {}

                        print("DataFrame after dashboard filtering}}}}}}}}-----")
                        print(dataframe.head())

                        # if filter_options:
                        #     for col, allowed_values in filter_options.items():
                        #         if col in dataframe.columns:
                        #             print("Applying filter on column:", col)
                        #             print("Allowed values:", allowed_values)
                        #             dataframe = dataframe[dataframe[col].isin(allowed_values)]


                        # if filter_options:
                        #     for col, allowed_values in filter_options.items():
                        #         if col in dataframe.columns:
                        #             print(f"Applying filter on column: {col}")
                                    
                        #             # CHECK: If the column is a date column and we are filtering by years
                        #             # we need to extract the year before using .isin()
                        #             is_date_col = pd.api.types.is_datetime64_any_dtype(dataframe[col]) or "date" in col.lower()
                                    
                        #             if is_date_col:
                        #                 # Ensure it's datetime format to extract the year
                        #                 temp_dates = pd.to_datetime(dataframe[col], errors='coerce')
                        #                 # Filter rows where the year of the date is in our allowed_values list
                        #                 dataframe = dataframe[temp_dates.dt.year.isin(allowed_values)]
                        #             else:
                        #                 # Normal filtering for non-date columns (like 'region' or 'product')
                        #                 dataframe = dataframe[dataframe[col].isin(allowed_values)]
                        # if filter_options:
                        #     for col, allowed_values in filter_options.items():
                        #         if col in dataframe.columns:
                        #             print(f"Applying filter on column: {col}")
                                    
                        #             is_date_col = pd.api.types.is_datetime64_any_dtype(dataframe[col]) or "date" in col.lower()
                                    
                        #             if is_date_col:
                        #                 # 1. Convert column to datetime for extraction
                        #                 temp_dates = pd.to_datetime(dataframe[col], errors='coerce')
                                        
                        #                 # 2. Check the nature of the filter values
                        #                 sample_val = str(allowed_values[0]) if allowed_values else ""
                                        
                        #                 if sample_val.isdigit():
                        #                     # Filter by YEAR (e.g., [2010, 2011])
                        #                     years = [int(v) for v in allowed_values]
                        #                     dataframe = dataframe[temp_dates.dt.year.isin(years)]
                                            
                        #                 elif sample_val.startswith('Q') and len(sample_val) <= 2:
                        #                     # Filter by QUARTER (e.g., ["Q1", "Q2"])
                        #                     # Extract '1' from 'Q1' and compare
                        #                     quarters = [int(v.replace('Q', '')) for v in allowed_values]
                        #                     dataframe = dataframe[temp_dates.dt.quarter.isin(quarters)]
                                            
                        #                 else:
                        #                     # Filter by MONTH NAME (e.g., ["January", "February"])
                        #                     # We compare month names (case-insensitive)
                        #                     allowed_months = [v.strip().capitalize() for v in allowed_values]
                        #                     dataframe = dataframe[temp_dates.dt.month_name().isin(allowed_months)]
                        #             else:
                        #                 # Normal filtering for non-date columns
                        #                 # allowed_values_lower = [str(v).lower() for v in allowed_values]
                        #                 # dataframe = dataframe[dataframe[col].str.lower().isin(allowed_values_lower)]
                        #                 dataframe = dataframe[dataframe[col].isin(allowed_values)]
                        # if filter_options:
                        #     final_mask = pd.Series(True, index=dataframe.index)

                        #     for col, filter_data in filter_options.items():
                        #         if col not in dataframe.columns:
                        #             continue

                        #         # Normalize
                        #         if isinstance(filter_data, dict):
                        #             values = filter_data.get("values", [])
                        #         else:
                        #             values = filter_data

                        #         if not values:
                        #             continue

                        #         # Build column mask
                        #         if pd.api.types.is_datetime64_any_dtype(dataframe[col]) or "date" in col.lower():
                        #             temp_dates = pd.to_datetime(dataframe[col], errors="coerce")
                        #             sample_val = str(values[0])

                        #             if sample_val.isdigit():
                        #                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])
                        #             elif sample_val.startswith("Q"):
                        #                 col_mask = temp_dates.dt.quarter.isin(
                        #                     [int(v.replace("Q", "")) for v in values]
                        #                 )
                        #             else:
                        #                 col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
                        #         else:
                        #             # OR within same column
                        # #             col_mask = dataframe[col].isin(values)

                        # #         # ✅ AND across columns
                        # #         final_mask &= col_mask

                        # #     dataframe = dataframe[final_mask]
                        # if filter_options:
                        #     and_mask = pd.Series(True, index=dataframe.index)
                        #     or_mask = pd.Series(False, index=dataframe.index)

                        #     has_or = False
                        #     has_and = False

                        #     for col, filter_data in filter_options.items():
                        #         if col not in dataframe.columns:
                        #             continue

                        #         if isinstance(filter_data, dict):
                        #             values = filter_data.get("values", [])
                        #             operator = filter_data.get("operator", "AND").upper()
                        #         else:
                        #             values = filter_data
                        #             operator = "AND"

                        #         if not values:
                        #             continue

                        #         # Build column mask
                        #         if pd.api.types.is_datetime64_any_dtype(dataframe[col]) or "date" in col.lower():
                        #             temp_dates = pd.to_datetime(dataframe[col], errors="coerce")
                        #             sample_val = str(values[0])

                        #             if sample_val.isdigit():
                        #                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])
                        #             elif sample_val.startswith("Q"):
                        #                 col_mask = temp_dates.dt.quarter.isin(
                        #                     [int(v.replace("Q", "")) for v in values]
                        #                 )
                        #             else:
                        #                 col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
                        #         else:
                        #             col_mask = dataframe[col].isin(values)

                        #         # 🔥 APPLY OPERATOR
                        #         if operator == "OR":
                        #             or_mask |= col_mask
                        #             has_or = True
                        #         else:
                        #             and_mask &= col_mask
                        #             has_and = True

                        #     # ✅ FINAL COMBINATION LOGIC
                        #     if has_and and has_or:
                        #         dataframe = dataframe[and_mask & or_mask]
                        #     elif has_or:
                        #         dataframe = dataframe[or_mask]
                        #     else:
                        #         dataframe = dataframe[and_mask]
                        # if filter_options:
                        #     base_mask = pd.Series(True, index=dataframe.index)

                        #     or_group_mask = pd.Series(False, index=dataframe.index)
                        #     has_or_group = False
                            

                        #     for col, filter_data in filter_options.items():
                        #         if col not in dataframe.columns:
                        #             continue

                        #         if isinstance(filter_data, dict):
                        #             values = filter_data.get("values", [])
                        #             operator = filter_data.get("operator", "AND").upper()
                        #         else:
                        #             values = filter_data
                        #             operator = "AND"

                        #         if not values:
                        #             continue

                        #         # Build column mask
                        #         if pd.api.types.is_datetime64_any_dtype(dataframe[col]) or "date" in col.lower():
                        #             temp_dates = pd.to_datetime(dataframe[col], errors="coerce")
                        #             sample_val = str(values[0])

                        #             if sample_val.isdigit():
                        #                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])
                        #             elif sample_val.startswith("Q"):
                        #                 col_mask = temp_dates.dt.quarter.isin(
                        #                     [int(v.replace("Q", "")) for v in values]
                        #                 )
                        #             else:
                        #                 col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
                        #         else:
                        #             col_mask = dataframe[col].isin(values)

                        #         # 🔥 GROUPED LOGIC
                        #         if operator == "OR":
                        #             or_group_mask |= col_mask
                        #             has_or_group = True
                        #         else:
                        #             base_mask &= col_mask

                        #     # ✅ FINAL COMBINATION (SQL-ACCURATE)
                        #     if has_or_group:
                        #         dataframe = dataframe[base_mask & or_group_mask]
                        #     else:
                        #         dataframe = dataframe[base_mask]
                        if filter_options:
                            and_mask = pd.Series(True, index=dataframe.index)
                            or_mask = pd.Series(False, index=dataframe.index)

                            # 🔥 detect if ANY OR exists
                            has_or = any(
                                isinstance(v, dict) and v.get("operator", "").upper() == "OR"
                                for v in filter_options.values()
                            )

                            for col, filter_data in filter_options.items():
                                if col not in dataframe.columns:
                                    continue

                                if isinstance(filter_data, dict):
                                    values = filter_data.get("values", [])
                                    operator = filter_data.get("operator", "AND").upper()
                                else:
                                    values = filter_data
                                    operator = "AND"

                                if not values:
                                    continue

                                # Build column mask
                                if pd.api.types.is_datetime64_any_dtype(dataframe[col]) or "date" in col.lower():
                                    temp_dates = pd.to_datetime(dataframe[col], errors="coerce")
                                    sample_val = str(values[0])

                                    if sample_val.isdigit():
                                        col_mask = temp_dates.dt.year.isin([int(v) for v in values])
                                    elif sample_val.startswith("Q"):
                                        col_mask = temp_dates.dt.quarter.isin(
                                            [int(v.replace("Q", "")) for v in values]
                                        )
                                    else:
                                        col_mask = temp_dates.dt.strftime("%Y-%m-%d").isin(values)
                                else:
                                    col_mask = dataframe[col].isin(values)

                                # ✅ AUTO GROUPING
                                if has_or and operator in ("AND", "OR") and col != "country":
                                    # region + product → OR group
                                    or_mask |= col_mask
                                else:
                                    # country → AND group
                                    and_mask &= col_mask

                            # ✅ FINAL SQL EQUIVALENT
                            if has_or:
                                dataframe = dataframe[or_mask & and_mask]
                            else:
                                dataframe = dataframe[and_mask]





                        # if filter_options:
                        #     for col, filter_data in filter_options.items():
                        #         if col not in dataframe.columns:
                        #             continue

                        #         print(f"Applying filter on column: {col}")

                        #         # 🔹 Normalize filter_data
                        #         if isinstance(filter_data, dict):
                        #             allowed_values = filter_data.get("values", [])
                        #             operator = filter_data.get("operator", "AND")
                        #         else:
                        #             allowed_values = filter_data
                        #             operator = "AND"

                        #         if not allowed_values:
                        #             continue

                        #         is_date_col = (
                        #             pd.api.types.is_datetime64_any_dtype(dataframe[col])
                        #             or "date" in col.lower()
                        #         )

                        #         if is_date_col:
                        #             temp_dates = pd.to_datetime(dataframe[col], errors="coerce")
                        #             sample_val = str(allowed_values[0])

                        #             if sample_val.isdigit():
                        #                 years = [int(v) for v in allowed_values]
                        #                 dataframe = dataframe[temp_dates.dt.year.isin(years)]

                        #             elif sample_val.startswith("Q"):
                        #                 quarters = [int(v.replace("Q", "")) for v in allowed_values]
                        #                 dataframe = dataframe[temp_dates.dt.quarter.isin(quarters)]

                        #             else:
                        #                 months = [v.strip().capitalize() for v in allowed_values]
                        #                 dataframe = dataframe[temp_dates.dt.month_name().isin(months)]
                        #         else:
                        #             # 🔹 AND / OR logic
                        #             if operator == "OR":
                        #                 dataframe = dataframe[dataframe[col].isin(allowed_values)]
                        #             else:  # AND (default & restricted roles)
                        #                 dataframe = dataframe[dataframe[col].isin(allowed_values)]
                        # if filter_options:
                        #     and_mask = pd.Series(True, index=dataframe.index)
                        #     or_masks = []
                        #     print("and_mask",and_mask)
                        #     print("or_masks",or_masks)

                        #     for col, filter_data in filter_options.items():
                        #         if col not in dataframe.columns:
                        #             continue

                        #         # Normalize
                        #         if isinstance(filter_data, dict):
                        #             values = filter_data.get("values", [])
                        #             operator = filter_data.get("operator", "AND").upper()
                        #             print("operator",operator)

                        #         else:
                        #             values = filter_data
                        #             operator = "AND"

                        #         if not values:
                        #             continue

                        #         is_date_col = (
                        #             pd.api.types.is_datetime64_any_dtype(dataframe[col])
                        #             or "date" in col.lower()
                        #         )

                        #         # Build column mask
                        #         if is_date_col:
                        #             temp_dates = pd.to_datetime(dataframe[col], errors="coerce")
                        #             sample_val = str(values[0])

                        #             if sample_val.isdigit():
                        #                 col_mask = temp_dates.dt.year.isin([int(v) for v in values])
                        #             elif sample_val.startswith("Q"):
                        #                 col_mask = temp_dates.dt.quarter.isin(
                        #                     [int(v.replace("Q", "")) for v in values]
                        #                 )
                        #             else:
                        #                 col_mask = temp_dates.dt.month_name().isin(
                        #                     [v.strip().capitalize() for v in values]
                        #                 )
                        #         else:
                        #             col_mask = dataframe[col].isin(values)

                        #         # 🔥 KEY FIX
                        #         if operator == "OR":
                        #             or_masks.append(col_mask)
                        #         else:
                        #             and_mask &= col_mask

                        #     # Apply OR group together
                        #     if or_masks:
                        #         or_mask = or_masks[0]
                        #         for m in or_masks[1:]:
                        #             or_mask |= m

                        #         dataframe = dataframe[and_mask & or_mask]
                        #         print("dataframe1",dataframe)
                        #     else:
                        #         dataframe = dataframe[and_mask]
                        # if filter_options:
                            # combined_mask = pd.Series(False, index=dataframe.index)

                            # for col, filter_data in filter_options.items():
                            #     if col not in dataframe.columns:
                            #         continue

                            #     if isinstance(filter_data, dict):
                            #         values = filter_data.get("values", [])
                            #         operator = filter_data.get("operator", "AND").upper()
                            #     else:
                            #         values = filter_data
                            #         operator = "AND"

                            #     if not values:
                            #         continue

                            #     # Build column mask
                            #     if pd.api.types.is_datetime64_any_dtype(dataframe[col]) or "date" in col.lower():
                            #         temp_dates = pd.to_datetime(dataframe[col], errors="coerce")
                            #         sample_val = str(values[0])

                            #         if sample_val.isdigit():
                            #             col_mask = temp_dates.dt.year.isin([int(v) for v in values])
                            #         elif sample_val.startswith("Q"):
                            #             col_mask = temp_dates.dt.quarter.isin(
                            #                 [int(v.replace("Q", "")) for v in values]
                            #             )
                            #         else:
                            #             col_mask = temp_dates.dt.month_name().isin(
                            #                 [v.strip().capitalize() for v in values]
                            #             )
                            #     else:
                            #         col_mask = dataframe[col].isin(values)

                            #     # 🔥 OR across different columns
                            #     combined_mask |= col_mask

                            # dataframe = dataframe[combined_mask]
                        




                        print("DataFrame after dashboard filtering:-----")
                        print(dataframe.head())
                        dateGranularity = selectedFrequency

                        # Parse granularity JSON if string
                        if isinstance(dateGranularity, str):
                            try:
                                import json
                                dateGranularity = json.loads(dateGranularity)
                            except:
                                dateGranularity = {}

                        if dateGranularity and isinstance(dateGranularity, dict):
                            for date_col, granularity in dateGranularity.items():
                                if date_col in dataframe.columns and date_col in x_axis:
                                    print(f"[AGG] Applying granularity: {date_col} -> {granularity}")

                                    dataframe[date_col] = pd.to_datetime(dataframe[date_col], errors='coerce')
                                    g = granularity.lower()

                                    granularity_col = f"{date_col}"
                                    print("=============================================(dataframe[date_col]dataframe[date_col])===================================")
                                    print("dataframe[date_col]",dataframe[date_col])

                                    # Apply correct granularity
                                    if g == "year":
                                        dataframe[granularity_col] = dataframe[date_col].dt.year.astype(str)
                                        print("=============================================year===================================")
                                        print("dataframe[granularity_col]",dataframe[granularity_col])

                                    elif g == "quarter":
                                        dataframe[granularity_col] = "Q" + dataframe[date_col].dt.quarter.astype(str)

                                    elif g == "month":
                                        dataframe[granularity_col] = dataframe[date_col].dt.month_name()

                                    elif g == "week":
                                        dataframe[granularity_col] = "Week " + dataframe[date_col].dt.isocalendar().week.astype(str)

                                    elif g == "day":
                                        dataframe[granularity_col] = dataframe[date_col].dt.strftime("%Y-%m-%d")

                                    else:
                                        print(f"Unsupported granularity: {granularity}")
                                        continue

                                    # Replace the x-axis column with the new granularity column
                                    x_axis = [granularity_col if c == date_col else c for c in x_axis]

                                    print(f"[AGG] Created granularity column → {granularity_col}")
                                    print(dataframe[[date_col, granularity_col]].head())
                            
                        grouped_df = dataframe.groupby(x_axis[0])[y_axis].agg(aggregate_py).reset_index()
                        print("Grouped DataFrame:", grouped_df.head())

                        # Apply optimization filtering if specified
                        if 'OptimizationData' in locals() or 'OptimizationData' in globals():
                            if OptimizationData == 'top10':
                                # Sort descending and get top 10
                                grouped_df = grouped_df.sort_values(by=y_axis[0], ascending=False).head(10)
                            elif OptimizationData == 'bottom10':
                                # Sort ascending and get bottom 10
                                grouped_df = grouped_df.sort_values(by=y_axis[0], ascending=True).head(10)
                            elif OptimizationData == 'both5':
                                # Get top 5 and bottom 5
                                top_df = grouped_df.sort_values(by=y_axis[0], ascending=False).head(5)
                                bottom_df = grouped_df.sort_values(by=y_axis[0], ascending=True).head(5)
                                grouped_df = pd.concat([top_df, bottom_df])

                        categories = grouped_df[x_axis[0]].tolist()
                        if categories:
                            if isinstance(categories[0], pd.Timestamp):  # Assumes at least one value is present
                                categories = [category.strftime('%Y-%m-%d') for category in categories]
                            else:
                                categories = [str(category) for category in categories]  
                        else:
                            categories = []
                        values = [float(value) for value in grouped_df[y_axis[0]]]

                        print("categories--222", categories)
                        print("values--222", values)

                        # # Filter categories and values based on filter_options
                        # filtered_categories = []
                        # filtered_values = []
                        # for category, value in zip(categories, values):
                        #     if category in filter_options:
                        #         filtered_categories.append(category)
                        #         filtered_values.append(value)
                        # if isinstance(filter_options, str):
                        #         try:
                        #             filter_options = json.loads(filter_options)  # Convert JSON string to dict
                        #         except json.JSONDecodeError:
                        #             raise ValueError("Invalid JSON format for filter_options")
                        if isinstance(selectedFrequency, str):
                            try:
                                selectedFrequency = json.loads(selectedFrequency)
                            except json.JSONDecodeError:
                                selectedFrequency = {}
                        print("selectedFrequency",selectedFrequency)
                        # if selectedFrequency:
                        #     filtered_categories = categories
                        #     filtered_values = values
                        # else:
                        #     filtered_categories = []
                        #     filtered_values = []
                        #     for category, value in zip(categories, values):
                        #         if category in filter_options:
                        #             print("category",category,filter_options)
                        #             filtered_categories.append(category)
                        #             filtered_values.append(value)
                        axis_col = x_axis[0]

                        # 🔥 Detect OR across ANY filter
                        has_or_logic = any(
                            isinstance(v, dict) and v.get("operator", "").upper() == "OR"
                            for v in filter_options.values()
                        )

                        if selectedFrequency or has_or_logic:
                            # ✅ TRUST GROUPED DATA — DO NOT AXIS FILTER
                            filtered_categories = categories
                            filtered_values = values

                        else:
                            filtered_categories = []
                            filtered_values = []

                            if not filter_options or axis_col not in filter_options:
                                filtered_categories = categories
                                filtered_values = values
                            else:
                                filter_data = filter_options[axis_col]

                                if isinstance(filter_data, dict):
                                    allowed_values = filter_data.get("values", [])
                                else:
                                    allowed_values = filter_data

                                allowed_values_lower = [str(v).strip().lower() for v in allowed_values]

                                for category, value in zip(categories, values):
                                    if str(category).strip().lower() in allowed_values_lower:
                                        filtered_categories.append(category)
                                        filtered_values.append(value)

                        print("Filtered Categories:", filtered_categories)
                        print("Filtered Values:", filtered_values)

                        # axis_col = x_axis[0]  # e.g., 'brand'
                        # if selectedFrequency:
                        #     filtered_categories = categories
                        #     filtered_values = values
                        #     print("Filtered Categories1:", filtered_categories)
                        #     print("Filtered Values1:", filtered_values)
                        # else:
                        #     filtered_categories = []
                        #     filtered_values = []
                        #     # allowed_values_lower = [str(v).strip().lower() for v in filter_options.get(axis_col, [])]
                        #     # if not allowed_values_lower:
                        #     #     filtered_categories = categories
                        #     #     filtered_values = values
                        #     # else:
                        #     #     for category, value in zip(categories, values):
                        #     #         if str(category).strip().lower() in allowed_values_lower:
                        #     #             filtered_categories.append(category)
                        #     #             filtered_values.append(value)

                        #     for category, value in zip(categories, values):
                        #         print("filter_options",filter_options,axis_col)

                        #         if not filter_options or axis_col not in filter_options:
                        #             filtered_categories = categories
                        #             filtered_values = values
                        #             # break  

                        #         if axis_col in filter_options and category in filter_options[axis_col]:
                        #         # allowed_values_lower = [str(v).lower() for v in filter_options[axis_col]]
                        #         # if str(category).lower() in allowed_values_lower:
                                    
                        #             filtered_categories.append(category)
                        #             filtered_values.append(value)
                        #     print("Filtered Categories:", filtered_categories)
                        #     print("Filtered Values:", filtered_values)


                        # print("Filtered Categories:", filtered_categories)
                        # print("Filtered Values:", filtered_values)

                        chart_data_list.append({
                            "categories": filtered_categories,
                            "values": filtered_values,
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_color": chart_color,
                            "x_axis": x_axis,
                            "y_axis": y_axis,
                            "aggregate": aggregation,
                            "positions": chart_positions.get(chart_id),
                            "xfontsize": xfontsize,
                            "fontstyle": fontstyle,
                            "categorycolor": categorycolor,
                            "valuecolor": valuecolor,
                            "yfontsize": yfontsize,
                            "chart_heading": chart_heading,
                            "headingColor": headingColor,
                            "table_name": table_name,
                            "database_name": database_name,
                            "filter_options": filter_options,
                            "ClickedTool": ClickedTool,
                            "Bgcolour": areacolour,
                            "table_name": table_name,
                            "database_name": database_name,
                            "OptimizationData": OptimizationData if 'OptimizationData' in locals() or 'OptimizationData' in globals() else None,
                            "opacity":final_opacity,
                            "calculationData":calculationData,
                            "chart_name": (user_id, chart_name),

                            "user_id": user_id,  
                            "xAxisTitle": xAxisTitle,
                            "yAxisTitle" :yAxisTitle       
                        })


            print("chart_ids",chart_ids)        
            conn.close()  # Close the main connection
            return chart_data_list

        except psycopg2.Error as e:
            
            print("Error fetching chart data:", e)
            conn.close()
            return None
    else:
        return None


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
                match = (
                    re.match(
                        r"if\s*\(\s*(.+?)\s*\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$",
                        calc_formula.strip(),
                        re.IGNORECASE
                    )
                    or
                    re.match(
                        r"if\s*\(\s*(.+?)\s*,\s*'?(.*?)'?\s*,\s*'?(.*?)'?\s*\)$",
                        calc_formula.strip(),
                        re.IGNORECASE
                    )
                )

                # match = re.match(r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$", calc_formula.strip(), re.IGNORECASE)
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


def fetch_TreeHierarchy_Data(connection, tableName):
    try:
        cursor = connection.cursor()

        # Use SQL composition to safely query using dynamic table and column names
        query = sql.SQL("SELECT * FROM {table}")
        query = query.format(
            table=sql.Identifier(tableName)
        )

        cursor.execute(query)
        results = cursor.fetchall()

        # Fetch the column names from the cursor
        column_names = [desc[0] for desc in cursor.description]

        # Convert the results to a DataFrame with the column names
        df = pd.DataFrame(results, columns=column_names)
        print("df",df)

        cursor.close()

        return df

    except Exception as e:
        raise Exception(f"Error fetching data from {tableName}: {str(e)}")
    


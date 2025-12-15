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
import numpy as np
import paramiko
import socket
import threading

from statsmodels.tsa.seasonal import seasonal_decompose

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
def fetch_project_names(user_id, database_name):
    conn_company = get_company_db_connection(database_name)
    all_employee_ids = []

    if conn_company:
        try:
            with conn_company.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name='employee_list' AND column_name='reporting_id'
                """)
                column_exists = cursor.fetchone()

                if column_exists:
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
                    """, (user_id, user_id))
                    reporting_employees = [row[0] for row in cursor.fetchall()]
                    all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]
                else:
                    all_employee_ids = [int(user_id)] # If no reporting_id, just include user_id
        except psycopg2.Error as e:
            print(f"Error fetching reporting employees for project names: {e}")
        finally:
            conn_company.close()

    conn_datasource = get_db_connection(DB_NAME)
    project_names = []

    if conn_datasource and all_employee_ids:
        try:
            with conn_datasource.cursor() as cursor:
                placeholders = ', '.join(['%s'] * len(all_employee_ids))
                query = f"""
                    SELECT DISTINCT project_name FROM table_dashboard
                    WHERE user_id IN ({placeholders}) AND company_name = %s;
                """
                cursor.execute(query, tuple(map(str, all_employee_ids)) + (database_name,))
                project_names = [row[0] for row in cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"Error fetching project names: {e}")
        finally:
            conn_datasource.close()

    return project_names


def get_dashboard_names(user_id, database_name, project_name=None):
    # Step 1: Get employees reporting to the given user_id from the company database.
    conn_company = get_company_db_connection(database_name)
    reporting_employees = []

    if conn_company:
        try:
            with conn_company.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name='employee_list' AND column_name='reporting_id'
                """)
                column_exists = cursor.fetchone()

                if column_exists:
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
                    """, (user_id, user_id))
                    reporting_employees = [row[0] for row in cursor.fetchall()]
                else:
                    reporting_employees = [] # If no reporting_id, only user_id will be considered below

        except psycopg2.Error as e:
            print(f"Error fetching reporting employees: {e}")
        finally:
            conn_company.close()

    # Include the user's own employee_id for fetching their charts.
    all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]

    # Step 2: Fetch dashboard names for these employees from the datasource database.
    conn_datasource = get_db_connection(DB_NAME)
    dashboard_names = {}

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                placeholders = ', '.join(['%s'] * len(all_employee_ids))
                # query = f"""
                #     SELECT user_id, file_name FROM table_dashboard
                #     WHERE user_id IN ({placeholders}) AND company_name = %s AND project_name = %s 
                # """
                # params = tuple(map(str, all_employee_ids)) + (database_name, project_name)
                
                # if project_name: # Add project_name filter if provided
                #     query += " AND project_name = %s"
                #     params += (project_name,)

                if project_name:
                    query = f"""
                        SELECT user_id, file_name
                        FROM table_dashboard
                        WHERE user_id IN ({placeholders})
                        AND company_name = %s
                        AND project_name = %s
                        ORDER BY updated_at DESC;
                    """
                    params = tuple(map(str, all_employee_ids)) + (database_name, project_name)

                else:
                    query = f"""
                        SELECT user_id, file_name
                        FROM table_dashboard
                        WHERE user_id IN ({placeholders})
                        AND company_name = %s
                        ORDER BY updated_at DESC;
                    """
                    params = tuple(map(str, all_employee_ids)) + (database_name,)
                cursor.execute(query, params)
                dashboards = cursor.fetchall()
                print("dashboards",dashboards)

                # for uid, file_name in dashboards:
                #     if uid not in dashboard_names:
                #         dashboard_names[uid] = []
                #     dashboard_names[uid].append(file_name)
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
                match = re.match(
                    r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$",
                    calc_formula, re.IGNORECASE
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

            else:
                # Default fallback: eval with replaced columns
                calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
                dataframe[new_col_name] = eval(calc_formula_python)

            print(f"✅ Created new column: {new_col_name}")

            # Update axes with new column name if replaced
            if y_axis:
                y_axis = [new_col_name if col == replace_col_name else col for col in y_axis]
            if x_axis:
                x_axis = [new_col_name if col == replace_col_name else col for col in x_axis]

        except Exception as e:
            print(f"Error processing formula '{calc_formula}': {e}")

    return dataframe




def get_dashboard_view_chart_data(chart_ids,positions,filter_options,areacolour,droppableBgColor,opacity,image_ids,chart_type,dashboard_Filter,view_mode):
    conn = create_connection()  # Initial connection to your main database
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
          

            print("chart_positions",chart_positions)
            print("Chart Filters:", chart_filters)
            print("Processed chart_areacolour:", chart_areacolour)
            for chart_id, position in chart_positions.items():
                if not isinstance(position, dict) or 'x' not in position or 'y' not in position:
                    print(f"Invalid position for chart_id {chart_id}: {position}")
                    return []
            sorted_chart_ids = sorted(chart_ids, key=lambda x: (chart_positions.get(x, {'x': 0, 'y': 0})['x'], chart_positions.get(x, {'x': 0, 'y': 0})['y']))
            chart_data_list = []
            print("chart_data_list",chart_data_list)
            for chart_id in sorted_chart_ids:
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
                    database_name = chart_data[1]  # Assuming `database_name` is the second field
                    table_name = chart_data[2]
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
                    # Clean agg_value from quotes
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
                    if isinstance(agg_value, str) and agg_value.lower() in ["minimum","maximum","sum", "count", "avg", "mean", "min", "max","average"]:
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
                        if isinstance(dashboard_Filter, str):
                            try:
                                dashboard_Filter = json.loads(dashboard_Filter.replace("'", '"'))
                            except Exception:
                                dashboard_Filter = ast.literal_eval(dashboard_Filter)

                        print("Normalized Dashboard Filter:", dashboard_Filter)

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
                        'maximum': 'max'
                    }.get(aggregate, 'sum')  # Default to 'sum' if no match

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

                        db_details = {
                            "name": connection_details[1],
                            "dbType": connection_details[2],
                            "host": connection_details[3],
                            "user": connection_details[4],
                            "password": connection_details[5],
                            "port": int(connection_details[6]),
                            "database": connection_details[7],
                            "use_ssh": connection_details[8],
                            "ssh_host": connection_details[9],
                            "ssh_port": int(connection_details[10]),
                            "ssh_username": connection_details[11],
                            "ssh_key_path": connection_details[12],
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

                        connection = psycopg2.connect(
                            dbname=db_details["database"],
                            user=db_details["user"],
                            password=db_details["password"],
                            host=host,
                            port=port
                        )

                        print("✅ External PostgreSQL connection established successfully!")

                        print('External Connection established:', connection)
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
                            'maximum': 'max'
                        }.get(aggregate, 'sum') 
                        
                        single_value_result = fetchText_data(database_name, table_name, x_axis[0], aggregate_py,selected_user)
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
                            'maximum': 'max'
                        }.get(aggregate, 'sum') 
                        single_value_result = fetchText_data(database_name, table_name, x_axis[0], aggregate_py,selected_user)
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
                            "opacity":final_opacity,
                            "chart_name": (user_id, chart_name),
                            "user_id": user_id  
                        })
                        continue  # Skip further processing for this chart ID
                    # Proceed with category and value generation for non-singleValueChart types
                    dataframe = fetch_chart_data(connection, table_name)
                    print("Chart ID", chart_id)
                 
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
                                match = re.match(r"if\s*\((.+?)\)\s*then\s*'?(.*?)'?\s*else\s*'?(.*?)'?$", calc_formula.strip(), re.IGNORECASE)
                                if not match:
                                    raise ValueError("Invalid IF format")
                                condition_expr, then_val, else_val = match.groups()
                                condition_expr_python = re.sub(r'\[(.*?)\]', replace_column, condition_expr)
                                dataframe[new_col_name] = np.where(eval(condition_expr_python), then_val.strip("'\""), else_val.strip("'\""))

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

                            else:
                                calc_formula_python = re.sub(r'\[(.*?)\]', replace_column, calc_formula)
                                dataframe[new_col_name] = eval(calc_formula_python)

                            print(f"✅ New column '{new_col_name}' created.")

                            # Replace in axes
                            if y_axis:
                                y_axis = [new_col_name if col == replace_col_name else col for col in y_axis]
                            if x_axis:
                                x_axis = [new_col_name if col == replace_col_name else col for col in x_axis]

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
                        
                        # Now apply the filters BEFORE grouping
                        if filter_options and isinstance(filter_options, dict):
                            print("Applying filters to df...")
                            print("Filter options received:", filter_options)
                            
                            for column, valid_values in filter_options.items():
                                print(f"\n--- Processing filter for column: '{column}' ---")
                                print(f"Filter values: {valid_values}")
                                
                                if column in df.columns:
                                    print(f"Column '{column}' found in dataframe")
                                    print(f"Unique values in '{column}' before filter (first 20):", sorted(df[column].unique())[:20])
                                    print(f"Data type of column '{column}':", df[column].dtype)
                                    print(f"Rows before filtering '{column}': {len(df)}")
                                    
                                    if valid_values:  # Check if valid_values is not empty
                                        # Convert both dataframe values and filter values to strings for consistent comparison
                                        df[column] = df[column].astype(str).str.strip()
                                        valid_values_str = [str(v).strip() for v in valid_values]
                                        
                                        print(f"Converted filter values to strings: {valid_values_str}")
                                        
                                        # Show some sample values to compare
                                        sample_df_values = df[column].unique()[:10]
                                        print(f"Sample df values (as strings): {sample_df_values}")
                                        
                                        # Check which filter values actually exist in the data
                                        existing_values = []
                                        for fv in valid_values_str:
                                            if fv in df[column].values:
                                                existing_values.append(fv)
                                            else:
                                                print(f"WARNING: Filter value '{fv}' not found in column '{column}'")
                                        
                                        print(f"Filter values that exist in data: {existing_values}")
                                        
                                        if existing_values:
                                            # Apply the filter
                                            df = df[df[column].isin(valid_values_str)]
                                            print(f"After filtering '{column}': {len(df)} rows remaining")
                                            print(f"Unique values in '{column}' after filter:", sorted(df[column].unique()))
                                        else:
                                            print(f"ERROR: None of the filter values for '{column}' exist in the data!")
                                            print(f"Available values in '{column}': {sorted(df[column].unique())}")
                                    else:
                                        print(f"WARNING: No valid values provided for column '{column}'")
                                else:
                                    print(f"ERROR: Column '{column}' not found in dataframe")
                                    print(f"Available columns: {list(df.columns)}")
                        
                        print(f"\n=== FINAL FILTERING RESULTS ===")
                        print(f"Final filtered df rows: {len(df)}")
                        
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
                            grouped_df = df.groupby(x_axis[0])[y_axis[0]].nunique().reset_index(name="count")

                            
                            
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

                        if filter_options:
                            for col, allowed_values in filter_options.items():
                                if col in dataframe.columns:
                                    dataframe = dataframe[dataframe[col].isin(allowed_values)]

                        print("DataFrame after dashboard filtering:")
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

                                    granularity_col = f"{date_col}_{g}"

                                    # Apply correct granularity
                                    if g == "year":
                                        dataframe[granularity_col] = dataframe[date_col].dt.year.astype(str)

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
                        if isinstance(categories[0], pd.Timestamp):  # Assumes at least one value is present
                            categories = [category.strftime('%Y-%m-%d') for category in categories]
                        else:
                            categories = [str(category) for category in categories]  
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
                        axis_col = x_axis[0]  # e.g., 'brand'
                        if selectedFrequency:
                            filtered_categories = categories
                            filtered_values = values
                            print("Filtered Categories1:", filtered_categories)
                            print("Filtered Values1:", filtered_values)
                        else:
                            filtered_categories = []
                            filtered_values = []
                            for category, value in zip(categories, values):
                                print("filter_options",filter_options,axis_col)
                                if not filter_options or axis_col not in filter_options:
                                    filtered_categories = categories
                                    filtered_values = values

                                if axis_col in filter_options and category in filter_options[axis_col]:
                                    
                                    filtered_categories.append(category)
                                    filtered_values.append(value)
                            print("Filtered Categories:", filtered_categories)
                            print("Filtered Values:", filtered_values)


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
                            "filter_options": filter_options,
                            "ClickedTool": ClickedTool,
                            "Bgcolour": areacolour,
                            "table_name": table_name,
                            "OptimizationData": OptimizationData if 'OptimizationData' in locals() or 'OptimizationData' in globals() else None,
                            "opacity":final_opacity,
                            "calculationData":calculationData,
                            "chart_name": (user_id, chart_name),

                            "user_id": user_id,  
                            "xAxisTitle": xAxisTitle,
                            "yAxisTitle" :yAxisTitle       
                        })


                      
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
    


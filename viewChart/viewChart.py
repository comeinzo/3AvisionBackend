
import pandas as pd
from psycopg2 import sql
import psycopg2
from psycopg2.extras import RealDictCursor
from config import USER_NAME, DB_NAME, PASSWORD, HOST, PORT
from bar_chart import fetch_external_db_connection,convert_calculation_to_sql
from user_upload import get_db_connection
import json
import paramiko
import socket
import threading
def get_db_connection_view(database_name):
    connection = psycopg2.connect(
        dbname=database_name,  # Connect to the specified database
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    return connection

def fetch_chart_data(connection, tableName):
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
        cursor.close()

        return df

    except Exception as e:
        raise Exception(f"Error fetching data from {tableName}: {str(e)}")

def fetch_ai_saved_chart_data(connection, tableName, chart_id):
    try:
        cursor = connection.cursor()

        # Safely construct the query to fetch data from the dynamic table
        query = sql.SQL(
            "SELECT ai_chart_data FROM {table} WHERE id = %s"
        ).format(
            table=sql.Identifier(tableName)  # Safely handle dynamic table names
        )

        # Execute the query with the parameterized chart_id
        cursor.execute(query, (chart_id,))
        results = cursor.fetchall()

        # Process results: Deserialize JSON if stored as JSON
        chart_data = []
        for record in results:
            ai_chart_data = record[0]
            if isinstance(ai_chart_data, list):
                ai_chart_data = json.dumps(ai_chart_data)  # Convert list to JSON string
            chart_data.append(json.loads(ai_chart_data))

        return chart_data

    except Exception as e:
        # Use logging for better error tracking
        print("Error fetching AI chart data:", e)
        return None

    finally:
        # Ensure cursor is closed
        cursor.close()
    



# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id, calculationData):
#     try:
#         connection = get_db_connection()
#         cursor = connection.cursor()

#         # Get selectedUser
#         cursor.execute("SELECT selectedUser FROM table_chart_save WHERE id = %s", (chart_id,))
#         selectedUser = cursor.fetchone()
#         selectedUser = selectedUser[0] if selectedUser else None

#         if selectedUser:
#             external_conn = fetch_external_db_connection(database_name, selectedUser)
#             host, user, password, dbname = external_conn[3], external_conn[4], external_conn[5], external_conn[7]
#             connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
#         else:
#             connection = get_db_connection_view(database_name)

#         cursor = connection.cursor(cursor_factory=RealDictCursor)
        
#         global global_df
#         cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Limit 0 to get schema without fetching data
#         colnames = [desc[0] for desc in cursor.description]
#         global_df = pd.DataFrame(columns=colnames)
#         print(f"Schema columns loaded: {global_df.columns.tolist()}")
        
#         # Handle X-Axis calculation
#         # matched_calc_x = next((calc for calc in calculationData if calc.get("columnName").lower() == x_axis.lower() and calc.get("calculation")), None)
#         matched_calc_x = None
#         if calculationData:
#             matched_calc_x = next((calc for calc in calculationData if calc.get("columnName").lower() == x_axis.lower() and calc.get("calculation")), None)
                
#         if matched_calc_x:
#             formula_sql_x = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
#             X_Axis = f"({formula_sql_x}) AS {x_axis}_calculated"
#             X_Axis_group = f"{x_axis}_calculated"
#         else:
#             X_Axis = x_axis
#             X_Axis_group = x_axis

#         # Handle Y-Axis with proper aggregation logic
#         Y_Axis = y_axis.split(", ")
#         aliases = ['series1', 'series2', 'series3']  # extend if needed
#         y_axis_aggregate_parts = []

#         for idx, y_col in enumerate(Y_Axis):
#             # matched_calc_y = next((calc for calc in calculationData if calc.get("columnName").lower() == y_col.lower() and calc.get("calculation")), None)
#             matched_calc_y = None
#             if calculationData:
#                 matched_calc_y = next((calc for calc in calculationData if calc.get("columnName").lower() == y_col.lower() and calc.get("calculation")), None)
        
#             if matched_calc_y:
#                 formula_sql_y = convert_calculation_to_sql(matched_calc_y["calculation"].strip())
#                 # Check if aggregation requires numeric casting
#                 if aggregate.upper() in ['COUNT']:
#                     y_expr = f"{aggregate}({formula_sql_y}) AS {aliases[idx]}"
#                 else:
#                     y_expr = f"{aggregate}(({formula_sql_y})::numeric) AS {aliases[idx]}"
#             else:
#                 # Check if aggregation requires numeric casting
#                 if aggregate.upper() in ['COUNT']:
#                     y_expr = f"{aggregate}({y_col}) AS {aliases[idx]}"
#                 else:
#                     y_expr = f"{aggregate}({y_col}::numeric) AS {aliases[idx]}"
            
#             y_axis_aggregate_parts.append(y_expr)

#         y_axis_aggregate = ", ".join(y_axis_aggregate_parts)
        
#         if category:
#             # Handle category filtering
#             if matched_calc_x and clicked_category_Xaxis.lower() == x_axis.lower():
#                 formula_sql_click = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
#                 where_expr = f"({formula_sql_click}) = %s"
#             else:
#                 where_expr = f'"{clicked_category_Xaxis}" = %s'

#             query = f"""
#                 SELECT {X_Axis}, {y_axis_aggregate}
#                 FROM "{table_name}"
#                 WHERE {where_expr}
#                 GROUP BY {X_Axis_group};
#             """
#             cursor.execute(query, (category,))
#             print("query", query)

#         else:
#             # Handle general filtering
#             connection = get_db_connection()
#             cursor = connection.cursor(cursor_factory=RealDictCursor)
#             cursor.execute("SELECT filter_options FROM table_chart_save WHERE id = %s", (chart_id,))
#             filterdata_result = cursor.fetchone()
#             filter_options = filterdata_result.get('filter_options') if filterdata_result else None

#             where_clauses = []
#             if filter_options:
#                 filter_dict = json.loads(filter_options)
#                 for key, values in filter_dict.items():
#                     if isinstance(values, list) and values:
#                         formatted_values = "', '".join(values)
#                         where_clauses.append(f'"{key}" IN (\'{formatted_values}\')')

#             where_clause_str = " AND ".join(where_clauses) if where_clauses else "1=1"
#             query = f"""
#                 SELECT {X_Axis}, {y_axis_aggregate}
#                 FROM "{table_name}"
#                 WHERE {where_clause_str}
#                 GROUP BY {X_Axis_group};
#             """
#             print("query2", query)

#             con = get_db_connection_view(database_name)
#             cursor = con.cursor(cursor_factory=RealDictCursor)
#             cursor.execute(query)

#         result = cursor.fetchall()
#         categories = [row.get(X_Axis_group) for row in result]

#         if len(Y_Axis) == 1:
#             return {"categories": categories, "values": [row['series1'] for row in result]}
        
#         output = {"categories": categories}
#         for idx in range(len(Y_Axis)):
#             output[f"series{idx+1}"] = [row[aliases[idx]] for row in result]

#         return output

#     except Exception as e:
#         print("Error fetching chart data:", e)
#         return None

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id, calculationData):
#     try:
#         connection = get_db_connection()
#         cursor = connection.cursor()

#         # Get selectedUser
#         cursor.execute("SELECT selectedUser FROM table_chart_save WHERE id = %s", (chart_id,))
#         selectedUser = cursor.fetchone()
#         selectedUser = selectedUser[0] if selectedUser else None

#         if selectedUser:
#             external_conn = fetch_external_db_connection(database_name, selectedUser)
#             host, user, password, dbname = external_conn[3], external_conn[4], external_conn[5], external_conn[7]
#             connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
#         else:
#             connection = get_db_connection_view(database_name)

#         cursor = connection.cursor(cursor_factory=RealDictCursor)

#         # Get schema (for calculationData if needed)
#         cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 0')
#         colnames = [desc[0] for desc in cursor.description]
#         global_df = pd.DataFrame(columns=colnames)
#         print(f"Schema columns loaded: {global_df.columns.tolist()}")

#         # Split X-axis into list
#         X_Axis_list = x_axis.split(", ") if isinstance(x_axis, str) else x_axis
#         Y_Axis = y_axis.split(", ") if isinstance(y_axis, str) else y_axis
#         aliases = ['series1', 'series2', 'series3']  # extend if needed

#         # ==============================
#         # CASE 1: Two X-axes (special handling)
#         # ==============================
#         if len(X_Axis_list) == 2 and category:
#             parent_x, child_x = X_Axis_list[0], X_Axis_list[1]

#             query = f"""
#                 SELECT {child_x} AS child, {aggregate}({Y_Axis[0]}::numeric) AS series2
#                 FROM "{table_name}"
#                 WHERE {parent_x} = %s
#                 GROUP BY {child_x};
#             """
#             print("query (2 x_axis):", query)
#             cursor.execute(query, (category,))
#             result = cursor.fetchall()

#             response = {
#                 "categories": [category],  # parent (clicked category)
#                 "series1": [row["child"] for row in result],  # child values
#                 "series2": [row["series2"] for row in result]  # aggregated values
#             }
#             return response

#         # ==============================
#         # CASE 2: Single X-axis (default)
#         # ==============================
#         else:
#             # Handle Y-axis aggregation
#             y_axis_aggregate_parts = []
#             for idx, y_col in enumerate(Y_Axis):
#                 if aggregate.upper() == "COUNT":
#                     y_expr = f"{aggregate}({y_col}) AS {aliases[idx]}"
#                 else:
#                     y_expr = f"{aggregate}({y_col}::numeric) AS {aliases[idx]}"
#                 y_axis_aggregate_parts.append(y_expr)

#             y_axis_aggregate = ", ".join(y_axis_aggregate_parts)

#             if category:
#                 where_expr = f'"{clicked_category_Xaxis}" = %s'
#                 # query = f"""
#                 #     SELECT {x_axis}, {y_axis_aggregate}
#                 #     FROM "{table_name}"
#                 #     WHERE {where_expr}
#                 #     GROUP BY {x_axis};
#                 query = f"""
#                 SELECT {x_axis}, {y_axis_aggregate}
#                 FROM "{table_name}"
#                 WHERE {where_expr}
#                 GROUP BY {X_Axis_group};
#                 """
#                 cursor.execute(query, (category,))
#                 print("query (1 x_axis with filter):", query)

#             else:
#                 # Apply filter_options if present
#                 connection = get_db_connection()
#                 cursor = connection.cursor(cursor_factory=RealDictCursor)
#                 cursor.execute("SELECT filter_options FROM table_chart_save WHERE id = %s", (chart_id,))
#                 filterdata_result = cursor.fetchone()
#                 filter_options = filterdata_result.get('filter_options') if filterdata_result else None

#                 where_clauses = []
#                 if filter_options:
#                     filter_dict = json.loads(filter_options)
#                     for key, values in filter_dict.items():
#                         if isinstance(values, list) and values:
#                             formatted_values = "', '".join(values)
#                             where_clauses.append(f'"{key}" IN (\'{formatted_values}\')')

#                 where_clause_str = " AND ".join(where_clauses) if where_clauses else "1=1"
#                 query = f"""
#                     SELECT {x_axis}, {y_axis_aggregate}
#                     FROM "{table_name}"
#                     WHERE {where_clause_str}
#                     GROUP BY {x_axis};
#                 """
#                 print("query (1 x_axis no filter):", query)
#                 cursor.execute(query)

#             result = cursor.fetchall()
#             categories = [row.get(x_axis) for row in result]

#             if len(Y_Axis) == 1:
#                 return {"categories": categories, "values": [row['series1'] for row in result]}

#             output = {"categories": categories}
#             for idx in range(len(Y_Axis)):
#                 output[f"series{idx+1}"] = [row[aliases[idx]] for row in result]

#             return output

#     except Exception as e:
#         print("Error fetching chart data:", e)
#         return None

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id, calculationData):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Get selectedUser
        cursor.execute("SELECT selectedUser FROM table_chart_save WHERE id = %s", (chart_id,))
        selectedUser = cursor.fetchone()
        selectedUser = selectedUser[0] if selectedUser else None

        # if selectedUser:
        #     external_conn = fetch_external_db_connection(database_name, selectedUser)
        #     host, user, password, dbname = external_conn[3], external_conn[4], external_conn[5], external_conn[7]
        #     connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        # else:
        #     connection = get_db_connection_view(database_name)
        if not selectedUser or selectedUser.lower() == 'null':
            print("🟢 Using default local database connection...")
            connection = get_db_connection_view(database_name)

        else:
            print(f"🟡 Using external database connection for user: {selectedUser}")
            connection_details = fetch_external_db_connection(database_name, selectedUser)

            if not connection_details:
                raise Exception(f"❌ Unable to fetch external database connection details for user '{selectedUser}'")

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

            ssh_client = None
            local_sock = None
            stop_event = threading.Event()
            tunnel_thread = None

            # ✅ If SSH is required, establish tunnel
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


        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        global global_df
        cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 0')  # schema only
        colnames = [desc[0] for desc in cursor.description]
        global_df = pd.DataFrame(columns=colnames)
        print(f"Schema columns loaded: {global_df.columns.tolist()}")

        # ===============================
        # Handle multiple X-Axis support
        # ===============================
        X_Axis_list = x_axis.split(", ") if isinstance(x_axis, str) else x_axis
        Y_Axis = y_axis.split(", ") if isinstance(y_axis, str) else y_axis
        aliases = ['series1', 'series2', 'series3']

        # CASE: Two X-Axis (drilldown mode)
        if len(X_Axis_list) == 2 and category:
            parent_x, child_x = X_Axis_list[0], X_Axis_list[1]

            query = f"""
                SELECT {child_x} AS child, {aggregate}({Y_Axis[0]}::numeric) AS series2
                FROM "{table_name}"
                WHERE {parent_x} = %s
                GROUP BY {child_x};
            """
            print("query (2 x_axis):", query)
            cursor.execute(query, (category,))
            result = cursor.fetchall()

            return {
                "categories": [category],                 # parent category
                "series1": [row["child"] for row in result],   # child categories
                "series2": [row["series2"] for row in result]  # aggregated values
            }

        # ===============================
        # CASE: Single X-Axis (default)
        # ===============================
        # Handle calculation if provided
        matched_calc_x = None
        if calculationData:
            matched_calc_x = next((calc for calc in calculationData if calc.get("columnName").lower() == x_axis.lower() and calc.get("calculation")), None)

        if matched_calc_x:
            formula_sql_x = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
            X_Axis = f"({formula_sql_x}) AS {x_axis}_calculated"
            X_Axis_group = f"{x_axis}_calculated"
        else:
            X_Axis = x_axis
            X_Axis_group = x_axis

        # Build Y aggregation
        y_axis_aggregate_parts = []
        for idx, y_col in enumerate(Y_Axis):
            if aggregate.upper() == 'COUNT':
                y_expr = f"{aggregate}({y_col}) AS {aliases[idx]}"
            else:
                y_expr = f"{aggregate}({y_col}::numeric) AS {aliases[idx]}"
            y_axis_aggregate_parts.append(y_expr)

        y_axis_aggregate = ", ".join(y_axis_aggregate_parts)

        if category:
            if matched_calc_x and clicked_category_Xaxis.lower() == x_axis.lower():
                formula_sql_click = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
                where_expr = f"({formula_sql_click}) = %s"
            else:
                where_expr = f'"{clicked_category_Xaxis}" = %s'

            query = f"""
                SELECT {X_Axis}, {y_axis_aggregate}
                FROM "{table_name}"
                WHERE {where_expr}
                GROUP BY {X_Axis_group};
            """
            cursor.execute(query, (category,))
            print("query (1 x_axis filter):", query)

        else:
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT filter_options FROM table_chart_save WHERE id = %s", (chart_id,))
            filterdata_result = cursor.fetchone()
            filter_options = filterdata_result.get('filter_options') if filterdata_result else None

            where_clauses = []
            if filter_options:
                filter_dict = json.loads(filter_options)
                for key, values in filter_dict.items():
                    if isinstance(values, list) and values:
                        formatted_values = "', '".join(values)
                        where_clauses.append(f'"{key}" IN (\'{formatted_values}\')')

            where_clause_str = " AND ".join(where_clauses) if where_clauses else "1=1"
            query = f"""
                SELECT {X_Axis}, {y_axis_aggregate}
                FROM "{table_name}"
                WHERE {where_clause_str}
                GROUP BY {X_Axis_group};
            """
            print("query (1 x_axis no filter):", query)
            cursor.execute(query)

        result = cursor.fetchall()
        categories = [row.get(X_Axis_group) for row in result]

        if len(Y_Axis) == 1:
            return {"categories": categories, "values": [row['series1'] for row in result]}
        
        output = {"categories": categories}
        for idx in range(len(Y_Axis)):
            output[f"series{idx+1}"] = [row[aliases[idx]] for row in result]

        return output

    except Exception as e:
        print("Error fetching chart data:", e)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# from psycopg2 import sql

def fetch_ai_saved_chart_data(connection, tableName, chart_id):
    print("Fetching AI chart data...")
    print("tableName",tableName)
    print("chart_id",chart_id)
    try:
        cursor = connection.cursor()

        # Safely construct the query to fetch data from the dynamic table
        query = sql.SQL(
            "SELECT ai_chart_data FROM {table} WHERE id = %s"
        ).format(
            table=sql.Identifier(tableName)  # Safely handle dynamic table names
        )

        # Execute the query with the parameterized chart_id
        cursor.execute(query, (chart_id,))
        results = cursor.fetchall()

        # Process results: Deserialize JSON if stored as JSON
        chart_data = []
        for record in results:
            ai_chart_data = record[0]
            if isinstance(ai_chart_data, list):
                ai_chart_data = json.dumps(ai_chart_data)  # Convert list to JSON string
            chart_data.append(json.loads(ai_chart_data))

        return chart_data

    except Exception as e:
        # Use logging for better error tracking
        print("Error fetching AI chart data:", e)
        return None

    finally:
        # Ensure cursor is closed
        cursor.close()
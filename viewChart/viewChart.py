
import pandas as pd
from psycopg2 import sql
import psycopg2
from psycopg2.extras import RealDictCursor
from config import USER_NAME, DB_NAME, PASSWORD, HOST, PORT
from bar_chart import fetch_external_db_connection,convert_calculation_to_sql
from user_upload import get_db_connection
import json

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
    



# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id, calculationData,chart_type):
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



# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id, calculationData, chart_type):
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
#         cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
#         colnames = [desc[0] for desc in cursor.description]
#         global_df = pd.DataFrame(columns=colnames)
#         print(f"Schema columns loaded: {global_df.columns.tolist()}")
        
#         # **NEW: Handle singleValueChart early**
#         if chart_type == "singleValueChart":
#             # Get filter options
#             connection_filter = get_db_connection()
#             cursor_filter = connection_filter.cursor(cursor_factory=RealDictCursor)
#             cursor_filter.execute("SELECT filter_options FROM table_chart_save WHERE id = %s", (chart_id,))
#             filterdata_result = cursor_filter.fetchone()
#             filter_options = filterdata_result.get('filter_options') if filterdata_result else None
#             cursor_filter.close()
#             connection_filter.close()

#             where_clauses = []
#             if filter_options:
#                 filter_dict = json.loads(filter_options)
#                 for key, values in filter_dict.items():
#                     if isinstance(values, list) and values:
#                         formatted_values = "', '".join(values)
#                         where_clauses.append(f'"{key}" IN (\'{formatted_values}\')')

#             where_clause_str = " AND ".join(where_clauses) if where_clauses else "1=1"
            
#             # Handle X-Axis calculation for counting
#             matched_calc_x = None
#             if calculationData:
#                 matched_calc_x = next((calc for calc in calculationData if calc.get("columnName").lower() == x_axis.lower() and calc.get("calculation")), None)
                    
#             if matched_calc_x:
#                 formula_sql_x = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
#                 X_Axis_group = f"({formula_sql_x})"
#             else:
#                 X_Axis_group = x_axis
            
#             # Count total distinct x_axis values
#             count_query = f"""
#                 SELECT COUNT(DISTINCT {X_Axis_group}) as total_count
#                 FROM "{table_name}"
#                 WHERE {where_clause_str};
#             """
#             print("singleValueChart count_query:", count_query)
            
#             con = get_db_connection_view(database_name)
#             cursor = con.cursor(cursor_factory=RealDictCursor)
#             cursor.execute(count_query)
#             count_result = cursor.fetchone()
#             total_x_axis = count_result['total_count'] if count_result else 0
            
#             print(f"singleValueChart result: total_x_axis = {total_x_axis}")
            
#             # Return simple output
#             output = {"total_x_axis": total_x_axis}
            
#             cursor.close()
#             con.close()
#             return {"values": output}
        
#         # **EXISTING CODE: Continue with other chart types**
#         # Handle X-Axis calculation
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
#         aliases = ['series1', 'series2', 'series3']
#         y_axis_aggregate_parts = []

#         for idx, y_col in enumerate(Y_Axis):
#             matched_calc_y = None
#             if calculationData:
#                 matched_calc_y = next((calc for calc in calculationData if calc.get("columnName").lower() == y_col.lower() and calc.get("calculation")), None)
        
#             if matched_calc_y:
#                 formula_sql_y = convert_calculation_to_sql(matched_calc_y["calculation"].strip())
#                 if aggregate.upper() in ['COUNT']:
#                     y_expr = f"{aggregate}({formula_sql_y}) AS {aliases[idx]}"
#                 else:
#                     y_expr = f"{aggregate}(({formula_sql_y})::numeric) AS {aliases[idx]}"
#             else:
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
#         import traceback
#         traceback.print_exc()
#         return None

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()


def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id, calculationData, chart_type):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Get selectedUser
        cursor.execute("SELECT selectedUser FROM table_chart_save WHERE id = %s", (chart_id,))
        selectedUser = cursor.fetchone()
        selectedUser = selectedUser[0] if selectedUser else None

        if selectedUser:
            external_conn = fetch_external_db_connection(database_name, selectedUser)
            host, user, password, dbname = external_conn[3], external_conn[4], external_conn[5], external_conn[7]
            connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        else:
            connection = get_db_connection_view(database_name)

        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        global global_df
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        colnames = [desc[0] for desc in cursor.description]
        global_df = pd.DataFrame(columns=colnames)
        print(f"Schema columns loaded: {global_df.columns.tolist()}")
        
        # **NEW: Handle singleValueChart early**
        if chart_type == "singleValueChart":
            # Handle X-Axis calculation for counting
            matched_calc_x = None
            if calculationData:
                matched_calc_x = next((calc for calc in calculationData if calc.get("columnName").lower() == x_axis.lower() and calc.get("calculation")), None)
                    
            if matched_calc_x:
                formula_sql_x = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
                X_Axis_group = f"({formula_sql_x})"
            else:
                X_Axis_group = x_axis
            
            # Determine WHERE clause based on category drill-down
            if category:
                # Use clicked category filter
                if matched_calc_x and clicked_category_Xaxis.lower() == x_axis.lower():
                    formula_sql_click = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
                    where_clause_str = f"({formula_sql_click}) = %s"
                    query_params = (category,)
                else:
                    where_clause_str = f'"{clicked_category_Xaxis}" = %s'
                    query_params = (category,)
            else:
                # Use general filters from table_chart_save
                connection_filter = get_db_connection()
                cursor_filter = connection_filter.cursor(cursor_factory=RealDictCursor)
                cursor_filter.execute("SELECT filter_options FROM table_chart_save WHERE id = %s", (chart_id,))
                filterdata_result = cursor_filter.fetchone()
                filter_options = filterdata_result.get('filter_options') if filterdata_result else None
                cursor_filter.close()
                connection_filter.close()

                where_clauses = []
                if filter_options:
                    filter_dict = json.loads(filter_options)
                    for key, values in filter_dict.items():
                        if isinstance(values, list) and values:
                            formatted_values = "', '".join(values)
                            where_clauses.append(f'"{key}" IN (\'{formatted_values}\')')

                where_clause_str = " AND ".join(where_clauses) if where_clauses else "1=1"
                query_params = None
            
            # Count total distinct x_axis values
            count_query = f"""
                SELECT COUNT(DISTINCT {X_Axis_group}) as total_count
                FROM "{table_name}"
                WHERE {where_clause_str};
            """
            print("singleValueChart count_query:", count_query)
            if query_params:
                print("singleValueChart query_params:", query_params)
            
            con = get_db_connection_view(database_name)
            cursor = con.cursor(cursor_factory=RealDictCursor)
            
            if query_params:
                cursor.execute(count_query, query_params)
            else:
                cursor.execute(count_query)
                
            count_result = cursor.fetchone()
            total_x_axis = count_result['total_count'] if count_result else 0
            
            print(f"singleValueChart result: total_x_axis = {total_x_axis}")
            
            # Return simple output
            output = {"total_x_axis": total_x_axis}
            
            cursor.close()
            con.close()
            return {"values": output}
        
        # **EXISTING CODE: Continue with other chart types**
        # Handle X-Axis calculation
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

        # Handle Y-Axis with proper aggregation logic
        Y_Axis = y_axis.split(", ")
        aliases = ['series1', 'series2', 'series3']
        y_axis_aggregate_parts = []

        for idx, y_col in enumerate(Y_Axis):
            matched_calc_y = None
            if calculationData:
                matched_calc_y = next((calc for calc in calculationData if calc.get("columnName").lower() == y_col.lower() and calc.get("calculation")), None)
        
            if matched_calc_y:
                formula_sql_y = convert_calculation_to_sql(matched_calc_y["calculation"].strip())
                if aggregate.upper() in ['COUNT']:
                    y_expr = f"{aggregate}({formula_sql_y}) AS {aliases[idx]}"
                else:
                    y_expr = f"{aggregate}(({formula_sql_y})::numeric) AS {aliases[idx]}"
            else:
                if aggregate.upper() in ['COUNT']:
                    y_expr = f"{aggregate}({y_col}) AS {aliases[idx]}"
                else:
                    y_expr = f"{aggregate}({y_col}::numeric) AS {aliases[idx]}"
            
            y_axis_aggregate_parts.append(y_expr)

        y_axis_aggregate = ", ".join(y_axis_aggregate_parts)
        
        if category:
            # Handle category filtering
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
            print("query", query)

        else:
            # Handle general filtering
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
            print("query2", query)

            con = get_db_connection_view(database_name)
            cursor = con.cursor(cursor_factory=RealDictCursor)
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
        import traceback
        traceback.print_exc()
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
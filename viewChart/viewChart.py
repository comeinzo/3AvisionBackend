
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
# def fetch_chart_data(connection, tableName, limit=None):
#     try:
#         cursor = connection.cursor()

#         # Build query with optional limit
#         if limit is not None:
#             query = sql.SQL("SELECT * FROM {table} LIMIT %s").format(
#                 table=sql.Identifier(tableName)
#             )
#             cursor.execute(query, (limit,))
#         else:
#             query = sql.SQL("SELECT * FROM {table}").format(
#                 table=sql.Identifier(tableName)
#             )
#             cursor.execute(query)

#         results = cursor.fetchall()
#         column_names = [desc[0] for desc in cursor.description]
#         df = pd.DataFrame(results, columns=column_names)
#         cursor.close()

#         return df

#     except Exception as e:
#         raise Exception(f"Error fetching data from {tableName}: {str(e)}")
        
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
    

# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate,clicked_catagory_Xaxis,category):
#     try:
#         # Connect to the PostgreSQL databases
#         connection = psycopg2.connect(
#             dbname=database_name,
#             user='postgres',
#             password='Gayu@123',
#             host='localhost',
#             port='5432'
#         )
#         cursor = connection.cursor(cursor_factory=RealDictCursor)

#         X_Axis= x_axis
#         Y_Axis= y_axis

#         # Construct the SQL query
#         query = f"""
#         SELECT {X_Axis}, {aggregate}({Y_Axis}::numeric) AS {Y_Axis}
#         FROM {table_name}
#         WHERE {clicked_catagory_Xaxis} = '{category}'
#         GROUP BY {X_Axis};
#         """
#         print("Query:", query)
#         cursor.execute(query)
#         result = cursor.fetchall()

#         categories = [row[x_axis] for row in result]
#         values = [row[y_axis] for row in result]

#         return {"categories": categories, "values": values}

#     except Exception as e:
#         print("Error fetching chart data:", e)
#         return None

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()


# ====================================   ABOVE  IS THE REAL CODE ================================



# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category,chart_id):
#     try:
      
#         connection =get_db_connection()
        
#         # Fetch selectedUser from the database based on chart_id
#         query = f"SELECT selectedUser FROM table_chart_save WHERE id = %s"
#         cursor = connection.cursor()
#         cursor.execute(query, (chart_id,))
#         selectedUser = cursor.fetchone()
#         print("selectedUser",selectedUser)
#         print("database_name====================",database_name)
#         # connection = get_db_connection_view(databaseName)
#         # df = fetch_chart_data(connection, tableName)
#         if selectedUser is None:
#             print("No selectedUser found for this chart_id.")
#             connection = get_db_connection_view(database_name)
#         else:
#             selectedUser = selectedUser[0]  # Extract the actual value from the tuple
#             # print("Fetched selectedUser:", selectedUser)


        
#             if selectedUser:
#                 connection = fetch_external_db_connection(database_name, selectedUser)
#                 host = connection[3]
#                 dbname = connection[7]
#                 user = connection[4]
#                 password = connection[5]

#                 # Create a new psycopg2 connection using the details from the tuple
#                 connection = psycopg2.connect(
#                     dbname=dbname,
#                     user=user,
#                     password=password,
#                     host=host
#                 )
#                 print('External Connection established:', connection)
#             else:
#                 print("No valid selectedUser found.")
#                 connection = get_db_connection_view(database_name)
#         cursor = connection.cursor(cursor_factory=RealDictCursor)

#         X_Axis = x_axis
#         Y_Axis = y_axis.split(", ")  # Split the Y_Axis if it contains multiple columns
#         print("---------------------------------------------", Y_Axis)

#         # Define aliases for each Y_Axis column
#         aliases = ['series1', 'series2']  # Extend this list if there are more columns
#         if len(Y_Axis) > 1 and len(Y_Axis) == len(aliases):
#             # Construct the SQL query with custom aliases (series1, series2)
#             y_axis_aggregate = ", ".join([f"{aggregate}({col}::numeric) AS {alias}" for col, alias in zip(Y_Axis, aliases)])
#         else:
#             y_axis_aggregate = f"{aggregate}({Y_Axis[0]}::numeric) AS series1"

       
#         if category is not None:
#             query = f"""
#             SELECT {X_Axis}, {y_axis_aggregate}
#             FROM "{table_name}"
#             WHERE {clicked_category_Xaxis} = '{category}'
#             GROUP BY {X_Axis};
#             """
#             cursor = connection.cursor(cursor_factory=RealDictCursor)
#             cursor.execute(query)
#             result = cursor.fetchall()
#         else:
#             connection =get_db_connection()
#             cursor = connection.cursor(cursor_factory=RealDictCursor)
#             print(connection)
#             # Fetch clicked_category_Xaxis (filterdata column name)
#             cursor.execute("SELECT filter_options FROM table_chart_save WHERE id = %s", (chart_id,))
#             filterdata_result = cursor.fetchone()
          

#             filter_options = filterdata_result.get('filter_options') if filterdata_result else None
#             where_clauses = []

#             if filter_options:
#                 filter_dict = json.loads(filter_options)
#                 # print("Parsed filter_dict:", filter_dict)

#                 for key, values in filter_dict.items():
#                     if isinstance(values, list) and values:  # Ensure non-empty list
#                         # Format as SQL-safe strings
#                         formatted_values = "', '".join(values)
#                         clause = f'"{key}" IN (\'{formatted_values}\')'
#                         where_clauses.append(clause)

#                 where_clause_str = " AND ".join(where_clauses) if where_clauses else "1=1"

#                 query = f"""
#                 SELECT {X_Axis}, {y_axis_aggregate}
#                 FROM "{table_name}"
#                 WHERE {where_clause_str}
#                 GROUP BY {X_Axis};
#                 """
#             else:
#                 # Fallback if no filter options
#                 query = f"""
#                 SELECT {X_Axis}, {y_axis_aggregate}
#                 FROM "{table_name}"
#                 GROUP BY {X_Axis};
#                 """

#             print("Query:", query)
#             con = get_db_connection_view(database_name)
#             print("con", con)
#             cursor = con.cursor(cursor_factory=RealDictCursor)
#             cursor.execute(query)
#         # cursor = connection.cursor(cursor_factory=RealDictCursor)
#         # cursor.execute(query)
#             result = cursor.fetchall()

#         # Extract categories
#         categories = [row[x_axis] for row in result]

#         # Extract values for single Y-axis (series1)
#         if len(Y_Axis) == 1:
#             values = [row['series1'] for row in result]
#             return {"categories": categories, "values": values}

#         # Extract values for multiple Y-axes (series1, series2)
#         series1 = [row['series1'] for row in result]
#         series2 = [row['series2'] for row in result]
        
#         return {"categories": categories, "series1": series1, "series2": series2}

#     except Exception as e:
#         print("Error fetching chart data:", e)
#         return None

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()
# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id,calculationData):
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
#         cursor.execute(f"SELECT * FROM {table_name} LIMIT 0") # Limit 0 to get schema without fetching data
#         colnames = [desc[0] for desc in cursor.description]
#         global_df = pd.DataFrame(columns=colnames)
#         print(f"Schema columns loaded: {global_df.columns.tolist()}")
#         matched_calc_x = next((calc for calc in calculationData if calc.get("columnName").lower() == x_axis.lower() and calc.get("calculation")), None)
#         if matched_calc_x:
#             formula_sql_x = convert_calculation_to_sql(matched_calc_x["calculation"].strip(), dataframe_columns=global_df.columns.tolist())
#             X_Axis = f"({formula_sql_x}) AS {x_axis}_calculated"
#             X_Axis_group = f"{x_axis}_calculated"
#         else:
#             X_Axis = x_axis
#             X_Axis_group = x_axis

#         # Replace Y-Axis (supporting one or more)
#         Y_Axis = y_axis.split(", ")
#         aliases = ['series1', 'series2', 'series3']  # extend if needed
#         y_axis_aggregate_parts = []

#         for idx, y_col in enumerate(Y_Axis):
#             matched_calc_y = next((calc for calc in calculationData if calc.get("columnName").lower() == y_col.lower() and calc.get("calculation")), None)
#             if matched_calc_y:
#                 formula_sql_y = convert_calculation_to_sql(matched_calc_y["calculation"].strip())
#                 y_expr = f"{aggregate}(({formula_sql_y})::numeric) AS {aliases[idx]}"
#             else:
#                 y_expr = f"{aggregate}({y_col}::numeric) AS {aliases[idx]}"
#             y_axis_aggregate_parts.append(y_expr)

#         y_axis_aggregate = ", ".join(y_axis_aggregate_parts)
#         if category:
#             # if matched_calc_x and clicked_category_Xaxis == x_axis:
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
#             print("query",query)

#         else:
#             # Filter logic
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
#             print("query2",query)

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



def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category, chart_id, calculationData):
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
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Limit 0 to get schema without fetching data
        colnames = [desc[0] for desc in cursor.description]
        global_df = pd.DataFrame(columns=colnames)
        print(f"Schema columns loaded: {global_df.columns.tolist()}")
        
        # Handle X-Axis calculation
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
        aliases = ['series1', 'series2', 'series3']  # extend if needed
        y_axis_aggregate_parts = []

        for idx, y_col in enumerate(Y_Axis):
            matched_calc_y = next((calc for calc in calculationData if calc.get("columnName").lower() == y_col.lower() and calc.get("calculation")), None)
            
            if matched_calc_y:
                formula_sql_y = convert_calculation_to_sql(matched_calc_y["calculation"].strip())
                # Check if aggregation requires numeric casting
                if aggregate.upper() in ['COUNT']:
                    y_expr = f"{aggregate}({formula_sql_y}) AS {aliases[idx]}"
                else:
                    y_expr = f"{aggregate}(({formula_sql_y})::numeric) AS {aliases[idx]}"
            else:
                # Check if aggregation requires numeric casting
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
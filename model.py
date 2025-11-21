# # # models.py
# # import psycopg2

# # class LoginHistoryTable:
# #     def __init__(self, cursor):
# #         self.cursor = cursor

# #     def create_table(self):
# #         self.cursor.execute("""
# #         CREATE TABLE IF NOT EXISTS login_history (
# #             login_id SERIAL PRIMARY KEY,
# #             employee_id INT,
# #             company_id INT,
# #             login_device VARCHAR(255),
# #             login_ip VARCHAR(50),
# #             login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# #             logout_time TIMESTAMP,  -- ✅ Added logout time
# #             login_status VARCHAR(10) CHECK (login_status IN ('login', 'logout')),
# #             FOREIGN KEY (company_id) REFERENCES organizationdatatest(id)
# #         );
# #         """)
# #         print("✅ login_history table created or already exists.")

# #     def add_missing_columns(self):
# #         columns_to_add = {
# #             "employee_id": "INT",
# #             "company_id": "INT",
# #             "login_device": "VARCHAR(255)",
# #             "login_ip": "VARCHAR(50)",
# #             "login_time": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
# #             "logout_time": "TIMESTAMP",  # ✅ ensure logout_time exists
# #             "login_status": "VARCHAR(10) DEFAULT 'login'"
# #         }

# #         for column_name, column_type in columns_to_add.items():
# #             self.cursor.execute("""
# #             SELECT column_name 
# #             FROM information_schema.columns 
# #             WHERE table_name = 'login_history' AND column_name = %s;
# #             """, (column_name,))
# #             column_exists = self.cursor.fetchone()

# #             if not column_exists:
# #                 self.cursor.execute(f"""
# #                 ALTER TABLE login_history ADD COLUMN {column_name} {column_type};
# #                 """)
# #                 print(f"✅ Added missing column: {column_name}")
    

# #     def setup(self):
# #         self.create_table()
# #         self.add_missing_columns()

# import psycopg2

# # ==============================================================
# # LOGIN HISTORY TABLE SETUP
# # ==============================================================

# class LoginHistoryTable:
#     def __init__(self, cursor):
#         self.cursor = cursor

#     def create_table(self):
#         self.cursor.execute("""
#         CREATE TABLE IF NOT EXISTS login_history (
#             login_id SERIAL PRIMARY KEY,
#             employee_id INT,
#             company_id INT,
#             login_device VARCHAR(255),
#             login_ip VARCHAR(50),
#             login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#             logout_time TIMESTAMP,
#             login_status VARCHAR(10) CHECK (login_status IN ('login', 'logout')),
#             FOREIGN KEY (company_id) REFERENCES organizationdatatest(id)
#         );
#         """)
#         print("✅ login_history table created or already exists.")

#     def add_missing_columns(self):
#         columns_to_add = {
#             "employee_id": "INT",
#             "company_id": "INT",
#             "login_device": "VARCHAR(255)",
#             "login_ip": "VARCHAR(50)",
#             "login_time": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
#             "logout_time": "TIMESTAMP",
#             "login_status": "VARCHAR(10) DEFAULT 'login'"
#         }

#         for column_name, column_type in columns_to_add.items():
#             self.cursor.execute("""
#             SELECT column_name 
#             FROM information_schema.columns 
#             WHERE table_name = 'login_history' AND column_name = %s;
#             """, (column_name,))
#             column_exists = self.cursor.fetchone()

#             if not column_exists:
#                 self.cursor.execute(f"""
#                 ALTER TABLE login_history ADD COLUMN {column_name} {column_type};
#                 """)
#                 print(f"✅ Added missing column: {column_name}")

#     def setup(self):
#         self.create_table()
#         self.add_missing_columns()
#         print("🎯 Login history setup completed successfully.")


# # ==============================================================
# # LICENSE MANAGEMENT SETUP
# # ==============================================================

# class LicenseManager:
#     def __init__(self, cursor):
#         self.cursor = cursor

#     def create_tables(self):
#         """Create all license-related tables if they do not exist."""
#         self.cursor.execute("""
#         CREATE TABLE IF NOT EXISTS license_plan (
#             id SERIAL PRIMARY KEY,
#             plan_name VARCHAR(100) UNIQUE NOT NULL,
#             description TEXT,
#             storage_limit INTEGER,
#             price DECIMAL(10,2),
#             duration_days INTEGER DEFAULT 30,
#             employee_limit INTEGER,
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#         """)

#         self.cursor.execute("""
#         CREATE TABLE IF NOT EXISTS license_features (
#             id SERIAL PRIMARY KEY,
#             plan_id INTEGER REFERENCES license_plan(id) ON DELETE CASCADE,
#             feature_name VARCHAR(100) NOT NULL,
#             is_enabled BOOLEAN DEFAULT TRUE
#         );
#         """)

#         self.cursor.execute("""
#         CREATE TABLE IF NOT EXISTS organization_license (
#             id SERIAL PRIMARY KEY,
#             company_id INTEGER REFERENCES organizationdatatest(id) ON DELETE CASCADE,
#             plan_id INTEGER REFERENCES license_plan(id) ON DELETE CASCADE,
#             start_date DATE NOT NULL,
#             end_date DATE NOT NULL,
#             is_active BOOLEAN DEFAULT TRUE
#         );
#         """)

#         self.cursor.execute("""
#         CREATE TABLE IF NOT EXISTS project_features (
#             id SERIAL PRIMARY KEY,
#             feature_name VARCHAR(100) UNIQUE NOT NULL,
#             description TEXT,
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#         """)

#         print("✅ License-related tables created or already exist.")

#     def add_missing_columns(self):
#         """Ensure all required columns exist for all license tables."""
#         tables_and_columns = {
#             "license_plan": {
#                 "plan_name": "VARCHAR(100)",
#                 "description": "TEXT",
#                 "storage_limit": "INTEGER",
#                 "price": "DECIMAL(10,2)",
#                 "duration_days": "INTEGER DEFAULT 30",
#                 "employee_limit": "INTEGER",
#                 "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
#             },
#             "license_features": {
#                 "plan_id": "INTEGER REFERENCES license_plan(id) ON DELETE CASCADE",
#                 "feature_name": "VARCHAR(100)",
#                 "is_enabled": "BOOLEAN DEFAULT TRUE"
#             },
#             "organization_license": {
#                 "company_id": "INTEGER REFERENCES organizationdatatest(id) ON DELETE CASCADE",
#                 "plan_id": "INTEGER REFERENCES license_plan(id) ON DELETE CASCADE",
#                 "start_date": "DATE",
#                 "end_date": "DATE",
#                 "is_active": "BOOLEAN DEFAULT TRUE"
#             },
#             "project_features": {
#                 "feature_name": "VARCHAR(100)",
#                 "description": "TEXT",
#                 "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
#             }
#         }

#         for table, columns in tables_and_columns.items():
#             for column, column_type in columns.items():
#                 self.cursor.execute("""
#                 SELECT column_name
#                 FROM information_schema.columns
#                 WHERE table_name = %s AND column_name = %s;
#                 """, (table, column))
#                 column_exists = self.cursor.fetchone()

#                 if not column_exists:
#                     self.cursor.execute(f"""
#                     ALTER TABLE {table} ADD COLUMN {column} {column_type};
#                     """)
#                     print(f"✅ Added missing column '{column}' in table '{table}'")

#         print("✅ Checked and added all missing columns for all license tables.")

#         for table, columns in tables_and_columns.items():
#             for column, column_type in columns.items():
#                 self.cursor.execute("""
#                 SELECT column_name
#                 FROM information_schema.columns
#                 WHERE table_name = %s AND column_name = %s;
#                 """, (table, column))
#                 column_exists = self.cursor.fetchone()

#                 if not column_exists:
#                     self.cursor.execute(f"""
#                     ALTER TABLE {table} ADD COLUMN {column} {column_type};
#                     """)
#                     print(f"✅ Added missing column: {column} in {table}")

#     def insert_default_data(self):
#         """Insert default features."""
#         default_features = [
#             ('share_dashboard', 'Allows users to share dashboards with others'),
#             ('AI_Analytics', 'Provides AI-powered analytics insights'),
#             ('custom_theme', 'Enables users to customize chart and UI themes'),
#             ('live_update', 'Allows real-time data updates in dashboards'),
#             ('Agentic_AI', 'Enables advanced Agentic AI functionalities')
#         ]

#         for feature_name, description in default_features:
#             self.cursor.execute("""
#                 INSERT INTO project_features (feature_name, description)
#                 VALUES (%s, %s)
#                 ON CONFLICT (feature_name) DO NOTHING;
#             """, (feature_name, description))

#         print("✅ Default license features inserted.")

#     def setup(self):
#         """Run all setup tasks: create tables, add missing columns, insert defaults."""
#         self.create_tables()
#         self.add_missing_columns()
#         self.insert_default_data()
#         print("🎯 License tables setup completed successfully.")






import psycopg2

# ==============================================================
# LOGIN HISTORY TABLE SETUP
# ==============================================================

class LoginHistoryTable:
    def __init__(self, cursor):
        self.cursor = cursor

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS login_history (
            login_id SERIAL PRIMARY KEY,
            employee_id INT,
            company_id INT,
            login_device VARCHAR(255),
            login_ip VARCHAR(50),
            login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            logout_time TIMESTAMP,
            login_status VARCHAR(10) CHECK (login_status IN ('login', 'logout')),
            FOREIGN KEY (company_id) REFERENCES organizationdatatest(id)
        );
        """)
        print("✅ login_history table created or already exists.")

    def add_missing_columns(self):
        columns_to_add = {
            "employee_id": "INT",
            "company_id": "INT",
            "login_device": "VARCHAR(255)",
            "login_ip": "VARCHAR(50)",
            "login_time": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "logout_time": "TIMESTAMP",
            "login_status": "VARCHAR(10) DEFAULT 'login'"
        }

        for column_name, column_type in columns_to_add.items():
            self.cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'login_history' AND column_name = %s;
            """, (column_name,))
            column_exists = self.cursor.fetchone()

            if not column_exists:
                self.cursor.execute(f"""
                ALTER TABLE login_history ADD COLUMN {column_name} {column_type};
                """)
                print(f"✅ Added missing column: {column_name}")

    def setup(self):
        self.create_table()
        self.add_missing_columns()
        print("🎯 Login history setup completed successfully.")


# ==============================================================
# LICENSE MANAGEMENT SETUP
# ==============================================================

class LicenseManager:
    def __init__(self, cursor):
        self.cursor = cursor

    def create_tables(self):
        """Create all license-related tables if they do not exist."""

        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS table_chart_save (
                id SERIAL PRIMARY KEY,
                User_id integer,
                company_name VARCHAR,
                chart_name VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                database_name VARCHAR,
                selected_table VARCHAR,
                x_axis VARCHAR[],
                y_axis VARCHAR[],
                aggregate VARCHAR,
                chart_type VARCHAR,
                chart_color VARCHAR,
                chart_heading VARCHAR,
                drilldown_chart_color VARCHAR,
                filter_options VARCHAR,
                ai_chart_data JSONB,
                selectedUser VARCHAR,
                xFontSize INTEGER,
                fontStyle VARCHAR,         
                categoryColor VARCHAR,      
                yFontSize INTEGER,          
                valueColor VARCHAR ,
                headingColor VARCHAR ,
                ClickedTool VARCHAR,
                Bgcolour VARCHAR,
                OptimizationData VARCHAR,
                calculationData JSONB,
                selectedFrequency VARCHAR,
                xAxisTitle VARCHAR(220),
                yAxisTitle VARCHAR(220),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)


        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS organizationdatatest (
            id SERIAL PRIMARY KEY,
            organizationName VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            userName VARCHAR(255) NOT NULL,
            password VARCHAR(255) NOT NULL,
            logo VARCHAR(255),
            status VARCHAR(50) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)


        self.cursor.execute("""
                                CREATE TABLE IF NOT EXISTS table_dashboard (
        id SERIAL PRIMARY KEY,
        user_id INTEGER, 
        company_name VARCHAR(255),  
        file_name VARCHAR(255), 
        chart_ids TEXT,
        position TEXT,
        chart_size TEXT,
        chart_type TEXT,
        chart_Xaxis TEXT,
        chart_Yaxis TEXT,
        chart_aggregate TEXT,
        filterdata TEXT,
        clicked_category VARCHAR(255),
        heading TEXT,
        dashboard_name VARCHAR(255),
        chartcolor TEXT,
        droppableBgColor TEXT,
        opacity TEXT,
        image_ids TEXT,
        project_name VARCHAR(255),
        font_style_state VARCHAR(255),
        font_size VARCHAR(255),
        font_color VARCHAR(255),
        wallpaper_id TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        

    );
    """)

        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS image_positions (
        id SERIAL PRIMARY KEY,
        image_id TEXT UNIQUE,
        src TEXT,
        x INTEGER,
        y INTEGER,
        width INTEGER,
        height INTEGER,
        zIndex INTEGER,
        disableDragging BOOLEAN
    );
    """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_wallpapers (
                wallpaper_id SERIAL PRIMARY KEY,
                src TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS calculation_suggestions (
                id SERIAL PRIMARY KEY,
                keyword VARCHAR(50) UNIQUE NOT NULL,
                template TEXT NOT NULL,
                category VARCHAR(50) DEFAULT 'default'
            );
        """)               

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS license_plan (
            id SERIAL PRIMARY KEY,
            plan_name VARCHAR(100) UNIQUE NOT NULL,
            description TEXT,
            storage_limit INTEGER,
            price DECIMAL(10,2),
            duration_days INTEGER DEFAULT 30,
            employee_limit INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS password_resets (
            email VARCHAR(255),
            company VARCHAR(255),
            token TEXT,
            used BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS license_features (
            id SERIAL PRIMARY KEY,
            plan_id INTEGER REFERENCES license_plan(id) ON DELETE CASCADE,
            feature_name VARCHAR(100) NOT NULL,
            is_enabled BOOLEAN DEFAULT TRUE
        );
        """)

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS organization_license (
            id SERIAL PRIMARY KEY,
            company_id INTEGER REFERENCES organizationdatatest(id) ON DELETE CASCADE,
            plan_id INTEGER REFERENCES license_plan(id) ON DELETE CASCADE,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE
        );
        """)

                            

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS project_features (
            id SERIAL PRIMARY KEY,
            feature_name VARCHAR(100) UNIQUE NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        print("✅ License-related tables created or already exist.")

    def add_missing_columns(self):
        """Ensure all required columns exist for all license tables."""
        tables_and_columns = {
            "license_plan": {
                "plan_name": "VARCHAR(100)",
                "description": "TEXT",
                "storage_limit": "INTEGER",
                "price": "DECIMAL(10,2)",
                "duration_days": "INTEGER DEFAULT 30",
                "employee_limit": "INTEGER",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "license_features": {
                "plan_id": "INTEGER REFERENCES license_plan(id) ON DELETE CASCADE",
                "feature_name": "VARCHAR(100)",
                "is_enabled": "BOOLEAN DEFAULT TRUE"
            },
            "organization_license": {
                "company_id": "INTEGER REFERENCES organizationdatatest(id) ON DELETE CASCADE",
                "plan_id": "INTEGER REFERENCES license_plan(id) ON DELETE CASCADE",
                "start_date": "DATE",
                "end_date": "DATE",
                "is_active": "BOOLEAN DEFAULT TRUE"
            },
            "project_features": {
                "feature_name": "VARCHAR(100)",
                "description": "TEXT",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "organizationdatatest": {
                "status": "VARCHAR(50) DEFAULT 'active'",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "table_chart_save": {
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "table_dashboard": {
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            }

        }

        for table, columns in tables_and_columns.items():
            for column, column_type in columns.items():
                self.cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s;
                """, (table, column))
                column_exists = self.cursor.fetchone()

                if not column_exists:
                    self.cursor.execute(f"""
                    ALTER TABLE {table} ADD COLUMN {column} {column_type};
                    """)
                    print(f"✅ Added missing column '{column}' in table '{table}'")

        print("✅ Checked and added all missing columns for all license tables.")

        for table, columns in tables_and_columns.items():
            for column, column_type in columns.items():
                self.cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s;
                """, (table, column))
                column_exists = self.cursor.fetchone()

                if not column_exists:
                    self.cursor.execute(f"""
                    ALTER TABLE {table} ADD COLUMN {column} {column_type};
                    """)
                    print(f"✅ Added missing column: {column} in {table}")

    def insert_default_data(self):
        """Insert default features."""
        default_features = [
            ('share_dashboard', 'Allows users to share dashboards with others'),
            ('AI_Analytics', 'Provides AI-powered analytics insights'),
            ('custom_theme', 'Enables users to customize chart and UI themes'),
            ('live_update', 'Allows real-time data updates in dashboards'),
            ('Agentic_AI', 'Enables advanced Agentic AI functionalities')
        ]

        for feature_name, description in default_features:
            self.cursor.execute("""
                INSERT INTO project_features (feature_name, description)
                VALUES (%s, %s)
                ON CONFLICT (feature_name) DO NOTHING;
            """, (feature_name, description))

        print("✅ Default license features inserted.")

    def insert_calculation_suggestions(self):
        """Insert default calculation suggestions."""
        # List of tuples: (keyword, template, category)
        default_suggestions = [
            ('if', 'if ([column] == "value") then "Result1" else "Result2"', 'default'),
            ('switch', 'switch([column], "A", "X", "B", "Y", default, "Z")', 'default'),
            ('iferror', 'iferror([numerator] / [denominator], 0)', 'default'),
            ('calculate', 'calculate(sum([sales]), [region]="Asia")', 'default'),
            ('sum', 'sum([column])', 'default'),
            ('avg', 'avg([column])', 'default'),
            ('count', 'count([column])', 'default'),
            ('min', 'min([column])', 'default'),
            ('max', 'max([column])', 'default'),
            ('abs', 'abs([column])', 'default'),
            ('maxx', 'maxx([column])', 'default'),
            ('minx', 'minx([column])', 'default'),
            ('len', 'len([column])', 'default'),
            ('lower', 'lower([column])', 'default'),
            ('upper', 'upper([column])', 'default'),
            ('concat', 'concat([first_name], " ", [last_name])', 'default'),
            ('round', 'round([column], 2)', 'default'),
            ('isnull', 'isnull([column], "default value")', 'default'),
            ('year', 'year([date_column])', 'date'),
            ('month', 'month([date_column])', 'date'),
            ('day', 'day([date_column])', 'date'),
            ('in', '[region] in ("Asia", "Europe")', 'default'),
            ('datediff', 'datediff([end_date], [start_date])', 'date'),
            ('today', 'today()', 'date'),
            ('now', 'now()', 'date'),
            ('dateadd', 'dateadd([date_column], 7, "day")', 'date'),
            ('formatdate', 'formatdate([date_column], "YYYY-MM-DD")', 'date'),
            ('replace', 'replace([column], "old", "new")', 'text'),
            ('trim', 'trim([column])', 'text')
        ]

        for keyword, template, category in default_suggestions:
            self.cursor.execute("""
                INSERT INTO calculation_suggestions (keyword, template, category)
                VALUES (%s, %s, %s)
                ON CONFLICT (keyword) DO NOTHING;
            """, (keyword, template, category))

        print("✅ Default calculation suggestions inserted.")

    # NOTE: This function assumes 'self.cursor' is an active database cursor object, 
    # typically from a PostgreSQL adapter like psycopg2, and that the 
    # 'calculation_suggestions' table has already been created.

    def setup(self):
        """Run all setup tasks: create tables, add missing columns, insert defaults."""
        self.create_tables()
        self.add_missing_columns()
        self.insert_default_data()
        print("🎯 License tables setup completed successfully.")

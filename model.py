# # models.py
# import psycopg2

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
#             logout_time TIMESTAMP,  -- ✅ Added logout time
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
#             "logout_time": "TIMESTAMP",  # ✅ ensure logout_time exists
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

    def setup(self):
        """Run all setup tasks: create tables, add missing columns, insert defaults."""
        self.create_tables()
        self.add_missing_columns()
        self.insert_default_data()
        print("🎯 License tables setup completed successfully.")

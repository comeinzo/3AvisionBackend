# models.py
import psycopg2

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
            logout_time TIMESTAMP,  -- ✅ Added logout time
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
            "logout_time": "TIMESTAMP",  # ✅ ensure logout_time exists
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

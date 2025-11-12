# schema.py
import psycopg2
from model import LoginHistoryTable
from config import PASSWORD, USER_NAME, HOST, PORT,DB_NAME
def create_schema():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER_NAME,
        password=PASSWORD
    )
    cur = conn.cursor()

    login_history = LoginHistoryTable(cur)
    login_history.setup()

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Schema setup completed.")

if __name__ == "__main__":
    create_schema()

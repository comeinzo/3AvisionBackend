# # schema.py
# import psycopg2
# from model import LoginHistoryTable,LicenseManager
# from config import PASSWORD, USER_NAME, HOST, PORT,DB_NAME
# def create_schema():
#     conn = psycopg2.connect(
#         host=HOST,
#         port=PORT,
#         database=DB_NAME,
#         user=USER_NAME,
#         password=PASSWORD
#     )
#     cur = conn.cursor()

#     login_history = LoginHistoryTable(cur)
#     license_manager = LicenseManager(cur)
#     login_history.setup()
#     license_manager.setup()

#     conn.commit()
#     cur.close()
#     conn.close()
#     print("✅ Schema setup completed.")

# if __name__ == "__main__":
#     create_schema()

import psycopg2
# Ensure the import matches your actual filename (models.py vs model.py)
from model import LoginHistoryTable, LicenseManager 
from config import PASSWORD, USER_NAME, HOST, PORT, DB_NAME

def create_schema():
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DB_NAME,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()

        # Instantiate classes
        login_history = LoginHistoryTable(cur)
        license_manager = LicenseManager(cur)

        # =========================================================
        # ✅ FIX: Run LicenseManager FIRST
        # This creates 'organizationdatatest' which is needed by 'login_history'
        # =========================================================
        print("⏳ Setting up License Manager tables...")
        license_manager.setup()

        print("⏳ Setting up Login History tables...")
        login_history.setup()

        # Commit the transaction to save changes
        conn.commit()
        print("✅ Schema setup completed successfully.")

    except Exception as e:
        # Rollback if there is an error so you don't end up with a half-broken DB
        if conn:
            conn.rollback()
        print(f"❌ Error creating schema: {e}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
if __name__ == "__main__":
    create_schema()
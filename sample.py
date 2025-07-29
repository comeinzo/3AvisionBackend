

import psycopg2
from faker import Faker
from tqdm import tqdm
from config import ALLOWED_EXTENSIONS, DB_NAME, USER_NAME, PASSWORD, HOST, PORT

fake = Faker()

def connect():
    return psycopg2.connect(
        host=HOST,
        dbname=DB_NAME,
        user=USER_NAME,
        password=PASSWORD,
        port=PORT
    )

def generate_row():
    return [fake.word() for _ in range(50)]

def insert_data(batch_size=1000, total_rows=10_000_000):
    conn = connect()
    cur = conn.cursor()
    
    sql = "INSERT INTO big_table ({}) VALUES ({})".format(
        ', '.join([f'col{i}' for i in range(1, 51)]),
        ', '.join(['%s'] * 50)
    )

    print("Inserting rows...")
    for _ in tqdm(range(0, total_rows, batch_size)):
        batch = [generate_row() for _ in range(batch_size)]
        cur.executemany(sql, batch)
        conn.commit()

    cur.close()
    conn.close()
    print("✅ Insertion complete!")

insert_data()

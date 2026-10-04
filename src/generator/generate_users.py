import random
import time
import psycopg2
from faker import Faker
import os

fake = Faker()

conn = psycopg2.connect(
    host=os.getenv('CDC_POSTGRES_HOST'),
    database=os.getenv('CDC_POSTGRES_DB'),
    user=os.getenv('CDC_POSTGRES_USER'),
    password=os.getenv('CDC_POSTGRES_PASSWORD')
)



cursor = conn.cursor()

while True:

    operation = random.choices(
        ["insert", "update", "delete"],
        weights=[60, 30, 10]
    )[0]

    if operation == "insert":

        cursor.execute(
            """
            INSERT INTO users(name,email)
            VALUES (%s,%s)
            """,
            (fake.name(), fake.email())
        )

    elif operation == "update":

        cursor.execute("""
            SELECT id
            FROM users
            ORDER BY RANDOM()
            LIMIT 1
        """)

        row = cursor.fetchone()

        if row:
            cursor.execute(
                """
                UPDATE users
                SET name=%s
                WHERE id=%s
                """,
                (fake.name(), row[0])
            )

    else:

        cursor.execute("""
            SELECT id
            FROM users
            ORDER BY RANDOM()
            LIMIT 1
        """)

        row = cursor.fetchone()

        if row:
            cursor.execute(
                "DELETE FROM users WHERE id=%s",
                (row[0],)
            )

    conn.commit()

    print(f"Executed: {operation}")

    time.sleep(10)
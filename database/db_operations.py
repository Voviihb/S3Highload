import psycopg2
from .db_config import db_config


def insert_file_info(name, size, hash_value, status):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        insert_query = """INSERT INTO files_info (name, size, hash, status) VALUES (%s, %s, %s, %s)"""
        cursor.execute(insert_query, (name, size, hash_value, status))

        connection.commit()
        print(f"Inserted file info: {name}, {size}, {status}")

    except Exception as error:
        print("Failed to insert data into PostgreSQL table", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

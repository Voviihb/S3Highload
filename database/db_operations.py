import psycopg2
from .db_config import db_config


def insert_file_info(data):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        for element in data:
            name, size, hash_value, status = element
            insert_query = """
                    INSERT INTO objects (name, size, hash, status)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name) DO NOTHING;
                    """
            cursor.execute(insert_query, (name, size, hash_value, status))

        connection.commit()
        print("Successfully inserted data into database")

    except Exception as error:
        print("Failed to insert data into PostgreSQL table", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

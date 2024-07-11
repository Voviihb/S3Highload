import json

import psycopg2
from confluent_kafka import Producer

from database.db_config import db_config


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def fetch_pending_objects():
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        fetch_query = "SELECT id, name, size FROM Objects WHERE status = 'waiting'"
        cursor.execute(fetch_query)
        objects = cursor.fetchall()

        cursor.close()
        connection.close()
        return objects

    except Exception as error:
        print("Error fetching data from PostgreSQL table", error)
        return []

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092'
    }

    producer = Producer(conf)
    topic = 'topic-demo'

    objects = fetch_pending_objects()
    if not objects:
        print("No pending objects to process")
        return

    for obj in objects:
        obj_id, name, size = obj
        message = json.dumps({
            'id': obj_id,
            'name': name,
            'size': size
        })
        producer.produce(topic, key=str(obj_id), value=message, callback=delivery_report)
        producer.poll(1)

    producer.flush()

if __name__ == '__main__':
    main()

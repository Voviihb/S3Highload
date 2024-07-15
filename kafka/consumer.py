import json
import random
import threading
import time

import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError

from database.db_config import db_config


def update_status(obj_id, new_status):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        update_query = "UPDATE Objects SET status = %s WHERE id = %s"
        cursor.execute(update_query, (new_status, obj_id))

        connection.commit()
        cursor.close()
        connection.close()
        print(f"Updated object {obj_id} status to {new_status}")

    except Exception as error:
        print(f"Error updating object {obj_id} status", error)


def put_hashsum(obj_id, hashsum):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        update_query = "UPDATE Objects SET hash = %s WHERE id = %s"
        cursor.execute(update_query, (hashsum, obj_id))

        connection.commit()
        cursor.close()
        connection.close()
        print(f"Updated object {obj_id} hash to {hashsum}")

    except Exception as error:
        print(f"Error updating object {obj_id} hash", error)


def process_message(msg, thread_id):
    try:
        message = json.loads(msg.value().decode('utf-8'))
        obj_id = message['id']
        name = message['name']
        size = message['size']

        print(f"Processing object {obj_id}: {name}, size: {size} by {thread_id}")

        # Здесь добавить код для загрузки данных в хранилище

        # TODO

        # Обновляем статус объекта после успешной обработки
        update_status(obj_id, 'uploaded')

    except Exception as error:
        print(f"Error processing message: {error}")


def consumer_loop(thread_id):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)

    topic = 'topic-demo'
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_message(msg, thread_id)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer_threads(num_threads):
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=consumer_loop, args=(i,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == '__main__':
    num_threads = 4  # Количество потоков
    start_consumer_threads(num_threads)

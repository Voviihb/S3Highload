import json
import threading

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException

from database.db_config import db_config
from kafka.kafka_config import consumer_conf, topic_conf


class Worker:
    def __init__(self, thread_num: int):
        self.thread_num = thread_num

    def print_info(self):
        print(f'Worker number: {self.thread_num}')

    def update_status(self, obj_id, new_status):
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

    def put_hashsum(self, obj_id, hashsum):
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

    def process_message(self, msg):
        try:
            message = json.loads(msg.value().decode('utf-8'))
            obj_id = message['id']
            name = message['name']
            size = message['size']

            print(f"Processing object {obj_id}: {name}, size: {size} by {self.thread_num}")

            # Здесь добавить код для загрузки данных в хранилище

            # TODO

            # Обновляем статус объекта после успешной обработки
            self.update_status(obj_id, 'uploaded')

        except Exception as error:
            print(f"Error processing message: {error}")

    def consumer_loop(self):
        consumer = Consumer(consumer_conf)

        topic = topic_conf
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
                    self.process_message(msg)

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    def run(self):
        self.consumer_loop()


if __name__ == '__main__':
    num_threads = 4  # Количество потоков
    threads = []

    for i in range(num_threads):
        worker = Worker(i)
        thread = threading.Thread(target=worker.run)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

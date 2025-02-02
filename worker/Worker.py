import json
import signal
import threading
import hashlib

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Gauge, Counter, Summary, start_http_server, CollectorRegistry, write_to_textfile

from database.db_config import db_config
from kafka.kafka_config import consumer_conf, topic_conf
from s3.storage_functions import *

# Создание реестра метрик
registry = CollectorRegistry()

# Путь для сохранения метрик
metrics_file = '/tmp/metrics.prom'


def load_metrics():
    if os.path.exists(metrics_file):
        with open(metrics_file, 'r') as f:
            for line in f:
                if line.startswith('#') or line.strip() == '':
                    continue
                name, value = line.split()
                value = float(value)
                if name == 'objects_total':
                    OBJECTS_TOTAL.set(value)
                elif name == 'size_total':
                    SIZE_TOTAL.set(value)
                elif name == 'uploads_total':
                    UPLOADS_TOTAL._value.set(value)
                elif name == 'uploads_success':
                    UPLOADS_SUCCESS._value.set(value)
                elif name == 'uploads_failure':
                    UPLOADS_FAILURE._value.set(value)
                elif name == 'deletions_total':
                    DELETIONS_TOTAL._value.set(value)
                elif name == 'deletions_success':
                    DELETIONS_SUCCESS._value.set(value)
                elif name == 'deletions_failure':
                    DELETIONS_FAILURE._value.set(value)


def save_metrics(signum, frame):
    write_to_textfile(metrics_file, registry)
    print(f'Metrics saved to {metrics_file}')
    os._exit(0)


# Регистрация обработчиков сигналов
signal.signal(signal.SIGINT, save_metrics)
signal.signal(signal.SIGTERM, save_metrics)

# Создание метрик
OBJECTS_TOTAL = Gauge('objects_total', 'Total number of objects in storage', registry=registry)
SIZE_TOTAL = Gauge('size_total', 'Total size of objects in storage (bytes)', registry=registry)

UPLOADS_TOTAL = Counter('uploads_total', 'Total number of file uploads', registry=registry)
UPLOADS_SUCCESS = Counter('uploads_success', 'Number of successful file uploads', registry=registry)
UPLOADS_FAILURE = Counter('uploads_failure', 'Number of failed file uploads', registry=registry)
UPLOAD_DURATION = Summary('upload_duration_seconds', 'Time taken for upload', registry=registry)

DELETIONS_TOTAL = Counter('deletions_total', 'Total number of file deletions', registry=registry)
DELETIONS_SUCCESS = Counter('deletions_success', 'Number of successful file deletions', registry=registry)
DELETIONS_FAILURE = Counter('deletions_failure', 'Number of failed file deletions', registry=registry)
DELETIONS_DURATION = Summary('deletions_duration_seconds', 'Time taken for deletions', registry=registry)

load_metrics()


class Worker:
    def __init__(self, thread_num: int, task_queue: list = None):
        self.thread_num = thread_num
        self.task_queue = task_queue or []

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

    @staticmethod
    def get_objects_by_status(status):
        try:
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()

            query = "SELECT id, name, size FROM Objects WHERE status = %s"
            cursor.execute(query, (status,))
            object_ids = cursor.fetchall()

            cursor.close()
            connection.close()
            print(object_ids)
            return object_ids

        except Exception as error:
            print(f"Error fetching objects with status {status}: {error}")
            return []

    def delete_object(self, obj_id, name, size):
        result = delete_file(bucket_name, name)
        if result:
            OBJECTS_TOTAL.dec()
            SIZE_TOTAL.dec(size)
            DELETIONS_TOTAL.inc()
            DELETIONS_SUCCESS.inc()
            print(f"Deleted object {obj_id}: {name} by {self.thread_num}")
            self.update_status(obj_id, 'deleted')
        else:
            DELETIONS_TOTAL.inc()
            DELETIONS_FAILURE.inc()
            print(f"Error deleting object {name}")

    def delete_objects_from_queue(self):
        while len(self.task_queue) > 0:
            obj_id, name, size = self.task_queue.pop(0)
            self.delete_object(obj_id, name, size)

    def create_file(self, name, size):
        """Создает файл с заданным именем и размером в байтах с случайным содержимым."""
        with open(name, 'wb') as f:
            f.write(os.urandom(size))
        print(f"Created file {name} with size {size} bytes.")

    def compute_md5(self, filename):
        """Вычисляет MD5 хеш для заданного файла."""
        hash_md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def process_message(self, msg):
        try:
            message = json.loads(msg.value().decode('utf-8'))
            obj_id = message['id']
            name = message['name']
            size = message['size']

            print(f"Processing object {obj_id}: {name}, size: {size} by {self.thread_num}")

            # Создание файла с случайным содержимым
            self.create_file(name, size)

            # Вычисление хеш-суммы
            hashsum = self.compute_md5(name)

            # Обновляем статус объекта и сохраняем хеш-сумму
            self.put_hashsum(obj_id, hashsum)
            with UPLOAD_DURATION.time():
                result = upload_file(name, bucket_name)
                if result:
                    UPLOADS_TOTAL.inc()
                    OBJECTS_TOTAL.inc()
                    SIZE_TOTAL.inc(size)
                    UPLOADS_SUCCESS.inc()
                    self.update_status(obj_id, 'uploaded')
            if os.path.exists(name):
                os.remove(name)

        except Exception as error:
            UPLOADS_TOTAL.inc()
            UPLOADS_FAILURE.inc()
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
            return
        finally:
            consumer.close()

    def run(self):
        self.consumer_loop()


if __name__ == '__main__':
    start_http_server(8000, registry=registry)

    while True:
        user_choice = input("1 - upload files from kafka\n2 - delete all objects\nend - exit\n")
        if user_choice == '1':
            num_threads = 4  # Количество потоков
            threads = []

            for i in range(num_threads):
                worker = Worker(i)
                thread = threading.Thread(target=worker.run)
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

        elif user_choice == '2':
            uploaded_objects = Worker.get_objects_by_status("uploaded")

            num_threads = 4
            threads = []

            for i in range(num_threads):
                worker = Worker(i, [uploaded_objects[j] for j in range(i, len(uploaded_objects), num_threads)])
                thread = threading.Thread(target=worker.delete_objects_from_queue)
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

        elif user_choice == 'end':
            break

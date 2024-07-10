from confluent_kafka import Producer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092'
    }

    producer = Producer(conf)

    topic = 'topic-demo'

    for i in range(10):
        # Produce a message
        producer.produce(topic, key=str(i), value=f'message {i}', callback=delivery_report)

        # Wait up to 1 second for events. Callbacks will be invoked during
        # this method call if the message is successfully delivered or
        # permanently fails delivery.
        producer.poll(1)

    # Wait for any outstanding messages to be delivered and delivery reports
    # to be received.
    producer.flush()

if __name__ == '__main__':
    main()

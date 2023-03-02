from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "active_data",
        'enable.auto.commit': False,
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)
print(consumer)
running = True
def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error:
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition')
                else:
                    print('Error consuming message: {}'.format(msg.error()))
            else:
                try:
                    data = msg.value().decode('utf-8')
                    if data.startswith('ERROR'):
                        print('Invalid data found: {}'.format(data))
                except Exception as e:
                    print('Error while decoding message: {}'.format(str(e)))
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
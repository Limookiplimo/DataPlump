from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from confluent_kafka import Consumer
# from collections import namedtuple



#Kafka topic
ACTIVE_TOPIC = 'active_data'

#Producer object
producer = KafkaProducer(bootstrap_servers='localhost:9092')


def validate_data():
    # Subscribe to kafka topic
    consumer = Consumer(ACTIVE_TOPIC,
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)
    message = next(consumer)
    print(message.value.decode())

#Consume messages and validate
    running = True
    try:
        while running:
            event = consumer.poll(1.0)
            if event is None:
                continue
            if event.error():
                if event.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition')
                else:
                    print('Error consuming message: {}'.format(event.error()))
            else:
                # Validate message
                try:
                    data = event.value().decode('utf-8')
                    # Add validation logic here
                    if data.startswith('ERROR'):
                        print('Invalid data found: {}'.format(data))
                except Exception as e:
                    print('Error while decoding message: {}'.format(str(e)))

                    #Dead letter queue
                    for row in data:
                        message = str(row).encode('utf-8')
                        producer.send('dead_data', value = message)
                        producer.flush()


    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

validate_data()
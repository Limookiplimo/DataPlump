from kafka import KafkaConsumer
from kafka.errors import KafkaError


#Kafka topic
DATA_KAFKA_TOPIC = 'active_data'


def validate_data():
    # Subscribe to kafka topic
    consumer = KafkaConsumer({
        'bootstrap.servers': 'localhost:9092'})
    consumer.subscribe([DATA_KAFKA_TOPIC])
    for x in consumer:
        print(x)

    # Consume messages and validate
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print('Error while consuming message: {}'.format(msg.error()))
        else:
            # Validate message
            try:
                data = msg.value().decode('utf-8')
                # Add validation logic here
                if data.startswith('ERROR'):
                    print('Invalid data found: {}'.format(data))
            except Exception as e:
                print('Error while decoding message: {}'.format(str(e)))



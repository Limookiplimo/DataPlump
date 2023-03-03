from kafka import KafkaConsumer, KafkaProducer
#Kafka topic
ACTIVE_TOPIC = 'data_extracts'

#Producer object
producer = KafkaProducer(bootstrap_servers='localhost:9092')


def validate_data():
    # Subscribe to kafka topic
    consumer = KafkaConsumer(ACTIVE_TOPIC,
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)
    message = next(consumer)
    print(message.value.decode())

validate_data()
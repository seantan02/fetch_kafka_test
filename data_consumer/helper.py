from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json
from cryptography.fernet import Fernet
import datetime

#Kafka consumer
def create_kafka_consumer(topic, server = "kafka:9092", auto_offset_reset = "earliest", enable_auto_commit = False, value_deserializer=lambda m: json.loads(m.decode('ascii')), consumer_timeout_ms = 1000) -> tuple:
    """
    This method creates a kafka consumer object
    :param topic: The name of the Kafka topic to consume messages from.
    :param server: The Kafka broker server address in the format "host:port". Defaults to "kafka:9092".
    :param auto_offset_reset: The auto offset reset policy for the consumer. Defaults to "earliest" (start consuming from the beginning of the topic).
    :param enable_auto_commit: Enable automatic offset commit. Defaults to False.
    :param value_deserializer: A function to deserialize the message values. Defaults to a function that assumes messages are JSON-encoded.
    :param consumer_timeout_ms: The maximum time, in milliseconds, the consumer will wait for new messages before returning control to the caller. Defaults to 1000ms (1 second).
    :return: A tuple in the format (bool, object). (True, kafka object) if no error; (False, exception) if an error occurs during consumer creation.
    """
    try:
        consumer = KafkaConsumer(topic, 
                bootstrap_servers=server,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=enable_auto_commit,
                value_deserializer=value_deserializer,
                consumer_timeout_ms=consumer_timeout_ms)
        return True, consumer
    except Exception as e:
        return False, e

#Kafka producer
def create_kafka_producer(server = "kafka:9092", value_serializer=lambda m: json.dumps(m).encode('ascii'), retries = 5):
    """
    This method creates a kafka consumer object
    :param server: The Kafka broker server address in the format "host:port". Defaults to "kafka:9092".
    :param value_serializer: A function to serialize the message values. Defaults to a function that assumes messages are JSON-encoded.
    :param retries: Number of retries attemp
    :return: A tuple in the format (bool, object). (True, kafka object) if no error; (False, exception) if an error occurs during consumer creation.
    """
    try:
        producer = KafkaProducer(bootstrap_servers=server,
                                acks='all',
                                value_serializer=value_serializer,
                                retries=retries)
        return True, producer
    except Exception as e:
        return False, e

def kafka_producer_send(producer_object, topic, data) -> bool:
    """
    This method creates a kafka consumer object
    :param producer_object: The Kafka producer object
    :param topic: Topic of the product
    :param data: Data to send
    :return: bool. True if send; False otherwise.
    """

    def on_send_success(record_metadata):
        print(f"Data successfult written with topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")

    def on_send_error(excp):
        print(f"Error occured: {str(excp)}")

    producer_object.send(topic, data).add_callback(on_send_success).add_errback(on_send_error).get()
    print(f"Data written into topic: {topic} with data: {str(data)}")
    producer_object.flush()
    return True


#Encryption and Decryption
def generate_key():
    key = Fernet.generate_key()
    return key

def encrypt_data(key, data):
    cipher_suite = Fernet(key)
    # Encrypt the data
    return cipher_suite.encrypt(data.encode())

def decrype_data(key, data):
    cipher_suite = Fernet(key)
    # Decrypt the string (later)
    return cipher_suite.decrypt(data).decode()

#Datetime
def convert_unix_to_datetime(timestamp):
    # Convert Unix timestamp to datetime
    datetime_obj = datetime.datetime.utcfromtimestamp(timestamp)
    # Print the datetime object in a specific format
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
import logging
from confluent_kafka import Producer
import time
import random
import string

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers):
        """
        Initializes the Kafka producer with the given bootstrap servers.
        """
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers
        }
        self.producer = Producer(self.producer_config)

    def produce_message(self, topic, key, value):
        """
        Produces a message to the specified Kafka topic with the given key and value.
        """
        self.producer.produce(topic, key=key, value=value)
        self.producer.flush()

def generate_random_otp(length=8):
    """
    Generates a random OTP code of the specified length.
    """
    characters = string.ascii_letters + string.digits
    otp = ''.join(random.choice(characters) for _ in range(length))
    return otp

def select_random_email(file_path):
    """
    Selects a random email from the given text file.
    """
    with open(file_path, 'r') as file:
        emails = file.readlines()
    if not emails:
        raise ValueError("The email list is empty.")
    return random.choice(emails).strip()

def kafka_producer_main(**kwargs):
    email_file_path = '/email_list.txt'  # Path to your email list file
    email_address = select_random_email(email_file_path)
    
    # Log the selected email
    logger.info(f"Selected email: {email_address}")

    bootstrap_servers = 'kafka1:19092'
    kafka_producer = KafkaProducerWrapper(bootstrap_servers)
    
    topic = "email_topic"
    
    # Generate a single OTP
    key = email_address
    value = generate_random_otp()
    
    # Produce the message
    kafka_producer.produce_message(topic, key, value)
    logger.info(f"Produced message: Email={key}, OTP={value}")
    
    # Flush the producer
    kafka_producer.producer.flush()
    logger.info(f"Producer flushed. Email produced: {email_address}")

    # Push the email to XCom
    kwargs['ti'].xcom_push(key='email', value=email_address)



if __name__ == "__main__":
    kafka_producer_main()

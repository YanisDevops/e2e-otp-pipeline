from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBConnector:
    def __init__(self, mongodb_uri, database_name, collection_name):
        try:
            self.client = MongoClient(mongodb_uri)
            self.db = self.client[database_name]
            self.collection_name = collection_name
            logger.info(f"Connected to MongoDB at {mongodb_uri}")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise

    def create_collection(self):
        try:
            if self.collection_name not in self.db.list_collection_names():
                self.db.create_collection(self.collection_name)
                logger.info(f"Created collection: {self.collection_name}")
            else:
                logger.warning(f"Collection {self.collection_name} already exists")
        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            raise

    def upsert_data(self, email, otp):
        document = {
            "email": email,
            "otp": otp
        }
        try:
            # Delete existing document if it exists
            self.db[self.collection_name].delete_one({"email": email})

            # Insert new document
            self.db[self.collection_name].insert_one(document)
            logger.info(f"Upserted document into MongoDB: {document}")
        except Exception as e:
            logger.error(f"Error upserting document into MongoDB: {e}")

    def close(self):
        self.client.close()
        logger.info("Closed MongoDB connection")

class KafkaConsumerWrapperMongoDB:
    def __init__(self, kafka_config, topics, mongodb_connector):
        try:
            self.consumer = Consumer(kafka_config)
            self.consumer.subscribe(topics)
            self.mongodb_connector = mongodb_connector
            logger.info("Kafka consumer initialized and subscribed to topics")
        except Exception as e:
            logger.error(f"Error initializing Kafka consumer: {e}")
            raise

    def consume_and_insert_messages(self):
        start_time = time.time()
        try:
            while True:
                elapsed_time = time.time() - start_time
                if elapsed_time >= 30:
                    break

                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info('Reached end of partition')
                    else:
                        logger.warning('Error: {}'.format(msg.error()))
                else:
                    email = msg.key().decode('utf-8')
                    otp = msg.value().decode('utf-8')

                    logger.info(f"Processing message: Email={email}, OTP={otp}")

                    # Upsert data: delete existing and insert new
                    self.mongodb_connector.upsert_data(email, otp)
                    logger.info(f'Received and upserted: Email={email}, OTP={otp}')

        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt. Closing consumer.")
        finally:
            self.mongodb_connector.close()
            self.consumer.close()
            logger.info("Kafka consumer closed")

def kafka_consumer_mongodb_main():
    mongodb_uri = 'mongodb://root:root@mongo:27017/'
    database_name = 'email_database'
    collection_name = 'email_collection'
    mongodb_connector = MongoDBConnector(mongodb_uri, database_name, collection_name)

    mongodb_connector.create_collection()

    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
        'group.id': 'mongo_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    topics = ['email_topic']

    kafka_consumer = KafkaConsumerWrapperMongoDB(kafka_config, topics, mongodb_connector)
    kafka_consumer.consume_and_insert_messages()

if __name__ == '__main__':
    kafka_consumer_mongodb_main()

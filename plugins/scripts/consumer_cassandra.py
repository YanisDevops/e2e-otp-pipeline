from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
import time
import logging
import socket

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self, contact_points):
        try:
            for point in contact_points:
                logger.info(f"Attempting to resolve {point}")
                resolved_ip = socket.gethostbyname(point)
                logger.info(f"Resolved {point} to {resolved_ip}")
            self.cluster = Cluster(contact_points)
            self.session = self.cluster.connect()

        except Exception as e:
            logger.error(f"Error connecting to Cassandra: {e}")
            raise

    def create_keyspace_and_table(self):
        try:
            self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS email_namespace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
            """)
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS email_namespace.email_table (
                    email text PRIMARY KEY,
                    otp text
                );
            """)
            logger.info("Keyspace and table created or already exist.")
        except Exception as e:
            logger.error(f"Error creating keyspace and table: {e}")

    def upsert_data(self, email, otp):
        try:
            # Delete existing record if exists
            self.session.execute("""
                DELETE FROM email_namespace.email_table WHERE email = %s
            """, (email,))

            # Insert new OTP for the email
            self.session.execute("""
                INSERT INTO email_namespace.email_table (email, otp)
                VALUES (%s, %s)
            """, (email, otp))

            logger.info(f"Upserted data into Cassandra: Email={email}, OTP={otp}")
        except Exception as e:
            logger.error(f"Error upserting data: {e}")

    def shutdown(self):
        self.cluster.shutdown()

def fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs):
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    start_time = time.time()
    try:
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time >= run_duration_secs:
                break

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('Reached end of partition')
                else:
                    logger.error('Error: {}'.format(msg.error()))
            else:
                email = msg.key().decode('utf-8')
                otp = msg.value().decode('utf-8')

                logger.info(f"Received message: Email={email}, OTP={otp}")

                # Upsert data: delete existing and insert new
                cassandra_connector.upsert_data(email, otp)
                            
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Closing consumer.")
    finally:
        consumer.close()

def kafka_consumer_cassandra_main():
    cassandra_connector = CassandraConnector(['cassandra'])
    
    # Create keyspace and table
    cassandra_connector.create_keyspace_and_table()

    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
        'group.id': 'cassandra_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    topic = 'email_topic'
    run_duration_secs = 20

    fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs)

    cassandra_connector.shutdown()

if __name__ == '__main__':
    kafka_consumer_cassandra_main()

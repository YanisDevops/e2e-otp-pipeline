from confluent_kafka.admin import AdminClient, NewTopic
import logging
from confluent_kafka.error import KafkaError
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

admin_config = {
    'bootstrap.servers': 'kafka1:19092',
    'client.id': 'kafka_admin_client'
}

def kafka_create_topic_main():
    """Checks if the topic email_topic exists or not. If not, creates the topic."""
    topic_name = 'email_topic'

    try:
        logger.info("Attempting to connect to Kafka...")
        admin_client = AdminClient(admin_config)
        logger.info("Connected to Kafka successfully.")

        logger.info("Listing existing topics...")
        existing_topics = admin_client.list_topics(timeout=5).topics
        logger.info(f"Existing topics: {existing_topics}")

        if topic_name in existing_topics:
            logger.info(f"Topic '{topic_name}' already exists.")
            return "Exists"
        
        logger.info(f"Creating new topic '{topic_name}'...")
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
        fs = admin_client.create_topics([new_topic])

        # Wait for each operation to finish.
        for topic, f in fs.items():
            try:
                f.result(timeout=10)  # 10 seconds timeout
                logger.info(f"Topic '{topic}' created")
            except KafkaError as e:
                logger.error(f"Failed to create topic {topic}: {e}")
                return f"Failed: {e}"

        return "Created"

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return f"Error: {e}"

if __name__ == "__main__":
    result = kafka_create_topic_main()
    logger.info(f"Final result: {result}")
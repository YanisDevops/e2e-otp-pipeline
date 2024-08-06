from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

    def select_data(self, email):
        data_dict = {'email': '', 'otp': ''}
        try:
            document = self.db[self.collection_name].find_one({"email": email})
            if document:
                data_dict['email'] = document.get('email', '')
                data_dict['otp'] = document.get('otp', '')
                logger.info(f"Found document for email {email}: {document}")
            else:
                logger.warning(f"No document found for email {email}")
        except Exception as e:
            logger.error(f"Error retrieving document: {e}")
        
        return data_dict

    def close(self):
        self.client.close()
        logger.info("Closed MongoDB connection")

def check_mongodb_main(email):
    mongodb_uri = 'mongodb://root:root@mongo:27017/'
    database_name = 'email_database'
    collection_name = 'email_collection'
    mongodb_connector = MongoDBConnector(mongodb_uri, database_name, collection_name)

    try:
        data_dict = mongodb_connector.select_data(email)
    finally:
        mongodb_connector.close()
    
    logger.info(f"Data found for email: {data_dict.get('email', '')}")
    logger.info(f"OTP: {data_dict.get('otp', '')}")

    return data_dict
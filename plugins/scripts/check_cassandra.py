from cassandra.cluster import Cluster
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self, contact_points):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect()

    def select_data(self, email):
        query = "SELECT email, otp FROM email_namespace.email_table WHERE email = %s"
        result = self.session.execute(query, (email,))
        
        data_dict = {'email': '', 'otp': ''}
        
        for row in result:
            data_dict['email'] = row.email
            data_dict['otp'] = row.otp
            logger.info(f"Email: {row.email}, OTP: {row.otp}")
        
        return data_dict

    def close(self):
        self.cluster.shutdown()

def check_cassandra_main(email):
    cassandra_connector = CassandraConnector(['cassandra'])
    
    try:
        data_dict = cassandra_connector.select_data(email)
    finally:
        cassandra_connector.close()
    
    logger.info(f"Data found for email: {data_dict.get('email', '')}")
    logger.info(f"OTP: {data_dict.get('otp', '')}")

    return data_dict

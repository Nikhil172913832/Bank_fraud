#Library imports
from kafka import KafkaConsumer
import msgpack
import logging
#Logger Configuration
logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('producer.log')
            ]
        )

#Consumer Configuration
transaction_consumer = KafkaConsumer('transactions',
                         group_id='fraud_detection_group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=msgpack.unpackb,
                         consumer_timeout_ms=5000,
                         auto_offset_reset='latest',
                         enable_auto_commit=False)

def transactions_consumer():








import os
import socket
import logging 
from confluent_kafka import Consumer, Producer 
from confluent_kafka.admin import AdminClient, NewTopic 
from confluent_kafka.cimpl import KafkaError

logger = logging.getLogger(__name__)

host = os.getenv("GUIDE_HOST", "localhost")
port = os.getenv("GUIDE_PORT", 9092)
kafka_broker = host + ":" + str(port)

# -----------------------------------------------------------------------------
# Admin Client 
# -----------------------------------------------------------------------------

def get_admin(): 
    config = {
        'bootstrap.servers': kafka_broker
    }
    try:
        admin_client = AdminClient(config)
        return admin_client
    except Exception as e:
        logger.error(f"Error: Unable to Connect with Kafka Admin Client. Error reason : {e}")
        print(
            "Error: Unable to Connect with Kafka Admin Client."
            "Error reason : {}".format(e)
        )
        return None

def delete_topics(topics):
    admin = get_admin()
    fs = admin.delete_topics(topics)
    for topic, f in fs.items():
        try:
            f.result()
            print("[+] Topic {} deleted".format(topic))
        except Exception as e:
            print("[-] Failed to delete topic {}: {}".format(topic, e))

def create_topics(topics):
    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    admin = get_admin()
    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("[+] Topic {} created successfully".format(topic))
            logger.info("[+] Topic {} created successfully".format(topic))
        except Exception as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:  
                print("[-] Topic {} already exists".format(topic))                                
            else:
                print("[-] Failed to create topic {}: {}".format(topic, e))

# -----------------------------------------------------------------------------
# Producer client  
# -----------------------------------------------------------------------------

def get_producer_client() -> Producer:
    config = {
        'bootstrap.servers': kafka_broker,
        'client.id': socket.gethostname(),
        'enable.idempotence': True
    }
    return Producer(config)

# -----------------------------------------------------------------------------
# Consumer client  
# -----------------------------------------------------------------------------

def get_consumer_client(group_id, process_id) -> Consumer:
    config = {
        'bootstrap.servers': kafka_broker,
        'group.id': group_id,
        'group.instance.id': process_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.offset.store': False,
        'client.id': socket.gethostname()
    }
    
    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    return Consumer(config, logger=logger)
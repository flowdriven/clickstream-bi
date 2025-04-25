"""Module providing utilities."""

import os
import socket
import logging
import json
import pandas as pd
from dateutil.parser import parse
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

logger = logging.getLogger(__name__)

host = os.getenv("GUIDE_HOST", "localhost")
port = os.getenv("GUIDE_PORT", "9092")
kafka_broker = host + ":" + str(port)

local_data_directory = os.getenv("LOCAL_DATA_DIRECTORY", "data")

# -----------------------------------------------------------------------------
# Admin Client
# -----------------------------------------------------------------------------

def get_admin():
    """Function providing Kafka admin client"""
    config = {
        'bootstrap.servers': kafka_broker
    }
    try:
        admin_client = AdminClient(config)
        return admin_client
    except Exception as e:
        logger.error("Error: Unable to Connect with Kafka Admin Client. Error reason : %s", e)
        print("Error: Unable to Connect with Kafka Admin Client.")
        print(f"Error reason : {e}")
        return None

def delete_topics(topics):
    """Function deleting existing topics"""
    admin = get_admin()
    fs = admin.delete_topics(topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"[+] Topic '{topic}' deleted")
        except Exception as e:
            print(f"[-] Failed to delete topic '{topic}': {e}")

def create_topics(topics):
    """Function creating new topics"""
    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    admin = get_admin()
    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"[+] Topic '{topic}' created successfully")
            logger.info("[+] Topic '%s' created successfully", topic)
        except Exception as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"[-] Topic '{topic}' already exists")
            else:
                print(f"[-] Failed to create topic '{topic}': {e}")

# -----------------------------------------------------------------------------
# Producer client
# -----------------------------------------------------------------------------

def get_producer_client() -> Producer:
    """Function providing kafka producer client"""
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
    """Function providing kafka consumer client"""
    config = {
        'bootstrap.servers': kafka_broker,
        'group.id': group_id,
        'group.instance.id': process_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.offset.store': False,
        'client.id': socket.gethostname()
    }

    # Create logger for consumer (logs will be emitted when poll() is called)
    #logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    return Consumer(config, logger=logger)

# -----------------------------------------------------------------------------
# Writer service
# -----------------------------------------------------------------------------

def write_event(record: str, offset: str) -> str:
    """Function for writing event to a file"""
    # Parse the JSON data into a Python dictionary
    record_dict = json.loads(record)

    df = pd.DataFrame(record_dict, index=[0])

    event_time = df['event_time'].values[:1][0]
    date_obj = parse(event_time[0:19])
    date_time = date_obj.strftime("%y-%m-%d_%H-%M-%S")
    event_type = df['event_type'].values[:1][0]
    filename = date_time + '_offset_' + offset + '_' + event_type + '.json'
    filepath = f"./{local_data_directory}/{filename}"

    df.to_json(filepath, orient='records')

    return filename
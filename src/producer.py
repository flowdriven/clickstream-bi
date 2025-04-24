import os
import glob 
import time
import csv 
import json
import uuid
import logging 
import utils  
from pathlib import Path
from typing import Generator, Dict

logger = logging.getLogger(__name__)
data_directory = os.getenv("DATA_DIRECTORY", "data")

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('(-) Message delivery failed: {}'.format(err))
        logger.error(f"Error: Message delivery failed. Error reason : {err}")
    #else:
    #    print('(+) Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def process_csv(filename: str) -> Generator[Dict, None, None]:
    with open(filename, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row

def process_topic(topic):
    count = 0
    start_time = time.time()

    producer_client = utils.get_producer_client()
 
    for filename in glob.glob('./' + data_directory + '/*csv'):
        file_path = Path(filename)
        topic_file = file_path.stem
        if topic_file == topic:
            for record in process_csv(filename):
                record_str = json.dumps(record)
                record_bytes = bytes(record_str, 'utf-8')
                producer_client.produce(
                    topic=topic, 
                    key=str(uuid.uuid4().hex),
                    value=record_bytes, 
                    callback=delivery_report
                )
                count += 1
            producer_client.flush() 
    
    print(f"(+) Events count == {count}")
    logger.info(f"(+) Events count == {count}")
    print("(+) Execution time: {} seconds \n".format(time.time() - start_time))
    logger.info("(+) Execution time: {} seconds \n".format(time.time() - start_time))

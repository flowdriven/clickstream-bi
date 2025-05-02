from typing import Generator, Dict

import codecs
import time
import csv
import json
import uuid
import logging
from src import utils
from src.aws_utils import get_file_from_s3

logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('(-) Message delivery failed: {err}')
        logger.error("Error: Message delivery failed. Error reason %s: ", err)
    #else:
    #    print('(+) Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def process_csv(data: object) -> Generator[Dict, None, None]:
    """
    Processes a CSV file retrieved from an S3 bucket.

    Args:
        data (object): 
            StreamingBody object containing the CSV file data from an S3 bucket.

    Returns:
        Generator[Dict, None, None]: 
            generator that yields each row of the CSV file as a dictionary.

    Features:
        - codecs.StreamReader to decode data from the stream 
            and returns the resulting object
        - the codecs.StreamReader takes in input a file-like object
            having a read() method 
        - the codecs.StreamReader supports the iterator protocol, therefore
            the resulting object is passed into the csv.DictReader
        - codecs.getreader() is the function used to create the StreamReader,  
            by passing the codec utf-8
        - the CSV file can be read row-by-row into a dictionary 
            by passing the codecs.StreamReader into csv.DictReader
    """
    # Create a StreamReader to decode the stream using utf-8 codec
    csv_reader = csv.DictReader(codecs.getreader("utf-8")(data["Body"]))
    # Yield each row of the CSV file as a dictionary
    yield from csv_reader

def process_topic(topic):
    """Function sending records to any topic."""
    count = 0
    start_time = time.time()

    producer_client = utils.get_producer_client()

    key = topic + '.csv'
    data = get_file_from_s3(key)

    for record in process_csv(data):
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
    logger.info("(+) Events count == %s", count)
    print(f"(+) Execution time: {time.time() - start_time} seconds \n")
    logger.info("(+) Execution time: %s seconds \n", (time.time() - start_time))

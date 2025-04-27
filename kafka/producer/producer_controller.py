"""Module for managing producer clients."""

import os
import logging
from multiprocessing import Process, current_process
from src import producer
from src import utils

logger = logging.getLogger(__name__)
topic_list = os.getenv("TOPIC_LIST").split(",")

def producer_process(topic):
    """Function managing process"""
    print(f"(+) {current_process().name} is processing '{topic}'")
    logger.info("(+) %s is processing '%s'", current_process().name, topic)
    producer.process_topic(topic)

def start_processing():
    """Function managing multiprocessing"""
    for topic in topic_list:
        Process(target=producer_process, args=(topic,)).start()

def main():
    """Function initializing topics and starting processing """
    utils.delete_topics(topic_list)
    utils.create_topics(topic_list)
    start_processing()

if __name__ == '__main__':
    main()
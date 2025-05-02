import os
import logging
from multiprocessing import Process, current_process
from src import local_producer
from src import utils

logger = logging.getLogger(__name__)
topic_list = os.getenv("TOPIC_LIST").split(",")

def producer_process(topic):
    """Function managing process"""
    print(f"(+) {current_process().name} is processing '{topic}'")
    logger.info("(+) %s is processing '%s'", current_process().name, topic)
    local_producer.process_topic(topic)

def start_processing():
    """Function managing multiprocessing"""
    for topic in topic_list:
        Process(target=producer_process, args=(topic,)).start()

def init_topics():
    """Function initializing topics"""
    utils.delete_topics(topic_list)
    utils.create_topics(topic_list)

def main():
    """Main function """
    init_topics()
    start_processing()

if __name__ == '__main__':
    main()
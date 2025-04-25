import time
from confluent_kafka import KafkaError
import src.utils as utils
from src.utils import write_event 

def process_msg(msg):
    offset = str(msg.offset())
    record = msg.value().decode('utf-8')

    event_filename = write_event(record, offset)
    return event_filename
     
def process_topic(topic, process_name):
    count = 0
    consumer = utils.get_consumer_client(topic, process_name)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    consumer.subscribe([topic], on_assign=print_assignment)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("(-) Consumer error: {}".format(msg.error()))
                    break
             
            filename = process_msg(msg)
            count += 1 
            print(f"(+) New event file: {filename} (#{count}) \n")  
            consumer.store_offsets(msg)

    except Exception as e:
        print(f"(-) Unexpected errExecutionor: {e}")
        # break before retrying
        time.sleep(1)
    except KeyboardInterrupt:
        consumer.close()
        print(f"(-) Aborted by user \n")
    finally: 
        consumer.close()

    print(f"(+) Events count == {count}")

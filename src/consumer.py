import time
from confluent_kafka import KafkaError
from src import utils
from src import aws_utils

TIMEOUT_SECONDS = 15

def termination_required(last_message_time):
    return time.time() - last_message_time > TIMEOUT_SECONDS

def process_msg(msg):
    """Function writing message to output file."""
    offset = str(msg.offset())
    record = msg.value().decode('utf-8')

    event_filename = aws_utils.write_event(record, offset)
    return event_filename

def process_topic(topic, process_name):
    """Function subscribing and reading from topic."""
    count = 0
    last_message_time = 0

    consumer = utils.get_consumer_client(topic, process_name)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    consumer.subscribe([topic], on_assign=print_assignment)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:       
                if last_message_time == 0:
                    last_message_time = time.time()
                else:
                    if termination_required(last_message_time):
                        print("(+) Timeout reached, stopping consumer.") 
                        break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"(-) Consumer error: {msg.error()}")
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
        print("(-) Aborted by user \n")
    finally:
        consumer.close()

    print(f"(+) Consumed: {count} events")
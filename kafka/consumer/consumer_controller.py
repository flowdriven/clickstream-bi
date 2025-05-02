import os
import time
from multiprocessing import Process, current_process
from src import local_consumer

topic_list = os.getenv("TOPIC_LIST").split(",")
TIMEOUT_SECONDS = 30

def timeout_procs(procs):
    start = time.time()
    while time.time() - start <= TIMEOUT_SECONDS:
        if any(p.is_alive() for p in procs):
            time.sleep(.1)
        else:
            for p in procs:
                p.join()
                print(f" [->] stopping process {p.name}")
            break
    else:
        print(" [->] timed out, killing all processes")
        for p in procs:
            if not p.is_alive():
                print(f" [->] process already finished: {p.name}")
            else:
                p.terminate()
                print(f" [->] stopping (joining) process {p.name}")
                p.join()

def consumer_process(topic):
    print(f"(+) {current_process().name} is processing '{topic}'")
    local_consumer.process_topic(topic, current_process().name)

def start_procs():
    procs = []
    # Assign one consumer per topic, each running in a separate process.
    # The consumers in the same group in this case won't balance partitions across topics.
    # Therefore, assign a unique group.id to each consumer to avoid rebalancing delays.
    for topic in topic_list:
        p = Process(target=consumer_process, args=(topic, ))
        procs.append(p)
        p.start()
    return procs

def start_processing():
    procs = start_procs()
    timeout_procs(procs)

if __name__ == '__main__':
    start_processing()
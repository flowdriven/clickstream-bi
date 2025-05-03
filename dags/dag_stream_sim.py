from datetime import timedelta
from datetime import datetime
from airflow import DAG 
from airflow.decorators import task, task_group

from src import utils, producer, consumer  

start_date = datetime.today() - timedelta(days=1)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": start_date,
    "retries": 1,  # number of retries before failing the task
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="dag_stream_sim",
    default_args=default_args,
    schedule=None, 
    tags=["stream simulation"],
) as dag: 
    
    @task(task_id="get_topic_list")
    def get_topic_list():
        topic_list = utils.get_topics()
        return topic_list

    @task_group(group_id="topic_handler")
    def topic_handler(topic):

        @task 
        def init(topic):
            utils.delete_topics([topic])
            utils.create_topics([topic])

        @task
        def produce(topic):
            producer.process_topic(topic)

        @task 
        def consume(topic):
            consumer.process_topic(topic, f"consumer_{topic}")

        init(topic) >> [produce(topic), consume(topic)]

    topic_handler.expand(topic=get_topic_list())

    #end task
    # launch aws glue job 


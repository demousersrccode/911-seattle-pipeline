import airflow

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

import os
from dotenv import load_dotenv

load_dotenv()


CLIENT_HOST=os.getenv('CLIENT_HOST')
CLIENT_PORT=os.getenv('CLIENT_PORT')
TOPIC=os.getenv('TOPIC')
INDEX=os.getenv('INDEX')
ZOOKEEPER_SERVER=os.getenv('ZOOKEEPER_SERVER')
GROUP_ID=os.getenv('GROUP_ID')


args = {
    'owner': 'airflow',
    'description': 'Spark Consumer',
    'start_date': airflow.utils.dates.days_ago(1),       # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}


spark_submit_dag = DAG(
    dag_id='spark_consumer_DAG',
    default_args={
			    'owner': 'airflow',
			    'description': 'Spark Consumer',
			    'start_date': airflow.utils.dates.days_ago(1),       # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
			    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
		},
    schedule_interval='@daily',        # set interval
    catchup=False                     # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)



# submitting spark job through shell and passing necessary arguments
task1 = BashOperator(
     task_id='pyspark_consumer',
     bash_command='/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit '
                  '--master local[*] '
                  '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1,'
                  'org.elasticsearch:elasticsearch-spark-20_2.10:7.7.0 '
                  '/usr/local/airflow/dags/src/spark_consume_data/pyspark_consumer.py ',
     dag=spark_submit_dag,
        )


task1
# task2        # set task priority

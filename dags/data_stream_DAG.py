import airflow
import os
from dotenv import load_dotenv
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.stream_911_data.kafka_producer import generate_stream

load_dotenv()

API_TOKEN=os.getenv('API_TOKEN')
BOOTSTRAP_SERVER=os.getenv('BOOTSTRAP_SERVER')
TOPIC=os.getenv('TOPIC')

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='data_stream_DAG',
    default_args=args,
    schedule_interval= '@once',             # set interval
    catchup=False                          # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)


task1 = PythonOperator(
    task_id='generate_transactions_stream',
    python_callable=generate_stream,              # function to be executed
    op_kwargs={'API_TOKEN': API_TOKEN,        # input arguments
               'BOOTSTRAP_SERVER': BOOTSTRAP_SERVER,
               'TOPIC': TOPIC},
    dag=dag,
)


task1                  

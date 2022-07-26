import airflow.utils.dates
import pendulum
from airflow import DAG, macros
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from includes.comp_scraper import database_operations, execute_etl


default_args = {
  'owner': 'V.iwuoha',
  'email': 'viciwuoha@gmail.com',
  'retries': 1,
  'retry_delay': timedelta(minutes=2)
}



with DAG(dag_id='comp_scraper_pipeline',
         default_args=default_args,
         schedule_interval= "59 7 * * 2", #05 12 cron will trigger at 13:05 pm, i'm in utc+1 so 1hr backwards
         start_date = pendulum.datetime(2021, 1, 1),
         description='Weekly Price Analytics Workflow',
         catchup=False) as dag:


         extract_and_load_data = PythonOperator(
            task_id= 'Extract_and_Load_to_dwh',
            python_callable = execute_etl 
         )

         analyze_and_report_data = PythonOperator(
            task_id = 'Analyze_and_report_data',
            python_callable = database_operations
         )

extract_and_load_data >> analyze_and_report_data


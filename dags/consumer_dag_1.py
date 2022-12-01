from pendulum import datetime

from airflow.operators.empty import EmptyOperator
from airflow.models import DAG
from airflow import Dataset
from pandas import DataFrame

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table


reporting_table_dataset = Dataset("astro://snowflake_default@?table=reporting_table")

with DAG(
    dag_id="consumer_dag_1",
    start_date=datetime(2019, 1, 1),
    schedule=[reporting_table_dataset],
    catchup=False,
):

    t1 = EmptyOperator(task_id="t1")
from pendulum import datetime

from airflow.models import DAG
from airflow import Dataset
from pandas import DataFrame

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

reporting_table_dataset = Dataset("astro://snowflake_default@?table=reporting_table")

# Define constants for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_REPORTING = "reporting_table"

# Define a function for transforming tables to dataframes
@aql.dataframe
def transform_dataframe(df: DataFrame):
    amounts = df.loc[:, "amount"]
    total_amount = amounts.sum()
    print("amounts:", amounts)
    print("Total amount:", total_amount)
    return total_amount

# Basic DAG definition. Run the DAG starting January 1st, 2019 on a daily schedule.
with DAG(
    dag_id="consumer_dag_2",
    start_date=datetime(2019, 1, 1),
    schedule=[reporting_table_dataset],
    catchup=False,
):

    # Transform the reporting table into a dataframe
    sum_amounts = transform_dataframe(
        Table(
            name=SNOWFLAKE_REPORTING,
            conn_id=SNOWFLAKE_CONN_ID,
        )
    )
from airflow.decorators import dag
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable
from startup_warehouse.tasks.main import dimension_tables, fact_tables

default_args = {
    "owner": "Oscar",
    "on_failure_callback": slack_notifier
}

@dag(
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["startup"],
    description="Extract, Transform and Load Startup Investments data into Warehouse"
)

def startup_warehouse():
    incremental_mode = eval(Variable.get('startup_warehouse_incremental_mode'))
    dimension_tables(incremental=incremental_mode) >> fact_tables(incremental=incremental_mode)

startup_warehouse()
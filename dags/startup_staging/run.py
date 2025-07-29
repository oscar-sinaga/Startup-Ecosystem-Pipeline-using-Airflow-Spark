from airflow.decorators import dag
from startup_staging.tasks.startup_db import startup_db
from startup_staging.tasks.startup_api import startup_api
from startup_staging.tasks.startup_spreadsheet import startup_spreadsheet
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable

default_args = {
    "owner": "Rahil",
    "on_failure_callback": slack_notifier
}

@dag(
    dag_id="startup_staging",
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["startup"],
    description="Extract, and Load startup data into Staging Area"
)

def startup_staging_dag():
    incremental_mode = eval(Variable.get('startup_staging_incremental_mode'))
    # Run the startup_db task with the incremental flag
    db_task = startup_db(incremental=incremental_mode)
    
    # Run the startup_api task
    api_task = startup_api(incremental=incremental_mode)
    
    # Run the startup_spreadsheet task
    spreadsheet_task = startup_spreadsheet()
    
    # Set the task dependencies
    [db_task, api_task, spreadsheet_task]

startup_staging_dag()
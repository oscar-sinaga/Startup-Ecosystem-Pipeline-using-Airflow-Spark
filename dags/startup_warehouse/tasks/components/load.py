from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from datetime import timedelta
from sqlalchemy import create_engine
from pangres import upsert
from datetime import datetime
import pandas as pd
import sys
from helper.pyspark_db import CustomPysparkPostgres

class Load:
    """
    A class used to load data into the warehouse.
    """

    @staticmethod
    def _warehouse(table_name, table_pkey, incremental, date):
        """
        Load data into the warehouse.

        Args:
            table_name (str): The name of the table to load data into.
            table_pkey (str): The primary key of the table.
            incremental (bool): Flag to indicate if the process is incremental.
            date (str): The date for incremental loading.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName(f"Load to warehouse - {table_name}") \
                .getOrCreate()

            # Define bucket and object name
            transformed_data_path = 's3a://transformed-data/'
            object_name = f'/{table_name}'

            # Evaluate table_pkey if it is a list
            if table_pkey.startswith('[') and table_pkey.endswith(']'):
                table_pkey = eval(table_pkey)

            # Adjust object name for incremental loading
            if table_name not in ['people','relationships']:
                if incremental:
                    print('incremental Start')
                    object_name = f'/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            try:
                # Read data from S3
                df = spark.read.parquet(f"{transformed_data_path}{object_name}")

            
            except:
                skip_msg = f"{table_name} doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "load",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # upsert data
            CustomPysparkPostgres._upsert_dataframe(
                data=df,
                table_name=table_name,
                table_pkey=table_pkey,
                schema='public',
                connection_id='warehouse_db'
            )

            # Logging Success
            log_msg = {
                "step": "warehouse",
                "process": "load",
                "status": "success",
                "source": "staging",
                "table_name": table_name,
                "error_msg": None,
                "etl_date": current_timestamp
                }

            
        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "load",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
                }
            raise AirflowException(f"Error when loading {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: load.py <table_name> <table_pkey> <incremental> <date>")
        sys.exit(-1)

    table_name = sys.argv[1]
    table_pkey = sys.argv[2]
    incremental = sys.argv[3].lower() == 'true'
    date = sys.argv[4]

    Load._warehouse(
        table_name, 
        table_pkey,
        incremental, 
        date
    )
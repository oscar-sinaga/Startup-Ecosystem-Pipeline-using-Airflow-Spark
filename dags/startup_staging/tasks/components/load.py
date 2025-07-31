from airflow.exceptions import AirflowException, AirflowSkipException
from pyspark.sql import SparkSession
from datetime import timedelta
from pangres import upsert
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from helper.minio import MinioClient, CustomMinio
from helper.pyspark_db import CustomPysparkPostgres
from datetime import datetime
import pandas as pd
import sys
import json

class Load:
    """
    A class used to load data into the staging area from various sources such as databases, APIs, and spreadsheets.
    """

    @staticmethod
    def _startup_db(table_name, table_pkey, incremental, date):
        """
        Load data from startup database into staging area.

        Args:
            table_name (str): Name of the table to load data into.
            table_pkey (str): Primary key of the table.
            incremental (bool): Flag to indicate if the loading is incremental.
            date (str): Date string for the data to load.
        """

        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(f"Load to staging - {table_name} from database") \
            .getOrCreate()
        
        current_timestamp = datetime.now().replace(microsecond=0)

        # Define bucket and object name
        bucket_name = 'extracted-data'
        try:

            # Change string list to actual list. e.g: '[orderid]' to ['orderid']
            if table_pkey.startswith('[') and table_pkey.endswith(']'):
                print("table_pkey.startswith('[') and table_pkey.endswith(']')")
                table_pkey = eval(table_pkey)

            # Adjust object name for incremental loading
            object_name = f'/startup-db/{table_name}/*.csv'
            if incremental:
                object_name = f'/startup-db/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            try:
                # Read data from S3
                df = spark.read.options(
                    delimiter=";",
                    header=True
                ).csv(f"s3a://{bucket_name}/{object_name}")
            
            except:
                skip_msg = f"{table_name} doesn't have new data. Skipped..."
                log_msg = {
                "step": "staging",
                "process": "load",
                "status": "skip",
                "source": "db",
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
                connection_id='staging_db'
            )

            # Logging Success
            log_msg = {
                "step": "staging",
                "process": "load",
                "status": "success",
                "source": "db",
                "table_name": table_name,
                "error_msg": None,
                "etl_date": current_timestamp
                }

            
        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "staging",
                "process": "load",
                "status": "failed",
                "source": "db",
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

    @staticmethod
    def _startup_api(table_name,table_pkey,incremental,date):
        """
        Load data from startup API into staging area.

        Args:
            date (str): Date string for the data to load.
        """
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(f"Load to staging - {table_name}") \
            .getOrCreate()
        current_timestamp = datetime.now().replace(microsecond=0)
        

        try:
            bucket_name = 'extracted-data'
            try:
                if incremental:
                    print('masuk incremental')
                    # Define bucket and object name
                    date_before = (pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")
                    object_name = f'/startup-api/{table_name}-{date_before}/*.csv'

                    # Read data from S3
                    df = spark.read.options(
                        delimiter=";",
                        header=True
                    ).csv(f"s3a://{bucket_name}/{object_name}")
                    
                else:
                    # Define bucket and object name
                    object_name = f'/startup-api/{table_name}/*.csv'
                    # Read data from S3
                    df = spark.read.options(
                        delimiter=";",
                        header=True
                    ).csv(f"s3a://{bucket_name}/{object_name}")
                    print('Data berhasil di baca')
                    
            except:
                skip_msg = f"{table_name} doesn't have new data. Skipped..."
                log_msg = {
                "step": "staging",
                "process": "load",
                "status": "skip",
                "source": "api",
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
                connection_id='staging_db'
            )
            print('Data berhasil disimpan')

            # Logging Success
            log_msg = {
                "step": "staging",
                "process": "load",
                "status": "success",
                "source": "api",
                "table_name": table_name,
                "error_msg": None,
                "etl_date": current_timestamp
                }
            
        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "staging",
                "process": "load",
                "status": "failed",
                "source": "api",
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

    @staticmethod
    def _startup_spreadsheet(table_name, table_pkey):
        """
        Load data from startup spreadsheet into staging area.
        """
        bucket_name = 'extracted-data'
        object_name = f'/startup-spreadsheet/{table_name}/*.csv'
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(f"Load to staging - {table_name} from spreadsheet") \
            .getOrCreate()
        
        current_timestamp = datetime.now().replace(microsecond=0)

        try:
            # Change string list to actual list. e.g: '[orderid]' to ['orderid']
            if table_pkey.startswith('[') and table_pkey.endswith(']'):
                table_pkey = eval(table_pkey)

            try:
                # Read data from S3
                df = spark.read.options(
                    delimiter=";",
                    header=True
                ).csv(f"s3a://{bucket_name}/{object_name}")
            
            except:
                skip_msg = f"{table_name} doesn't have new data. Skipped..."
                log_msg = {
                "step": "staging",
                "process": "load",
                "status": "skip",
                "source": "spreadsheet",
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
                connection_id='staging_db'
            )

            # Logging Success
            log_msg = {
                "step": "staging",
                "process": "load",
                "status": "success",
                "source": "spreadsheet",
                "table_name": table_name,
                "error_msg": None,
                "etl_date": current_timestamp
                }

            
        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "staging",
                "process": "load",
                "status": "failed",
                "source": "spreadsheet",
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
    """
    Main entry point for the script. Loads data into startup staging based on command line arguments.

    Usage:
        python load.py <source_type> <table_name> <table_pkey> [<incremental>] [<date>]

    Examples:
        python load.py startup_db customers "customer_id" true 2025-07-11
        python load.py startup_api customer_orders_history "[customer_id, order_id]" 2025-07-11
        python load.py startup_spreadsheet people "[id]"
    """
    args = sys.argv[1:]

    def print_usage():
        print("\nUsage:")
        print("  python load.py <source_type> <table_name> <table_pkey> [<incremental>] [<date>]\n")
        print("  Examples:")
        print("  python load.py startup_db customers \"customer_id\" true 2025-07-11")
        print("  python load.py startup_api customer_orders_history \"[customer_id, order_id]\" 2025-07-11")
        print("  python load.py startup_spreadsheet people \"[id]\"")
        sys.exit(1)

    if len(args) < 3:
        print("Error: Not enough arguments.")
        print_usage()

    source_type = args[0].lower()
    table_name = args[1]
    table_pkey = args[2]

    if source_type == "startup_db":
        if len(args) != 5:
            print("Error: 'startup_db' requires 5 arguments.")
            print("source_type, table_name, table_pkey, incremental, date")
            print_usage()
        incremental = args[3].lower() == 'true'
        date = args[4]
        Load._startup_db(table_name, table_pkey, incremental, date)

    elif source_type == "startup_api":
        if len(args) != 5:
            print("Error: 'startup_api' requires 4 arguments.")
            print("source_type, table_name, table_pkey, incremental, date")
            print_usage()

        incremental = args[3].lower() == 'true'
        date = args[4]
        Load._startup_api(table_name, table_pkey,incremental, date)

    elif source_type == "startup_spreadsheet":
        if len(args) != 3:
            print("Error: 'startup_spreadsheet' requires 3 arguments.")
            print("source_type, table_name, table_pkey")
            print_usage()
        Load._startup_spreadsheet(table_name, table_pkey)

    else:
        print(f"Error: Unknown source_type '{source_type}'.")
        print("Allowed values: startup_db | startup_api | startup_spreadsheet")
        print_usage()

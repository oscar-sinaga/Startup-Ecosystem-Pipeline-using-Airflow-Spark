from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helper.pyspark_db import CustomPysparkPostgres, PysparkPostgresClient, PysparkApiHelper
from datetime import timedelta, datetime
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

import pandas as pd
import requests
import gspread
import traceback
import sys

BASE_PATH = "/opt/airflow/dags"


class Extract:
    @staticmethod
    def _startup_db(table_name, incremental, date):
        spark = SparkSession.builder.appName(f"Extract from database - {table_name}").getOrCreate()
        current_timestamp = datetime.now().replace(microsecond=0)

        try:
            query = f"(SELECT * FROM {table_name}) as data"
            object_name = f'/startup-db/{table_name}'
            bucket_name = 'extracted-data'

            if eval(incremental):
                print(f'incremental dijalankan :{incremental}')
                query = f"(SELECT * FROM {table_name} WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                object_name = f'/startup-db/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            DB_URL, DB_USER, DB_PASS = PysparkPostgresClient._get_conn_config('startup_investments_db')

            df = spark.read.jdbc(
                url=DB_URL,
                table=query,
                properties={"user": DB_USER, "password": DB_PASS, "driver": "org.postgresql.Driver"}
            )

            if df.isEmpty():
                skip_msg = f"{table_name} doesn't have new data. Skipped..."
                log_msg = {
                "step": "staging",
                "process": "extraction",
                "status": "skip",
                "source": "db",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            for col_name, col_type in df.dtypes:
                if col_type == 'string':
                    df = df.withColumn(col_name, regexp_replace(col(col_name), '\n', ' '))

            df.write \
                .format("csv") \
                .option("header", "true") \
                .option("delimiter", ";") \
                .mode("overwrite") \
                .save(f"s3a://{bucket_name}/{object_name}")
            
            log_msg = {
                "step": "staging",
                "process": "extraction",
                "status": "success",
                "source": "db",
                "table_name": table_name,
                "error_msg": None,
                "etl_date": current_timestamp
            }

            print(f"Successfully extracted {table_name}  from database")


        except Exception:
            error_msg = traceback.format_exc()
            log_msg = {
                "step": "staging",
                "process": "extraction",
                "status": "failed",
                "source": "db",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _startup_api(table_name: str,incremental, date):
        spark = SparkSession.builder.appName(f"Extract from api - {table_name}").getOrCreate()
        current_timestamp = datetime.now().replace(microsecond=0)

        try:
            if eval(incremental):
                response = requests.get(
                    url=Variable.get('startup_api_url'),
                    params={"start_date": date, "end_date": date},
                )

                if response.status_code != 200:
                    raise AirflowException(f"Failed to fetch data from API. Status code: {response.status_code}")

                json_data = response.json()

                if not json_data:
                    skip_msg = f"No new data in API {table_name}. Skipped..."
                    log_msg = {
                        "step": "staging",
                        "process": "extraction",
                        "status": "skip",
                        "source": "api",
                        "table_name": table_name,
                        "error_msg": skip_msg,
                        "etl_date": current_timestamp
                    }
                    print(skip_msg)
                    return 

                json_data = PysparkApiHelper.replace_newlines(json_data)
                df = spark.createDataFrame(json_data)

                object_name = f'/startup-api/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'
                
                bucket_name = 'extracted-data'
                df.write \
                    .format("json") \
                    .mode("overwrite") \
                    .save(f"s3a://{bucket_name}/{object_name}")
                
            else:
                df = PysparkApiHelper.extract_all_data_api(spark, Variable.get('startup_api_url'))
                object_name = f'/startup-api/{table_name}'
                bucket_name = 'extracted-data'

                df.write \
                .format("csv") \
                .option("header", "true") \
                .option("delimiter", ",") \
                .mode("overwrite") \
                .save(f"s3a://{bucket_name}/{object_name}")


            log_msg = {
                    "step": "staging",
                    "process": "extraction",
                    "status": "success",
                    "source": "api",
                    "table_name": table_name,
                    "error_msg": None,
                    "etl_date": current_timestamp
                }
            
        except Exception as e:
            log_msg = {
                "step": "staging",
                "process": "extraction",
                "status": "failed",
                "source": "api",
                "table_name": table_name,
                "error_msg": str(e),
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting startup API {table_name}: {e}")

    
        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _startup_spreadsheet(table_name: str):
        spark = SparkSession.builder.appName(f"Extract from spreadsheet - {table_name}").getOrCreate()
        current_timestamp = datetime.now().replace(microsecond=0)

        try:
            hook = GoogleBaseHook(gcp_conn_id="startup_analytics")
            credentials = hook.get_credentials()
            google_credentials = gspread.Client(auth=credentials)

            table_name = table_name.lower()
            sheet = google_credentials.open(table_name)
            worksheet = sheet.worksheet(table_name)

            records = worksheet.get_all_records()
            if not records:
                skip_msg = f"No data in {table_name} Spreadsheets. Skipped..."
                log_msg = {
                "step": "staging",
                "process": "extraction",
                "status": "skipped",
                "source": "spreadsheet",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return 

            df = spark.createDataFrame(records)

            object_name = f'/startup-spreadsheet/{table_name}'
            bucket_name = 'extracted-data'
            df.write \
                .format("csv") \
                .option("header", "true") \
                .option("delimiter", ";") \
                .mode("overwrite") \
                .save(f"s3a://{bucket_name}/{object_name}")

            log_msg = {
                "step": "staging",
                "process": "extraction",
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
            "process": "extraction",
            "status": "failed",
            "source": "spreadsheet",
            "table_name": table_name,
            "error_msg": str(e),
            "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting {table_name} Spreadsheets: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as error_msg:
                raise AirflowException(f"Error when logging {table_name}: {str(error_msg)}")
            finally:
                spark.stop()



if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage:")
        print("  For database:     python extract.py db <table_name> <incremental: true|false> <date: YYYY-MM-DD>")
        print("  For API:          python extract.py api <table_name> <incremental: true|false> <date: YYYY-MM-DD>")
        print("  For spreadsheet:  python extract.py spreadsheet <table_name>")
        sys.exit(1)

    source = sys.argv[1].lower()
    table_name = sys.argv[2]

    if source == "startup_db":
        if len(sys.argv) != 5:
            print("Error: 'db' source requires <table_name> <incremental: true|false> <date>")
            sys.exit(1)

        incremental = sys.argv[3]
        date = sys.argv[4]
        Extract._startup_db(table_name, incremental, date)

    elif source == "startup_api":
        if len(sys.argv) != 5:
            print("Error: 'api' source requires <table_name> <incremental: true|false> <date>")
            sys.exit(1)

        incremental = sys.argv[3]
        date = sys.argv[4]
        Extract._startup_api(table_name,incremental, date)

    elif source == "startup_spreadsheet":
        if len(sys.argv) != 3:
            print("Error: 'spreadsheet' source requires <table_name>")
            sys.exit(1)

        Extract._startup_spreadsheet(table_name)

    else:
        print(f"Unknown source '{source}'. Must be one of: db, api, spreadsheet.")
        sys.exit(1)

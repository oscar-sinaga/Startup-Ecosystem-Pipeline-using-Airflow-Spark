from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import pandas as pd
from airflow.exceptions import AirflowException
from helper.pyspark_db import CustomPysparkPostgres
import sys
from helper.minio import CustomMinio

class Validation:
    @staticmethod
    def _data_validations(incremental, table_name, tabel_pkey: str, date_cols: str = None, date = None):
        current_timestamp = datetime.now().replace(microsecond=0)
        invalid_bucket = 'invalid-data'

        spark = SparkSession.builder \
            .appName(f"Validate {table_name}") \
            .getOrCreate()
        
        try:
            if incremental and table_name not in ['people','relationships']:
                print('incremental Start')
                table_file_name = f'{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'
            else:
                table_file_name = f'{table_name}'
            print(table_file_name)
            data = spark.read.parquet(f's3a://transformed-data/{table_file_name}')

        except Exception:
            skip_msg = f"{table_name} doesn't have new data. Skipped..."
            log_msg = {
                "step": "staging",
                "process": "validation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
            }
            print(skip_msg)
            return

        try:
            if tabel_pkey.startswith('[') and tabel_pkey.endswith(']'):
                tabel_pkey = eval(tabel_pkey)

            if date_cols.startswith('[') and date_cols.endswith(']'):
                print(date_cols)
                print(type(date_cols))
                date_cols = eval(date_cols)
            else:
                date_cols = None
            
            columns_to_check = [c for c in data.columns if c not in (tabel_pkey if isinstance(tabel_pkey, list) else [tabel_pkey])]
            print(f"Validating columns: {columns_to_check}")
            report = {}

            for colname in columns_to_check:
                if isinstance(date_cols, list) and colname in date_cols:
                    df_missing = data.filter(col(colname) == 21000101)
                else:
                    df_missing = data.filter(col(colname).isNull())
                    
                num_missing = df_missing.count()
                pkey_cols = tabel_pkey if isinstance(tabel_pkey, list) else [tabel_pkey]

                missing_ids = df_missing.select(*pkey_cols).dropna().collect()
                list_missing_ids = [{col: row[col] for col in pkey_cols} for row in missing_ids]

                report[colname] = {
                    f"num_missing_{colname}": num_missing,
                    f"missing_rows": list_missing_ids
                }


            CustomMinio._put_json(report, invalid_bucket, f'{table_file_name}-validation')

            log_msg = {
                "step": "staging",
                "process": "validation",
                "status": "success",
                "source": "staging",
                "table_name": table_name,
                "error_msg": None,
                "etl_date": current_timestamp
            }

        except Exception as e:
            log_msg = {
                "step": "staging",
                "process": "validation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": str(e),
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when validating {table_name}: {e}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark, log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()



if __name__ == "__main__":
    """
    Main entry point for the script.
    Usage:
        python validations.py <incremental> <table_name> <tabel_pkey> <date_cols> <date>
    Example:
        python validations.py true company id '[created_at]' 2024-07-01
    """
    if len(sys.argv) != 6:
        print("Usage: validations.py <incremental> <table_name> <tabel_pkey> <date_cols> <date>")
        sys.exit(1)

    incremental = sys.argv[1].lower() == 'true'
    table_name = sys.argv[2]
    tabel_pkey = sys.argv[3]
    date_cols = sys.argv[4]
    date = sys.argv[5]

    Validation._data_validations(
        incremental=incremental,
        table_name=table_name,
        tabel_pkey=tabel_pkey,
        date_cols=date_cols,
        date=date
    )

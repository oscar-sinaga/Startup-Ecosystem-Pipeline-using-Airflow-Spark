from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession,  DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd

class PysparkPostgresClient(PostgresHook):
    @staticmethod
    def _get_conn_config(connection_id, is_connect=False):
        hook = PostgresHook(postgres_conn_id=connection_id)
        conn = hook.get_connection(connection_id)
        DB_URL = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        
        if is_connect:
            # ✅ pakai hook.get_conn(), bukan airflow_conn.get_conn()
            return DB_URL, conn.login, conn.password, hook.get_conn()

        return DB_URL, conn.login, conn.password


class CustomPysparkPostgres(PostgresHook):

    @staticmethod
    def _insert_log(spark,log_msg,connection_id="log_db",table_name="etl_log"):
        try:
            DB_URL, DB_USER, DB_PASS = PysparkPostgresClient._get_conn_config(connection_id)

            # set config
            connection_properties = {
                "user": DB_USER,
                "password": DB_PASS,
                "driver": "org.postgresql.Driver" # set driver postgres
            }

            schema = StructType([
                    StructField("step", StringType(), True),
                    StructField("process", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("table_name", StringType(), True),
                    StructField("error_msg", StringType(), True),
                    StructField("etl_date", TimestampType(), True) # atau TimestampType() jika kamu sudah yakin formatnya timestamp

                ])

            log_msg = [(log_msg['step'], 
                        log_msg['process'], 
                        log_msg['status'], 
                        log_msg['source'], 
                        log_msg['table_name'], 
                        log_msg['error_msg'], 
                        log_msg['etl_date'])]
            # Cast schema sesuai dengan struktur PostgreSQL
            log_msg = spark.createDataFrame(log_msg, schema)

            log_msg.write.jdbc(url = DB_URL,
                        table = table_name,
                        mode = "append",
                        properties = connection_properties)
            
        except Exception as e:
            print("Can't save your log message. Cause: ", str(e))
            raise  # ⬅️ ini yang bikin Airflow tahu kalau insert log-nya gagal

    @staticmethod
    def _upsert_dataframe(data, schema: str, table_name: str, table_pkey: str, connection_id: str):
        # Setup koneksi PostgreSQL
        DB_URL, DB_USER, DB_PASS, conn = PysparkPostgresClient._get_conn_config(connection_id, is_connect=True)

        connection_properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver"
        }

        # Ubah table_pkey ke list jika berupa string tunggal
        if isinstance(table_pkey, str):
            if table_pkey.startswith('[') and table_pkey.endswith(']'):
                table_pkey = eval(table_pkey)
            else:
                table_pkey = [table_pkey]  # ubah jadi list tunggal

        # Buat nama temporary table (unik)
        temp_table = f"temp_upsert_{table_name}"

        # Step 1: Simpan ke temporary table di PostgreSQL
        data.write.jdbc(
            url=DB_URL,
            table=f"{schema}.{temp_table}",
            mode="overwrite",
            properties=connection_properties
        )

        columns = data.columns
        columns_str = ", ".join(columns)
        update_str = ", ".join([
            f"{col}=EXCLUDED.{col}" for col in columns if col not in table_pkey
        ])

        conflict_str =  ", ".join(table_pkey)

        upsert_sql = f"""
        INSERT INTO {schema}.{table_name} ({columns_str})
        SELECT {columns_str} FROM {schema}.{temp_table}
        ON CONFLICT ({conflict_str})
        DO UPDATE SET {update_str};
        """

        # print(upsert_sql)
        # print(f'table_key length = {len(table_pkey)}')
        # print(f'table_key = {table_pkey}')

        try:
            with conn.cursor() as cur:
                # ✅ Step tambahan: cek apakah table target ada
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema, table_name))
                exists = cur.fetchone()[0]

                # ✅ Jika tidak ada, buat table dengan skema dari Spark DataFrame
                if not exists:
                    print(f"Table {schema}.{table_name} not found. Creating...")
                    data.limit(0).write.jdbc(
                        url=DB_URL,
                        table=f"{schema}.{table_name}",
                        mode="overwrite",
                        properties=connection_properties
                    )

                    pk_str = ", ".join(table_pkey)

                    alter_sql = f"""
                    ALTER TABLE {schema}.{table_name}
                    ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_str});
                    """
                    try:
                        cur.execute(alter_sql)
                        conn.commit()
                        print(f"Added primary key constraint on {schema}.{table_name}")
                    except Exception as e:
                        conn.rollback()
                        raise Exception(f"Failed to add primary key on {schema}.{table_name}. Error: {e}")

                # Jalankan UPSERT
                cur.execute(upsert_sql)
                conn.commit()

        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to UPSERT data into {schema}.{table_name}. Error: {e}")


        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {schema}.{temp_table};")
                    conn.commit()
            except Exception as drop_err:
                print(f"Failed to drop temp table: {drop_err}")
            finally:
                conn.close()
class PysparkApiHelper:

    @staticmethod
    def replace_problem_chars(obj):
        if isinstance(obj, dict):
            return {k: PysparkApiHelper.replace_problem_chars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [PysparkApiHelper.replace_problem_chars(elem) for elem in obj]
        elif isinstance(obj, str):
            return obj.replace('\n', ' ').replace('\r', '').replace('"', "'").strip()
        return obj

    @staticmethod
    def extract_api_spark(spark: SparkSession, link_api: str, list_parameter: dict):
        try:
            # Call the API
            resp = requests.get(link_api, params=list_parameter)
            raw_response = resp.json()

            raw_response = PysparkApiHelper.replace_problem_chars(raw_response)

            # Jika data kosong, kembalikan DataFrame kosong dengan schema dummy
            if not raw_response:
                # Misal: buat schema kosong tapi tetap sesuai dengan struktur yang diharapkan
                empty_schema = StructType([
                    StructField("created_at", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("milestone_at", StringType(), True),
                    StructField("milestone_code", StringType(), True),
                    StructField("milestone_id", StringType(), True),
                    StructField("object_id", StringType(), True),
                    StructField("source_description", StringType(), True),
                    StructField("source_url", StringType(), True),
                    StructField("updated_at", StringType(), True),

                ])
                return spark.createDataFrame([], schema=empty_schema)

            # Convert ke Pandas DataFrame
            df = pd.DataFrame(raw_response)

            # Replace empty or NaN values with Python None
            df = df.replace([np.nan, ''], None)

            # Convert all columns to string (Spark expects uniform types)
            df = df.astype(str)

            # Create Spark DataFrame
            df_spark = spark.createDataFrame(df)

            # Replace string 'nan' or 'None' (if any got through) with null
            for col_name in df_spark.columns:
                df_spark = df_spark.withColumn(
                    col_name,
                    when((col(col_name) == 'nan') | (col(col_name) == 'None'), None).otherwise(col(col_name))
                )

            return df_spark

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the API request: {e}")
        except ValueError as e:
            print(f"An error occurred while parsing the response JSON: {e}")

    @staticmethod
    def extract_all_data_api(spark: SparkSession, link_api: str) -> DataFrame:
        df_milestones = None

        start_date = "2008-06-18"
        current_date = "2013-12-12"
        # Konversi ke string jika bukan string
        if not isinstance(start_date, str):
            start_date =  start_date.strftime("%Y-%m-%d")
        if not isinstance(current_date, str):
            current_date = current_date.strftime("%Y-%m-%d")

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(current_date, "%Y-%m-%d")

        def append_and_dedup(base_df: DataFrame, new_df: DataFrame) -> DataFrame:
            if new_df is None or new_df.rdd.isEmpty():
                return base_df
            if base_df is None:
                return new_df.dropDuplicates()
            return base_df.union(new_df).dropDuplicates()

        # Jika hanya 1 hari
        if start == end:
            list_parameter = {
                "start_date": start_date,
                "end_date": (end + timedelta(days=1)).strftime("%Y-%m-%d")
            }
            df_backfilling = PysparkApiHelper.extract_api_spark(spark, link_api, list_parameter)
            return df_backfilling.dropDuplicates()

        # Jika <= 1 tahun
        if end - start <= timedelta(days=365):
            list_parameter = {
                "start_date": start_date,
                "end_date": current_date
            }
            df_backfilling = PysparkApiHelper.extract_api_spark(spark, link_api, list_parameter)
            return df_backfilling.dropDuplicates()

        # Jika > 1 tahun
        while start + relativedelta(years=1) < end:
            temp_end = start + relativedelta(years=1)
            list_parameter = {
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": temp_end.strftime("%Y-%m-%d")
            }
            df_backfilling = PysparkApiHelper.extract_api_spark(spark, link_api, list_parameter)
            df_milestones = append_and_dedup(df_milestones, df_backfilling)
            start = temp_end

        # Sisa data terakhir
        list_parameter = {
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": current_date
        }
        df_backfilling = PysparkApiHelper.extract_api_spark(spark, link_api, list_parameter)
        df_milestones = append_and_dedup(df_milestones, df_backfilling)

        return df_milestones
    
    # @staticmethod
    # def _upsert_dataframe(spark: SparkSession, data, schema: str, table_name: str, table_pkey: str,connection_id):
    #     # Setup koneksi PostgreSQL
    #     DB_URL, DB_USER, DB_PASS, _ = PysparkPostgresClient._get_conn_config(connection_id)
    #     full_table = f"{schema}.{table_name}"
    #     connection_properties = {
    #         "user": DB_USER,
    #         "password": DB_PASS,
    #         "driver": "org.postgresql.Driver"
    #     }

    #     # Step 1: Load data lama dari PostgreSQL
    #     try:
    #         existing_df = spark.read.jdbc(
    #             url=DB_URL,
    #             table=full_table,
    #             properties=connection_properties
    #         )
    #     except:
    #         existing_df = None  # Tabel mungkin belum ada

    #     # Step 2: Gabungkan data baru dengan data lama
    #     if existing_df:
    #         # Hapus data yang sudah ada berdasarkan table_pkey
    #         non_matching_df = data.join(existing_df, on=table_pkey, how="left_anti")  # Data yang tidak ada di existing_df
    #     else:
    #         non_matching_df = data  # Jika existing_df kosong, gunakan semua data baru

    #     # Step 3: Menambahkan data baru ke PostgreSQL
    #     non_matching_df.write.jdbc(
    #         url=DB_URL,
    #         table=full_table,
    #         mode="append",  # Append data baru ke tabel
    #         properties=connection_properties
    #     )

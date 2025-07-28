from airflow.decorators import task_group
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Constants
DATE = '{{ ds }}'

# Define the list of JAR files required for Spark
jar_list = [
    '/opt/spark/jars/hadoop-aws-3.3.1.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
    '/opt/spark/jars/postgresql-42.2.23.jar'
]

# Define Spark configuration
spark_conf = {
    'spark.hadoop.fs.s3a.access.key': 'minio',
    'spark.hadoop.fs.s3a.secret.key': 'minio123',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.maxExecutors': '2',
    'spark.dynamicAllocation.minExecutors': '1',
    'spark.dynamicAllocation.initialExecutors': '1',
    'spark.executor.memory': '1g',
    'spark.executor.cores': '1',
    'spark.scheduler.mode': 'FAIR',

    # --- TAMBAHKAN KONFIGURASI CLEANUP DI SINI ---
    'spark.worker.cleanup.enabled': 'true',
    'spark.worker.cleanup.interval': '1800',  # Cek setiap 30 menit
    'spark.worker.cleanup.appDataTtl': '3600'   # Hapus data aplikasi yang lebih tua dari 1 jam
}

@task_group
def startup_db(incremental):
    """
    Task group to handle the extraction and loading of startup database data.

    Args:
        incremental (bool): Flag to indicate if the process is incremental.
    """

    @task_group
    def extract():
        """
        Task group to handle the extraction of data from startup database.
        """
        # Get the list of tables to extract from Airflow Variable
        table_to_extract = eval(Variable.get('list_startup_table_db'))
        source = 'startup_db'

        # Create a SparkSubmitOperator for each table to extract
        for table_name in table_to_extract:
            SparkSubmitOperator(
                task_id=table_name,
                conn_id="spark-conn",
                application="dags/startup_staging/tasks/components/extract.py",
                application_args=[
                    f'{source}',
                    f'{table_name}',
                    f'{incremental}',
                    f'{DATE}'
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed'
            )

    @task_group
    def load():
        """
        Task group to handle the loading of data into startup database.
        """
        # Get the list of tables to load and their primary keys from Airflow Variable
        table_to_load = eval(Variable.get('list_startup_table_db'))
        print(table_to_load)
        table_pkey = eval(Variable.get('pkey_startup_table_db'))
        print(table_pkey)
        previous_task = None

        # Create a SparkSubmitOperator for each table to load
        for table_name in table_to_load:
            current_task = SparkSubmitOperator(
                task_id=table_name,
                conn_id="spark-conn",
                application="dags/startup_staging/tasks/components/load.py",
                application_args=[
                    'startup_db',
                    table_name,
                    str(table_pkey[table_name]),
                    str(incremental),
                    '{{ ds }}'
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed'
            )

            # Set task dependencies
            if previous_task:
                previous_task >> current_task

            previous_task = current_task

    # Define the task dependencies between extract and load task groups
    extract() >> load()
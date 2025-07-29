from airflow.decorators import task_group
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import pytz

# Constants
DATE = '{{ ds }}'

# Define the list of JAR files required for Spark
jar_list = [
    '/opt/spark/jars/hadoop-aws-3.3.1.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
    '/opt/spark/jars/postgresql-42.2.23.jar'
]

# Define Spark configuration
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
def dimension_tables(incremental):
    """
    Step 1 of the ETL process: Extract, Transform, Validate, and Load data for categories, customers, and customers_history.
    """

    @task_group
    def extract_transform():
        """
        Extract and transform data for categories, customers, and customers_history.
        """

        tables_and_dependencies = {
            'dim_company': "startup_db.load.company",
            'dim_people': "startup_spreadsheet.load.people",
        }

        
        # Define a function to specify the execution date to monitor
        
        for table_name, dependency in tables_and_dependencies.items():
            wait_for_staging = ExternalTaskSensor(
                task_id=f'wait_for_staging_{table_name.replace('dim_','')}',
                external_dag_id='startup_staging',
                external_task_id=dependency,
                poke_interval=10
                )
            
            spark_job = SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/startup_warehouse/tasks/components/extract_transform.py',
                application_args=[
                    f'{table_name}',
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )
        

            wait_for_staging >> spark_job

    @task_group
    def validations():
        """
        Validate data for categories, customers, and customers_history.
        """
        validation_tasks = {
                            'dim_company': 'company_nk',
                            'dim_people':'people_nk'
                            }
        date_cols = {
                    'dim_company': None,
                    'dim_people':None
        }

        for table_name,table_pkey in validation_tasks.items():
            SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/startup_warehouse/tasks/components/validations.py',
                application_args=[
                    f'{incremental}',
                    f'{table_name}',
                    f'{table_pkey}',
                    f'{date_cols[table_name]}',
                    f'{DATE}'
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

    @task_group
    def load():
        """
        Load data for categories, customers, and customers_history into the warehouse.
        """
        load_tasks =  {
                    'dim_company': 'company_nk',
                    'dim_people':'people_nk'
                    }
        
        for table_name,table_pkey in load_tasks.items():
            SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/startup_warehouse/tasks/components/load.py',
                application_args=[
                    f'{table_name}',
                    f'{table_pkey}',
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

    # Define the order of execution for step 1
    extract_transform() >> validations() >> load()

@task_group
def fact_tables(incremental):
    """
    Step 1 of the ETL process: Extract, Transform, Validate, and Load data for categories, customers, and customers_history.
    """

    @task_group
    def extract_transform():
        tables_and_dependencies = {
            'fact_relationships': "startup_spreadsheet.load.relationships",
            'fact_acquisition': "startup_db.load.acquisition",
            'fact_funds': "startup_db.load.funds",
            'fact_ipos': "startup_db.load.ipos",
            'fact_milestones': "startup_api.load.milestones",
            'temp_investments': "startup_db.load.investments",
            'temp_funding_rounds': "startup_db.load.funding_rounds",

        }

        tables_temp = ['temp_investments', 'temp_funding_rounds']
        table_name_combine = "fact_investment_round_participation"

        temp_spark_job = []

        
        # Define a function to specify the execution date to monitor
        
        for table_name, dependency in tables_and_dependencies.items():
            wait_for_staging = ExternalTaskSensor(
                task_id=f'wait_for_staging_{table_name.replace('fact_','').replace('temp_','')}',
                external_dag_id='startup_staging',
                external_task_id=dependency,
                poke_interval=10
                )
            
            spark_job = SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/startup_warehouse/tasks/components/extract_transform.py',
                application_args=[
                    f'{table_name}',
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='all_success',  # <- untuk memastikan semua selesai
            )

            if table_name  in tables_temp:
                temp_spark_job.append(spark_job)

            wait_for_staging >> spark_job

        spark_job_combine = SparkSubmitOperator(
                task_id=f'{table_name_combine}',
                conn_id='spark-conn',
                application=f'dags/startup_warehouse/tasks/components/extract_transform.py',
                application_args=[
                    f'{table_name_combine}',
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )
        
        for spark_job in temp_spark_job:
            spark_job >> spark_job_combine
                    

    @task_group
    def validations():
        """
        Validate data for categories, customers, and customers_history.
        """
        validation_tasks = {
                            'fact_relationships': "relationship_nk",
                            'fact_acquisition': "acquisition_nk",
                            'fact_funds': "fund_nk",
                            'fact_ipos': "ipo_nk",
                            'fact_milestones': "milestone_nk",
                            'fact_investment_round_participation': "['investment_nk', 'funding_round_nk']",
                            }
        
        date_cols = {
                    'fact_relationships': "['end_at', 'start_at']",
                    'fact_acquisition': "['acquired_at']",
                    'fact_funds': "['funded_at']",
                    'fact_ipos': "['public_at']",
                    'fact_milestones': "['milestone_at']",
                    'fact_investment_round_participation': "['funded_at']",
                    }
        
        

        for table_name,table_pkey in validation_tasks.items():
            SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/startup_warehouse/tasks/components/validations.py',
                application_args=[
                    f'{incremental}',
                    f'{table_name}',
                    f'{table_pkey}',
                    f'{date_cols[table_name]}',
                    f'{DATE}'
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

    @task_group
    def load():
        """
        Load data for categories, customers, and customers_history into the warehouse.
        """
        load_tasks = {
                        'fact_relationships': "relationship_nk",
                        'fact_acquisition': "acquisition_nk",
                        'fact_funds': "fund_nk",
                        'fact_ipos': "ipo_nk",
                        'fact_milestones': "milestone_nk",
                        'fact_investment_round_participation': "['investment_nk', 'funding_round_nk']",
                        }
        
        for table_name,table_pkey in load_tasks.items():
            SparkSubmitOperator(
                task_id=f'{table_name}',
                conn_id='spark-conn',
                application=f'dags/startup_warehouse/tasks/components/load.py',
                application_args=[
                    f'{table_name}',
                    f'{table_pkey}',
                    f'{incremental}',
                    DATE
                ],
                conf=spark_conf,
                jars=','.join(jar_list),
                trigger_rule='none_failed',
            )

    # Define the order of execution for step 1
    extract_transform() >> validations() >> load()
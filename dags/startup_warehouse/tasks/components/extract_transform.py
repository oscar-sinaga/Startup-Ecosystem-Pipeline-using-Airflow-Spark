from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from helper.pyspark_db import CustomPysparkPostgres, PysparkPostgresClient
import sys
from datetime import datetime
from pyspark.sql.functions import col, lit, when,concat_ws, date_format, to_timestamp, to_date, coalesce
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType, FloatType
from datetime import datetime, timedelta
import pandas as pd
# Define paths for transformed, valid, and invalid data
transformed_data_path = 's3a://transformed-data/'
valid_data_path = 's3a://valid-data/'
invalid_data_path = 's3a://invalid-data/'

# Define PostgreSQL connection properties
DB_URL_STAGING, DB_USER_STAGING, DB_PASS_STAGING = PysparkPostgresClient._get_conn_config('staging_db')
DB_URL_WAREHOUSE, DB_USER_WAREHOUSE, DB_PASS_WAREHOUSE = PysparkPostgresClient._get_conn_config('warehouse_db')


class ExtractTransform:
    """
    A class used to extract and transform data from the staging database to the warehouse.
    """

    @staticmethod
    def _dim_company(incremental, date):
        """
        Extract and transform data from the company table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "company"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - Company") \
                .getOrCreate()

            # Define query for extracting data
            query = "(SELECT * FROM public.company) as data"
            transformed_table_name = f"dim_{table_name}"
            transformed_table_name_file = f"{transformed_table_name}"
            if incremental:
                print('incremental Start')
                query = f"(SELECT * FROM public.company WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                transformed_table_name_file = f"{transformed_table_name_file}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"Company doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Select columns
            columns_to_picked = [
                'object_id', 
                'description',
                'region',
                'city',
                'state_code',
                'country_code',
                'latitude',
                'longitude',
                # 'created_at',
                # 'updated_at'
            ]
            df = df.select(*columns_to_picked)

            # Rename column: object_id -> company_nk
            df = df.withColumnRenamed('object_id', 'company_nk')

            # Fill null values (set default values)
            # data = data.withColumn('description', when(col('description').isNull(), lit('No Description')).otherwise(col('description')))
            # data = data.withColumn('region', when(col('region').isNull(), lit('Unknown')).otherwise(col('region')))
            df = df.withColumn('city', when(col('city').isNull(), lit('Unknown')).otherwise(col('city')))
            df = df.withColumn('state_code', when(col('state_code').isNull(), lit('N/A')).otherwise(col('state_code')))
            # data = data.withColumn('country_code', when(col('country_code').isNull(), lit('N/A')).otherwise(col('country_code')))
            # data = data.withColumn('latitude', when(col('latitude').isNull(), lit(999)).otherwise(col('latitude')))
            # data = data.withColumn('longitude', when(col('longitude').isNull(), lit(999)).otherwise(col('longitude')))

            # Cast column
            df = df.withColumn('latitude', col('latitude').cast(FloatType()))
            df = df.withColumn('longitude', col('longitude').cast(FloatType()))
            

            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to be {transformed_table_name}  from staging")


        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _dim_people(incremental, date):
        """
        Extract and transform data from the people table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "people"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - people") \
                .getOrCreate()

            # Define query for extracting data
            query = "(SELECT * FROM public.people) as data"
            transformed_table_name = f"dim_{table_name}"
            transformed_table_name_file = f"{transformed_table_name}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"people doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Select necessary columns
            columns_to_picked = [
                'object_id', 
                'first_name',
                'last_name',
                'affiliation_name',
                'birthplace'
                ]
            df = df.select(*columns_to_picked)

            # Derived column: full_name
            df = df.withColumn('full_name', concat_ws(' ', col('first_name'), col('last_name')))

            # Rename columns
            df = df.withColumnRenamed('object_id', 'people_nk') \
                        .withColumnRenamed('affiliation_name', 'affiliation')

            # Deduplicate based on people_nk
            df = df.dropDuplicates(['people_nk'])

            # # Fill nulls
            # df = df.withColumn('affiliation', when(col('affiliation').isNull(), lit('Unknown')).otherwise(col('affiliation')))
            # df = df.withColumn('birthplace', when(col('birthplace').isNull(), lit('Unknown')).otherwise(col('birthplace')))
            
            
            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to be {transformed_table_name}  from staging")


        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()
                
    @staticmethod
    def _fact_relationships(incremental, date):
        """
        Extract and transform data from the relationships table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "relationships"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - relationships") \
                .getOrCreate()

            # Define query for extracting data
            query = "(SELECT * FROM public.relationships) as data"
            transformed_table_name = f"fact_{table_name}"
            transformed_table_name_file = f"{transformed_table_name}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"relationships doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            columns_to_picked = [
                'relationship_id', 
                'person_object_id',
                'relationship_object_id',
                'title',
                'start_at',
                'end_at',
                'is_past',
                'sequence',
            ]

            df = df.select(*columns_to_picked)

            # Rename columns
            df = df \
                .withColumnRenamed("relationship_id", "relationship_nk") \
                .withColumnRenamed("person_object_id", "people_nk") \
                .withColumnRenamed("relationship_object_id", "company_nk") \
                .withColumnRenamed("is_past", "relationship_status") \
                .withColumnRenamed("sequence", "relationship_order")

            # Cast column
            df = df.withColumn('relationship_nk', col('relationship_nk').cast(LongType()))
            df = df.withColumn('relationship_status', col('relationship_status').cast(StringType()))
            df = df.withColumn('relationship_order', col('relationship_order').cast(IntegerType()))


            df = df.withColumn(
                'relationship_status',
                when(col('relationship_status').isNull(), lit('Unknown'))
                .when(col('relationship_status') == 'TRUE', 'Past')
                .when(col('relationship_status') == 'FALSE', 'Current')
                .otherwise(col('relationship_status'))  # selain kondisi di atas, tetap isi awalnya
            )
                

            # # Fill NULLs
            # df = df.withColumn("title", when(col("title").isNull(), lit("Unknown")).otherwise(col("title")))
            # df = df.withColumn("relationship_status", when(col("relationship_status").isNull(), lit("Unknown")).otherwise(col("relationship_status")))
            # df = df.withColumn("relationship_order", when(col("relationship_order").isNull(), lit(0)).otherwise(col("relationship_order")))
            
            # Convert start_at and end_at to int date format (yyyyMMdd), null jadi 21000101
            for col_name in ['start_at', 'end_at']:
                df = df.withColumn(
                    col_name,
                    date_format(
                        to_timestamp(col(col_name), 'yyyy-MM-dd HH:mm:ss.SSS'),
                        'yyyyMMdd'
                    ).cast(IntegerType())
                )
                df = df.withColumn(col_name, when(col(col_name).isNull(), lit(21000101)).otherwise(col(col_name)))

            # Convert NK to ID by joining with dim_people
            # Extract df from the `categories` table
            dim_people = spark.read.jdbc(
                url=DB_URL_WAREHOUSE,
                table="(SELECT * FROM dim_people) as data",
                properties={"user": DB_USER_WAREHOUSE, "password": DB_PASS_WAREHOUSE, "driver": "org.postgresql.Driver"}
            )

            df = df.join(dim_people.select("people_nk", "people_id"), on="people_nk", how="left")


            # Join with dim_company
            dim_company = spark.read.jdbc(
                url=DB_URL_WAREHOUSE,
                table="(SELECT * FROM dim_company) as data",
                properties={"user": DB_USER_WAREHOUSE, "password": DB_PASS_WAREHOUSE, "driver": "org.postgresql.Driver"}
            )

            df = df.join(dim_company.select("company_nk", "company_id"), on="company_nk", how="left")
            

            # Drop NK columns
            df = df.drop("people_nk", "company_nk")

            # Cast column
            df = df.withColumn('relationship_nk', col('relationship_nk').cast(LongType()))
            df = df.withColumn('people_id', col('people_id').cast(LongType()))
            df = df.withColumn('company_id', col('company_id').cast(LongType()))
            df = df.withColumn('start_at', col('start_at').cast(IntegerType()))
            df = df.withColumn('end_at', col('end_at').cast(IntegerType()))
            df = df.withColumn('relationship_order', col('relationship_order').cast(IntegerType()))

            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to be {transformed_table_name}  from staging")


        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _fact_acquisition(incremental, date):
        """
        Extract and transform data from the acquisition table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "acquisition"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - acquisition") \
                .getOrCreate()

            # Define query for extracting data
            query = "(SELECT * FROM public.acquisition) as data"
            transformed_table_name = f"fact_{table_name}"
            transformed_table_name_file = f"{transformed_table_name}"
            if incremental:
                query = f"(SELECT * FROM public.acquisition WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                transformed_table_name_file = f"{transformed_table_name_file}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"acquisition doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Pick only selected columns
            columns_to_picked = [
                'acquisition_id', 
                'acquiring_object_id',
                'acquired_object_id',
                'acquired_at',
                'price_amount',
                'price_currency_code',
                'term_code'
            ]
            df = df.select(*columns_to_picked)

            # Rename columns
            df = df \
                .withColumnRenamed('acquisition_id', 'acquisition_nk') \
                .withColumnRenamed('acquiring_object_id', 'acquiring_company_nk') \
                .withColumnRenamed('acquired_object_id', 'acquired_company_nk')


            # Cast nk columns
            df = df \
                .withColumn('acquisition_nk', col('acquisition_nk').cast(LongType()))

            # Fill NA and cast types
            df = df \
                        .withColumn('acquired_at', to_date(col('acquired_at'))) \
                        .withColumn('acquired_at', when(col('acquired_at').isNull(), lit("2100-01-01")).otherwise(col('acquired_at'))) \
                        .withColumn('acquired_at', date_format(col('acquired_at'), 'yyyyMMdd').cast(IntegerType())) \
                        .withColumn('term_code', coalesce(col('term_code'), lit('Unknown'))) \
                        # .withColumn('price_currency_code', coalesce(col('price_currency_code'), lit('N/A'))) \
                        # .withColumn('price_amount', coalesce(col('price_amount'), lit(0)))
                        
            # Load dimension company for ID lookups
            dim_company = spark.read.jdbc(
                url=DB_URL_WAREHOUSE,
                table="(SELECT * FROM dim_company) as data",
                properties={"user": DB_USER_WAREHOUSE, "password": DB_PASS_WAREHOUSE, "driver": "org.postgresql.Driver"}
            )

            dim_company = dim_company.select("company_nk", "company_id")

            # Join to get acquiring_company_id
            df = df.join(
                dim_company.withColumnRenamed("company_nk", "acquiring_company_nk"),
                on="acquiring_company_nk", how="left"
            ).withColumnRenamed("company_id", "acquiring_company_id")

            # Join to get acquired_company_id
            df = df.join(
                dim_company.withColumnRenamed("company_nk", "acquired_company_nk"),
                on="acquired_company_nk", how="left"
            ).withColumnRenamed("company_id", "acquired_company_id")

            # Drop natural key columns
            df = df.drop("acquiring_company_nk", "acquired_company_nk")

            # Cast column
            df = df.withColumn('acquisition_nk', col('acquisition_nk').cast(LongType()))
            df = df.withColumn('acquiring_company_id', col('acquiring_company_id').cast(LongType()))
            df = df.withColumn('acquired_company_id', col('acquired_company_id').cast(LongType()))
            df = df.withColumn('acquired_at', col('acquired_at').cast(IntegerType()))
            df = df.withColumn('price_amount', col('price_amount').cast(DecimalType()))
            
            # Write transformed df to parquet
            df.write.parquet(
                f"{transformed_data_path}/{transformed_table_name_file}",
                mode="overwrite"
            )
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to become {transformed_table_name}  from staging")

        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _temp_funding_rounds(incremental, date):
        """
        Extract and transform data from the funding_rounds table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Extract & Transform From Staging - funding_rounds") \
            .getOrCreate()
        table_name = "funding_rounds"
    
        try:
            # Define query for extracting data
            query = "(SELECT * FROM public.funding_rounds) as data"
            transformed_table_name = f"temp_{table_name}"
            transformed_table_name_file = f"{transformed_table_name}"
            if incremental:
                query = f"(SELECT * FROM public.funding_rounds WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                transformed_table_name_file = f"{transformed_table_name_file}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"funding_rounds doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Select relevant columns
            columns_to_picked = [
                'funding_round_id',
                'object_id',
                'funded_at',
                'funding_round_type',
                'funding_round_code',
                'raised_amount',
                'pre_money_valuation',
                'post_money_valuation',
                'participants',
                'is_first_round',
                'is_last_round'
            ]
            df = df.select(*columns_to_picked)

            # Rename columns
            df = df \
                .withColumnRenamed('object_id', 'investee_company_nk') \
                .withColumnRenamed('funding_round_id', 'funding_round_nk') \
                .withColumnRenamed('participants', 'number_of_participants') \
                .withColumnRenamed('is_first_round', 'round_position_desc') \
                .withColumnRenamed('is_last_round', 'round_stage_desc')

            # Map boolean to string
            df = df \
                .withColumn('round_position_desc', when(col('round_position_desc') == True, 'First Round')
                            .otherwise('Not First Round')) \
                .withColumn('round_stage_desc', when(col('round_stage_desc') == True, 'Last Round')
                            .otherwise('Ongoing Round'))

            # Deduplication
            df = df.dropDuplicates(['funding_round_nk'])

            # Cast nk columns
            df = df \
                .withColumn('funding_round_nk', col('funding_round_nk').cast(LongType()))\
                .withColumn('number_of_participants', col('number_of_participants').cast(IntegerType()))


            # Fill nulls and cast
            df = df \
                .withColumn('funded_at', to_date(col('funded_at'))) \
                .withColumn('funded_at', when(col('funded_at').isNull(), lit("2100-01-01")).otherwise(col('funded_at'))) \
                .withColumn('funded_at', date_format(col('funded_at'), 'yyyyMMdd').cast(IntegerType())) \
                # .withColumn('raised_amount', coalesce(col('raised_amount'), lit(0))) \
                # .withColumn('pre_money_valuation', coalesce(col('pre_money_valuation'), lit(0))) \
                # .withColumn('post_money_valuation', coalesce(col('post_money_valuation'), lit(0)))

            # Lookup company_id
            dim_company = spark.read.jdbc(
                url=DB_URL_WAREHOUSE,
                table="(SELECT * FROM dim_company) as data",
                properties={"user": DB_USER_WAREHOUSE, "password": DB_PASS_WAREHOUSE, "driver": "org.postgresql.Driver"}
            )

            dim_company = dim_company.select("company_id", "company_nk")

            df = df \
                .join(dim_company.withColumnRenamed("company_nk", "investee_company_nk").withColumnRenamed("company_id", "investee_company_id"),
                    on="investee_company_nk", how="left")
            # Drop natural key
            df = df.drop("investee_company_nk")

            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to be {transformed_table_name}  from staging")

        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _fact_funds(incremental, date):
        """
        Extract and transform data from the funds table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "funds"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - funds") \
                .getOrCreate()

            # Define query for extracting data
            query = "(SELECT * FROM public.funds) as data"
            transformed_table_name = f"fact_{table_name}"
            transformed_table_name_file = f"{transformed_table_name}"
            if incremental:
                query = f"(SELECT * FROM public.funds WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                transformed_table_name_file = f"{transformed_table_name_file}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"funds doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Select columns
            columns_to_picked = [
                'fund_id', 
                'object_id',
                'funded_at',
                'name',
                'raised_amount',
                'raised_currency_code'
            ]
            df = df.select(*columns_to_picked)

            # Rename columns
            df = df \
                .withColumnRenamed('fund_id', 'fund_nk') \
                .withColumnRenamed('object_id', 'company_nk') \
                .withColumnRenamed('name', 'fund_name')

            # Deduplication
            df = df.dropDuplicates(['fund_nk'])

            # Cast nk columns
            df = df \
                .withColumn('fund_nk', col('fund_nk').cast(LongType()))

            # Handle nulls and date formatting
            df = df \
                .withColumn('funded_at', to_date(col('funded_at'))) \
                .withColumn('funded_at', when(col('funded_at').isNull(), lit("2100-01-01")).otherwise(col('funded_at'))) \
                .withColumn('funded_at', date_format(col('funded_at'), 'yyyyMMdd').cast(IntegerType())) \
                # .withColumn('raised_amount', coalesce(col('raised_amount'), lit(0))) \
                # .withColumn('raised_currency_code', coalesce(col('raised_currency_code'), lit("N/A")))

            # Lookup company_id from dim_company
            dim_company = spark.read.jdbc(
                url=DB_URL_WAREHOUSE,
                table="(SELECT * FROM dim_company) as data",
                properties={"user": DB_USER_WAREHOUSE, "password": DB_PASS_WAREHOUSE, "driver": "org.postgresql.Driver"}
            )

            dim_company = dim_company.select("company_id", "company_nk")

            df = df.join(dim_company, on="company_nk", how="left")

            # Drop natural key
            df = df.drop("company_nk")

            # Cast column
            df = df.withColumn('fund_nk', col('fund_nk').cast(LongType()))
            df = df.withColumn('company_id', col('company_id').cast(LongType()))
            df = df.withColumn('funded_at', col('funded_at').cast(IntegerType()))
            df = df.withColumn('raised_amount', col('raised_amount').cast(DecimalType()))

            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to be {transformed_table_name}  from staging")

        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _temp_investments(incremental, date):
        """
        Extract and transform data from the investments table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "investments"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - investments") \
                .getOrCreate()

            # Define query for extracting data
            query = "(SELECT * FROM public.investments) as data"
            transformed_table_name = f"temp_{table_name}"
            transformed_table_name_file = f"{transformed_table_name}"
            if incremental:
                query = f"(SELECT * FROM public.investments WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                transformed_table_name_file = f"{transformed_table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"{table_name} doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Select relevant columns
            columns_to_picked = [
                'investment_id', 
                'funding_round_id',
                'investor_object_id'
            ]
            df = df.select(*columns_to_picked)

            # Rename columns
            df = df \
                .withColumnRenamed('investment_id', 'investment_nk') \
                .withColumnRenamed('funding_round_id', 'funding_round_nk') \
                .withColumnRenamed('investor_object_id', 'investor_company_nk')

            # Deduplication
            df = df.dropDuplicates(['investment_nk'])

            # Convert df types
            df = df \
                .withColumn('investment_nk', col('investment_nk').cast(LongType())) \
                .withColumn('funding_round_nk', col('funding_round_nk').cast(IntegerType()))

            # Lookup `company_id` from `dim_company` for investor
            dim_company = spark.read.jdbc(
                url=DB_URL_WAREHOUSE,
                table="(SELECT * FROM dim_company) as data",
                properties={"user": DB_USER_WAREHOUSE, "password": DB_PASS_WAREHOUSE, "driver": "org.postgresql.Driver"}
            )

            dim_company = dim_company.select("company_id", "company_nk")

            df = df \
                .join(dim_company.withColumnRenamed("company_nk", "investor_company_nk").withColumnRenamed("company_id", "investor_company_id"),
                    on="investor_company_nk", how="left")

            # Drop natural keys
            df = df.drop('investor_company_nk')

            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                        "step": "warehouse",
                        "process": "extraction and transformation",
                        "status": "success",
                        "source": "staging",
                        "table_name": table_name,
                        "error_msg": None,
                        "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to be {transformed_table_name}  from staging")

        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _fact_investment_round_participation(incremental, date):
        """
        Extract and transform data from the investment and funding_rounds table in the staging database to fact_investment_round_participation.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        # Define bucket and object name
        
        
        try:
            process = "Combine Transformations"
            table_names= 'investments, funding_rounds'
            table_name = "fact_investment_round_participation"
            temp_tables = ["temp_investments", "temp_funding_rounds"]

            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Joining Transformations table investments and funding_rounds") \
                .getOrCreate()
            
            transformed_table_name_file = f"{table_name}"
            
            temp_data = {}

            for temp_table in temp_tables:
                temp_table_path = f"{temp_table}"
                try:
                    if incremental:
                        transformed_table_name_file = f"{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"
                        temp_table_path = f"{temp_table}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"

                    # Read data from S3
                    temp_data[temp_table] = spark.read.parquet(f"{transformed_data_path}/{temp_table_path}")

                    temp_data[temp_table].show(5)

                
                except:
                    skip_msg = f"{temp_table} doesn't have new data. Skipped..."
                    log_msg = {
                    "step": "warehouse",
                    "process": "extraction and transformation",
                    "status": "skip",
                    "source": "staging",
                    "table_name": temp_table,
                    "error_msg": skip_msg,
                    "etl_date": current_timestamp
                    }
                    print(skip_msg)
                    return

            # Join data
            df = temp_data['temp_investments'].join(temp_data['temp_funding_rounds'], on='funding_round_nk', how='left')

            

            # Cast column
            df = df.withColumn('investment_nk', col('investment_nk').cast(LongType()))
            df = df.withColumn('funding_round_nk', col('funding_round_nk').cast(LongType()))
            df = df.withColumn('investee_company_id', col('investee_company_id').cast(LongType()))
            df = df.withColumn('investor_company_id', col('investor_company_id').cast(LongType()))
            df = df.withColumn('funded_at', col('funded_at').cast(IntegerType()))
            df = df.withColumn('raised_amount', col('raised_amount').cast(DecimalType()))
            df = df.withColumn('pre_money_valuation', col('pre_money_valuation').cast(DecimalType()))
            df = df.withColumn('post_money_valuation', col('post_money_valuation').cast(DecimalType()))
            df = df.withColumn('number_of_participants', col('number_of_participants').cast(DecimalType()))

            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"{table_names} don't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_names,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                        "step": "warehouse",
                        "process": "extraction and transformation",
                        "status": "success",
                        "source": "staging",
                        "table_name": table_names,
                        "error_msg": None,
                        "etl_date": current_timestamp
                        }

            print(f"Successfully Combining {table_names} to {table_name} from stagings")

        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_names,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_names}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_names}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _fact_ipos(incremental, date):
        """
        Extract and transform data from the ipos table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "ipos"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - ipos") \
                .getOrCreate()

            # Define query for extracting data
            query = "(SELECT * FROM public.ipos) as data"
            transformed_table_name = f"fact_{table_name}"
            transformed_table_name_file = transformed_table_name
            if incremental:
                transformed_table_name_file = f"{transformed_table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"
                query = f"(SELECT * FROM public.ipos WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"ipos doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Select relevant columns
            columns_to_picked = [
                'ipo_id', 
                'object_id',
                'public_at',
                'valuation_currency_code',
                'raised_currency_code',
                'valuation_amount',
                'raised_amount',
                'stock_symbol'
            ]
            df = df.select(*columns_to_picked)

            # Rename columns
            df = df \
                .withColumnRenamed('ipo_id', 'ipo_nk') \
                .withColumnRenamed('object_id', 'company_nk')

            # Deduplicate
            df = df.dropDuplicates(['ipo_nk'])

            # Cast nk columns
            df = df \
                .withColumn('ipo_nk', col('ipo_nk').cast(LongType()))
            
            # Handle nulls and format date
            df = df \
                .withColumn('public_at', to_date(col('public_at'))) \
                .withColumn('public_at', when(col('public_at').isNull(), lit("2100-01-01")).otherwise(col('public_at'))) \
                .withColumn('public_at', date_format(col('public_at'), 'yyyyMMdd').cast(IntegerType())) \
                # .withColumn('valuation_amount', coalesce(col('valuation_amount'), lit(0))) \
                # .withColumn('raised_amount', coalesce(col('raised_amount'), lit(0))) \
                # .withColumn('valuation_currency_code', coalesce(col('valuation_currency_code'), lit("N/A"))) \
                # .withColumn('raised_currency_code', coalesce(col('raised_currency_code'), lit("N/A"))) \
                # .withColumn('stock_symbol', coalesce(col('stock_symbol'), lit("N/A")))


            

            # Lookup dim_company to get company_id
            dim_company = spark.read.jdbc(
                url=DB_URL_WAREHOUSE,
                table="dim_company",
                properties={"user": DB_USER_WAREHOUSE, "password": DB_PASS_WAREHOUSE, "driver": "org.postgresql.Driver"}
            )
            df = df.join(dim_company.select("company_nk", "company_id"), on="company_nk", how="left")

            # Drop natural key
            df = df.drop("company_nk")

            df = df.withColumn('ipo_nk', col('ipo_nk').cast(LongType()))
            df = df.withColumn('company_id', col('company_id').cast(LongType()))
            df = df.withColumn('public_at', col('public_at').cast(IntegerType()))
            df = df.withColumn('valuation_amount', col('valuation_amount').cast(DecimalType()))
            df = df.withColumn('raised_amount', col('raised_amount').cast(DecimalType()))

            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to {transformed_table_name}  from staging")

        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

    @staticmethod
    def _fact_milestones(incremental, date):
        """
        Extract and transform data from the milestones table in the staging database.
        """
        current_timestamp = datetime.now().replace(microsecond=0)
        try:
            table_name = "milestones"
            transformed_table_name = f"fact_{table_name}"
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform From Staging - milestones") \
                .getOrCreate()

            # Define query for extracting data
            
            query = "(SELECT * FROM public.milestones) as data"
            transformed_table_name_file = transformed_table_name
            if incremental:
                query = f"(SELECT * FROM public.milestones WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"
                transformed_table_name_file = f"{transformed_table_name_file}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"

            # Read data from PostgreSQL
            df = spark.read.jdbc(
                url=DB_URL_STAGING,
                table=query,
                properties={"user": DB_USER_STAGING, "password": DB_PASS_STAGING, "driver": "org.postgresql.Driver"}
            )
            
            # Check if DataFrame is empty
            if df.isEmpty():
                skip_msg = f"ipos doesn't have new data. Skipped..."
                log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "skip",
                "source": "staging",
                "table_name": table_name,
                "error_msg": skip_msg,
                "etl_date": current_timestamp
                }
                print(skip_msg)
                return

            # Transform data
            
            # Select relevant columns
            columns_to_picked = [
                'milestone_id', 
                'object_id',
                'milestone_at',
                'description',
                'milestone_code'
            ]
            df = df.select(*columns_to_picked)

            # Rename columns
            df = df \
                .withColumnRenamed('milestone_id', 'milestone_nk') \
                .withColumnRenamed('object_id', 'company_nk')

            # Deduplicate
            df = df.dropDuplicates(['milestone_nk'])

            # Convert df types
            df = df \
                .withColumn('milestone_nk', col('milestone_nk').cast(LongType()))
            
            # Handle nulls and date conversion
            df = df \
                .withColumn('milestone_at', to_date(col('milestone_at'))) \
                .withColumn('milestone_at', when(col('milestone_at').isNull(), lit("2100-01-01")).otherwise(col('milestone_at'))) \
                .withColumn('milestone_at', date_format(col('milestone_at'), 'yyyyMMdd').cast(IntegerType())) \
                # .withColumn('description', coalesce(col('description'), lit("N/A"))) \
                # .withColumn('milestone_code', coalesce(col('milestone_code'), lit("N/A")))

            # Lookup dim_company to get company_id
            dim_company = spark.read \
                .format("jdbc") \
                .option("url", DB_URL_WAREHOUSE) \
                .option("dbtable", "dim_company") \
                .option("user", DB_USER_WAREHOUSE) \
                .option("password", DB_PASS_WAREHOUSE) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            df = df.join(dim_company.select("company_nk", "company_id"), on="company_nk", how="left")
        
            # Drop natural key
            df = df.drop("company_nk")

            df = df.withColumn('milestone_nk', col('milestone_nk').cast(LongType()))
            df = df.withColumn('company_id', col('company_id').cast(LongType()))
            df = df.withColumn('milestone_at', col('milestone_at').cast(IntegerType()))
            
            # Write transformed data to parquet
            df.write \
                .mode("overwrite") \
                .parquet(f"{transformed_data_path}/{transformed_table_name_file}")
            
            log_msg = {
                            "step": "warehouse",
                            "process": "extraction and transformation",
                            "status": "success",
                            "source": "staging",
                            "table_name": table_name,
                            "error_msg": None,
                            "etl_date": current_timestamp
                        }

            print(f"Successfully extracted and transformed {table_name} to {transformed_table_name}  from staging")

        except Exception as e:
            error_msg = str(e)
            log_msg = {
                "step": "warehouse",
                "process": "extraction and transformation",
                "status": "failed",
                "source": "staging",
                "table_name": table_name,
                "error_msg": error_msg,
                "etl_date": current_timestamp
            }
            raise AirflowException(f"Error when extracting and transforming {table_name}: {error_msg}")

        finally:
            try:
                if log_msg:
                    CustomPysparkPostgres._insert_log(spark,log_msg)
            except Exception as log_error:
                raise AirflowException(f"Error when logging {table_name}: {str(log_error)}")
            finally:
                spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: extract_transform.py <function_name> <incremental> <date>")
        sys.exit(-1)

    function_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    # Dictionary mapping function names to their corresponding methods
    function_map = {
        "dim_company": ExtractTransform._dim_company,
        "dim_people": ExtractTransform._dim_people,
        "fact_relationships": ExtractTransform._fact_relationships,
        "fact_acquisition": ExtractTransform._fact_acquisition,
        "temp_investments": ExtractTransform._temp_investments,
        "temp_funding_rounds": ExtractTransform._temp_funding_rounds,
        "fact_investment_round_participation": ExtractTransform._fact_investment_round_participation,
        "fact_funds": ExtractTransform._fact_funds,
        "fact_ipos": ExtractTransform._fact_ipos,
        "fact_milestones": ExtractTransform._fact_milestones,
    }

    if function_name in function_map:
        function_map[function_name](incremental, date)
    else:
        print(f"Unknown function name: {function_name}")
        sys.exit(-1)
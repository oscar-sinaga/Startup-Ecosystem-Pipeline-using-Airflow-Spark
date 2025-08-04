from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
# import sentry_sdk
from pathlib import Path
import pandas as pd


# Load .env dari root
BASE_DIR = Path(__file__).resolve().parents[2]  # Mengarah ke folder data_pipeline_pyspark/
load_dotenv(dotenv_path=BASE_DIR / ".env")


STAGING_DB_USER = os.getenv("STAGING_DB_USER")
STAGING_DB_PASSWORD = os.getenv("STAGING_DB_PASSWORD")
STAGING_DB_NAME = os.getenv("STAGING_DB_NAME")
STAGING_PORT = os.getenv("STAGING_PORT")

WAREHOUSE_DB_USER = os.getenv("WAREHOUSE_DB_USER")
WAREHOUSE_DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD")
WAREHOUSE_DB_NAME = os.getenv("WAREHOUSE_DB_NAME")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT")



def stg_engine():
    return create_engine(f"postgresql://{STAGING_DB_USER}:{STAGING_DB_PASSWORD}@localhost:{STAGING_PORT}/{STAGING_DB_NAME}")

def wh_engine():
    return create_engine(f"postgresql://{WAREHOUSE_DB_USER}:{WAREHOUSE_DB_PASSWORD}@localhost:{WAREHOUSE_PORT}/{WAREHOUSE_DB_NAME}")


def read_sql(PATH, table_name):
    #open your file .sql
    with open(f"{PATH}/{table_name}.sql", 'r') as file:
        content = file.read()
    
    #return query text
    return content

import re
import pandas as pd
import datetime
from minio import Minio
from io import BytesIO
# from src.utils.helper import ACCESS_KEY_MINIO,SECRET_KEY_MINIO,PROFILING_BUCKET_NAME,MINIO_PORT,MINIO_HOST
import json


class Profiling:
    # ACCESS_KEY_MINIO = ACCESS_KEY_MINIO
    # SECRET_KEY_MINIO = SECRET_KEY_MINIO
    # bucket_name = PROFILING_BUCKET_NAME
    # MINIO_PORT=MINIO_PORT
    # MINIO_HOST=MINIO_HOST

    def __init__(self, data, table_name) -> None:
        """
        iniate data and table_name for profiling
        """
        self.data = data
        self.table_name = table_name
        self.list_columns = data.columns.tolist()

    def get_columns(self):
        """
        get columns name from data
        """
        return self.data.columns.tolist()
    
    def selected_columns(self,data_type_list,unique_column_list,missing_column_list,date_column_list):
        """
        define wich columns will be used for profiling 
        a. check data type: data_type_list
        b. check unique value: unique_column_list
        c. check percentage missing value: missing_column_list
        d. check percentage valid date: date_column_list
        """
        self.data_type_list = data_type_list
        self.unique_column_list = unique_column_list
        self.missing_column_list = missing_column_list
        self.date_column_list = date_column_list
        self.column_profiling = set(self.data_type_list + self.unique_column_list + self.missing_column_list + self.date_column_list)

    def check_data_type(self, col_name: str):
        """
        check data type of column
        """
        dtype = self.data[col_name].dtypes

        return str(dtype)
    
    def check_value(self, col_name:str):
        """
        check unique value of column
        """
        list_value = self.data[col_name].unique().tolist()

        return list_value
    
    def get_percentage_missing_values(self, col_name: str):
        """
        check percentage missing value of column
        """

        n = len(self.data)

        n_missing = self.data[col_name].isnull().sum()
        percentage_null_value = (n_missing/n) * 100

        return percentage_null_value
    
    def validation_date(self, str_date):
        """
        check valid date format
        """
        pattern = r'\b\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])\b'

        if re.match(pattern,str_date):
            return True
        else:
            return False
    
    def get_percentage_valid_date(self,col_name:str):
        """
        check percentage valid date of column
        """
        self.data[col_name] = self.data[col_name].astype(str)
        self.data[f"{col_name}_valid_date"] = self.data[col_name].apply(self.validation_date)
        valid_data = self.data[self.data[f"{col_name}_valid_date"] == True]

        n = len(self.data)
        n_valid = len(valid_data)

        percentage = (n_valid/n)*100

        return percentage
        
    def check_negative_value(self, col_name: str) -> str:
        """
        check if column has negative value
        """
        if(self[col_name] < 0).any():
            return True
        else:
            return False
    
    # def save_report(self):

    #     current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    #     # Initialize MinIO client
    #     client = Minio(f'{self.MINIO_HOST}:{self.MINIO_PORT}',
    #                 access_key=self.ACCESS_KEY_MINIO,
    #                 secret_key=self.SECRET_KEY_MINIO,
    #                 secure=False)

    #     # Make a bucket if it doesn't exist
    #     if not client.bucket_exists(self.bucket_name):
    #         client.make_bucket(self.bucket_name)

    #     # Convert dict to JSON and then to bytes
    #     json_report = json.dumps(self.dict_report)
    #     json_bytes = json_report.encode('utf-8')

    #     # Upload the CSV file to the bucket
    #     client.put_object(
    #         bucket_name=self.bucket_name,
    #         object_name=f"{self.table_name}_{current_date}.json", #name the fail source name and current etl date
    #         data=BytesIO(json_bytes),
    #         length=len(json_bytes),
    #         content_type='application/csv'
    #     )
    #     return f"Save as {self.table_name}_{current_date}.json"

    # def reporting(self):
    #     """
    #     generate profiling report
    #     """
    #     self.dict_report = {
    #         "created_at": datetime.datetime.now().strftime("%Y-%m-%d"),
    #         "report":{}
    #     }
    #     for col in self.list_columns:
    #         self.dict_report["report"][col] = {}
    #         if col in self.data_type_list:
    #             self.dict_report["report"][col]["data_type"] = self.check_data_type(col)
    #         if col in self.unique_column_list:
    #             self.dict_report["report"][col]["unique_value"] = self.check_value(col)
    #         if col in self.missing_column_list:
    #             self.dict_report["report"][col]["percentage_missing_value"] = round(self.get_percentage_missing_values(col),2)
    #         if col in self.date_column_list:
    #             self.dict_report["report"][col]["percentage_valid_date"] = round(self.get_percentage_valid_date(col),2)
        
    #     print(self.dict_report)
    #     self.save_report()
    #     return self.dict_report
    
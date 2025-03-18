import pandas as pd
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format
from pyspark.sql.types import TimestampType
from app.helpers.data_helpers import transform_data, partition_data
from app.helpers.erros_helper import NoDataForDateError

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_PATH = os.path.join(DATA_DIR, 'raw', 'datos.xlsx')
PROCESSED_DATA_PATH = os.path.join(DATA_DIR, 'processed', 'data.json')
PROCESSED_DATA_PATH_PROCESS = os.path.join(DATA_DIR, 'processed', 'data_processed.json')

def load_data_pyspark():
    try:
        start_time = time.time()

        spark = SparkSession.builder \
            .appName("DataProcessing") \
            .getOrCreate()
        
        sheets = pd.read_excel(RAW_DATA_PATH, engine='openpyxl', sheet_name=None)
        df = pd.concat(sheets.values(), ignore_index=True)

        spark_df = spark.createDataFrame(df)

        for col_name in ["fact_datetime_start_date", "fact_datetime_end_date", "createdAt", "updatedAt", "fact_datetime_time_now", "fact_datetime_start_event", "fact_datetime_end_event"]:
                if col_name in spark_df.columns:
                    spark_df = spark_df.withColumn(col_name, date_format(to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'")))

        spark_df = spark_df.na.fill("")

        transformed_data = transform_data(spark_df)

        data_dict = transformed_data.toPandas().to_dict(orient='records')

        partitioned_data = partition_data(data_dict, chunk_size=5)

        end_time = time.time()
        execution_time = end_time - start_time

        print(f"Execution time: {execution_time:.4f} seconds")

        if not transformed_data:
            return {"error": "No data available for the current date.","status_code": 400}

        return {"data": partitioned_data,"status_code": 200}
    except Exception as e:
            print(f"Error loading data: {e}")
            return {"error": "An error occurred while processing the data.","status_code": 500} 

def load_data():
    try:
        start_time = time.time()

        sheets = pd.read_excel(RAW_DATA_PATH, engine='openpyxl', sheet_name=None)
        df = pd.concat(sheets.values(), ignore_index=True)  

        for col in ["fact_datetime_start_date", "fact_datetime_end_date", "createdAt", "updatedAt", "fact_datetime_time_now", "fact_datetime_start_event", "fact_datetime_end_event"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                df[col] = df[col].replace("NaT", "")

        df = df.fillna("")

        transformed_data = transform_data(df) 
        data_dict = df.to_dict(orient='records')
        # save_data_to_json(data_dict, True)
        # save_data_to_json(transformed_data, False)

        partitioned_data = partition_data(transformed_data, chunk_size=5)

        end_time = time.time()
        execution_time = end_time - start_time

        print(f"Execution time: {execution_time:.4f} seconds")

        if not transformed_data:
            return {
                "error": "No data available for the current date.",
                "status_code": 400  
            }
        
       

        return {
            "data": partitioned_data,
            "status_code": 200  
        }

    except Exception as e:
        print(f"Error loading data: {e}")
        return {
            "error": "An error occurred while processing the data.",
            "status_code": 500  
        }

def save_data_to_json(data, value):
    try:
        os.makedirs(os.path.dirname(PROCESSED_DATA_PATH if value else PROCESSED_DATA_PATH_PROCESS), exist_ok=True)
        with open(PROCESSED_DATA_PATH if value else PROCESSED_DATA_PATH_PROCESS, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error al guardar los datos procesados: {e}")

def save_data_to_excel(data, value):
    try:
        file_path = PROCESSED_DATA_PATH if value else PROCESSED_DATA_PATH_PROCESS
        file_path = file_path.replace(".json", ".xlsx")  
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        data.to_excel(file_path, index=False, engine='openpyxl')
        print(f"Datos guardados exitosamente en {file_path}")

    except Exception as e:
        print(f"Error al guardar los datos procesados: {e}")
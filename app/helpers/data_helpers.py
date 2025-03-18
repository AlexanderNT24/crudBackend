import pandas as pd
from datetime import datetime


def is_today(date_string):
    if pd.isna(date_string) or not date_string:
        return False
    date_part = date_string.split("T")[0]
    today = datetime.now().strftime('%Y-%m-%d')
    return date_part == today

def transform_data(df):
    transformed_data = []
    for _, row in df.iterrows():
        start_date = row.get("fact_datetime_start_date", "")
        if not is_today(start_date):
            continue  
        transformed_data.append({
            "dim_name_detail_status": "" if pd.isna(row.get("dim_name_detail_status")) else row.get("dim_name_detail_status", "").strip(),
            "end_time": "" if pd.isna(row.get("fact_datetime_end_date")) else row.get("fact_datetime_end_date", "").strip(),
            "equipment_name": "" if pd.isna(row.get("dim_name_equipment")) else row.get("dim_name_equipment", "").strip(),
            "fact_datetime_start_date": start_date.strip(),
            "main_fleet_id": "" if pd.isna(row.get("dim_id_fleet_type")) else str(row.get("dim_id_fleet_type", "")).strip(),
            "secondary_fleet_id": "" if pd.isna(row.get("dim_id_fleet")) else str(row.get("dim_id_fleet", "")).strip(),
            "start_time": "" if pd.isna(row.get("fact_datetime_start_date")) else row.get("fact_datetime_start_date", "").strip(),
            "status_main_text": "" if pd.isna(row.get("dim_name_main_status")) else row.get("dim_name_main_status", "").strip().replace(",", ""),
            "value": 0 if pd.isna(row.get("fact_float_sec_duration")) else row.get("fact_float_sec_duration", 0),
            "main_fleet_name": "" if pd.isna(row.get("dim_name_fleet_type")) else str(row.get("dim_name_fleet_type", "")).strip(),
            "secondary_fleet_name": "" if pd.isna(row.get("dim_name_fleet")) else str(row.get("dim_name_fleet", "")).strip(),
            "equipment_id": "" if pd.isna(row.get("dim_id_equipment")) else str(row.get("dim_id_equipment", "")).strip()
        })
    return transformed_data

def partition_data(data, chunk_size=5):
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


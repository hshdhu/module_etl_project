import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

def load_data(input_folder, output_data):
    db_name = "marine_data"
    # Kết nối MongoDB
    client = MongoClient('mongodb+srv://manhhoang2608tt:260804@cluster0.23b39zc.mongodb.net/')
    db = client[db_name]
    collection_name = os.path.basename(os.path.normpath(input_folder))
    print(collection_name)
    collection = db[collection_name]
    # Duyệt qua tất cả file CSV trong output_data
    for file in os.listdir(output_data):
        if file.endswith(".csv"):
            file_path = os.path.join(output_data, file)

            try:
                df = pd.read_csv(file_path)

                if 'TIME' in df.columns:
                    df['TIME'] = pd.to_datetime(df['TIME'], errors='coerce')

                records = df.to_dict(orient='records')

                if records:
                    collection.insert_many(records)
            except Exception as e:
                print(f" Lỗi khi xử lý {file}: {e}")
if __name__ == "__main__":
    load_data(
        "data_global_ocean_observation",
        "module_etl_project/output_cleaned",
    )

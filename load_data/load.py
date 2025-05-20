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
    # Đường dẫn tới file đã xử lý
    loaded_path="module_etl_project/load_data/loaded_files.txt"
    # Đọc danh sách file đã xử lý
    with open(loaded_path, "r") as f:
        loaded_files = f.read().splitlines()


    # Duyệt qua tất cả file CSV trong output_data
    for file in os.listdir(output_data):
        if file.endswith(".csv") and file not in loaded_files:
            file_path = os.path.join(output_data, file)
            try:
                df = pd.read_csv(file_path)

                if 'TIME' in df.columns:
                    df['TIME'] = pd.to_datetime(df['TIME'], errors='coerce')

                records = df.to_dict(orient='records')

                if records:
                    collection.insert_many(records)
                with open(loaded_path, "a") as f:
                    f.write(file + "\n")
                print("đã load")
            except Exception as e:
                print(f" Lỗi khi xử lý {file}: {e}")
import pandas as pd
import requests
import os
import json
# Lấy data từ web
df = pd.read_csv("https://ourworldindata.org/grapher/fish-seafood-production.csv?v=1&csvType=full&useColumnShortNames=true", storage_options = {'User-Agent': 'Our World In Data data fetch/1.0'})


metadata = requests.get("https://ourworldindata.org/grapher/fish-seafood-production.metadata.json?v=1&csvType=full&useColumnShortNames=true").json()
output_csv_folder = "D:/Year 3/IT4930/project ETL/extract_data/crawl_sanluong_thuysan/data_sanluong_thuysan"
os.makedirs(output_csv_folder, exist_ok=True)
df.to_csv(os.path.join(output_csv_folder, "fish_seafood_production_data.csv"), index=False)


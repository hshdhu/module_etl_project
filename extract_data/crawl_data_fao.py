import requests
import os
url = "https://www.fao.org/fishery/services/statistics/api/data/download/query/aquaculture_quantity/en"
download_folder = "data_input/data_fao"
file_name = "aquaculture_data.csv"
file_path = os.path.join(download_folder, file_name)
os.makedirs(download_folder, exist_ok=True)

headers = {
    "Authorization": "Bearer <token>",
    "Content-Type": "application/json",
}
years = [str(y) for y in range(2023, 1949, -1)]  # từ 2023 đến 1950
payload = {
    "aggregationType": "sum",
    "disableSymbol": "false",
    "includeNullValues": "true",
    "grouped": True,
    "rows": [
        {
            "field": "country_un_code",
            "group": "COUNTRY",
            "groupField": "name_en",
            "order": "asc"
        }
    ],
    "columns": [
        {
            "field": "year",
            "order": "desc",
            "values": years,
            "condition": "In",
            "sort": "desc"
        }
    ],
    "filters": []
}

res = requests.post(url, json=payload, headers=headers)

with open(file_path, "wb") as f:
    f.write(res.content)



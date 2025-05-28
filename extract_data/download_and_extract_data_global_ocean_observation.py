import requests
import xml.etree.ElementTree as ET
import os
import pandas as pd
import xarray as xr

output_csv_folder = "data_input/data_global_ocean_observation/data_global_ocean_observation_csv_files"
os.makedirs(output_csv_folder, exist_ok=True)
# Cấu hình
base_url = "https://mdl-native-01.s3.waw3-1.cloudferro.com/"
prefix = "native/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/monthly/TS/202504/"
params = {
    "list-type": "2",
    "delimiter": "/",
    "prefix": prefix,
}
local_dir = "data_input/data_global_ocean_observation/data_global_ocean_observation_nc_files"

# Tạo thư mục lưu file nếu chưa có
os.makedirs(local_dir, exist_ok=True)

# Liệt kê và tải file .nc
def list_and_download_all_files():
    continuation_token = None

    while True:
        if continuation_token:
            params["continuation-token"] = continuation_token

        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print("Lỗi khi truy vấn S3:", response.status_code)
            break

        root = ET.fromstring(response.text)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        t = 1
        for contents in root.findall("s3:Contents", ns):
            key = contents.find("s3:Key", ns).text
            if key.endswith(".nc"):
                file_url = base_url + key
                filename = key.split("/")[-1]
                local_path = os.path.join(local_dir, filename)

                print(f"Tải {filename} ...")
                r = requests.get(file_url)
                with open(local_path, "wb") as f:
                    f.write(r.content)
                print(f"Lưu tại: {local_path}")
                ds = xr.open_dataset(local_path, engine="netcdf4")
                df = ds.to_dataframe().reset_index()
                df.to_csv(os.path.join(output_csv_folder, f"file_{t}.csv"), index=False)
                print(f"Đã lưu dữ liệu từ {filename} vào CSV.")
            t += 1

        # Kiểm tra xem có phân trang không
        is_truncated = root.find("s3:IsTruncated", ns)
        if is_truncated is not None and is_truncated.text == "true":
            continuation_token = root.find("s3:NextContinuationToken", ns).text
        else:
            break

# Gọi hàm chính
list_and_download_all_files()

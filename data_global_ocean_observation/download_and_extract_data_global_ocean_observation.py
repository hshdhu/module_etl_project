import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import xarray as xr
import pandas as pd

# ======== Cấu hình ========
download_folder = "D:/Year 3/IT4930/project ETL/extract_data/data_global_ocean_observation/data_global_ocean_observation_nc_files"
os.makedirs(download_folder, exist_ok=True)

# URL trang chứa danh sách file
url = "https://data.marine.copernicus.eu/product/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/files?path=INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030%2Fcmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311%2Fmonthly%2FTS%2F202302%2F&subdataset=cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311--ext--monthly"

# ======== Khởi tạo Selenium với Chrome cấu hình để tự động tải file ========
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_folder.replace("/", "\\"),  # Windows path
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

print("[+] Đang mở trang...")
driver.get(url)
time.sleep(20)  # Đợi trang tải hoàn toàn

# ======== Trích xuất và click từng file .nc để tải ========
elements = driver.find_elements(By.XPATH, "//span[contains(text(), '.nc')]")
file_names = [el.text.strip() for el in elements if el.text.endswith(".nc")]
print(f"[+] Tìm thấy {len(file_names)} file .nc.")

for el in elements:
    name = el.text.strip()
    if name.endswith(".nc"):
        try:
            print(f"[↓] Đang tải: {name}")
            el.click()
            time.sleep(1)  # đợi tải xong (có thể điều chỉnh tùy mạng)
        except Exception as e:
            print(f"[!] Lỗi khi tải {name}: {e}")

driver.quit()
print("[✓] Hoàn tất tải file.")

# ======== Chuyển file .nc sang CSV ========
output_csv_folder = "D:/Year 3/IT4930/project ETL/extract_data/data_global_ocean_observation/data_global_ocean_observation_csv_files"
os.makedirs(output_csv_folder, exist_ok=True)

t = 1
for file_nc in os.listdir(download_folder):
    if file_nc.endswith(".nc"):
        try:
            file_path = os.path.join(download_folder, file_nc)
            ds = xr.open_dataset(file_path, engine="netcdf4")
            df = ds.to_dataframe().reset_index()
            df.to_csv(os.path.join(output_csv_folder, f"file_{t}.csv"), index=False)
            print(f"[✓] Đã lưu dữ liệu từ {file_nc} vào CSV.")
            t += 1
        except Exception as e:
            print(f"[!] Lỗi khi xử lý file {file_nc}: {e}")
print("[✓] Hoàn tất.")

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
import time
import re
import os

# Cấu hình trình duyệt
service = Service("C:\\Programme\\chromedriver-win64\\chromedriver.exe")  # Cập nhật đường dẫn đúng
driver = webdriver.Chrome(service=service)

# Mở trang web dự báo
url = "https://nchmf.gov.vn/Kttv/vi-VN/1/thoi-tiet-bien-24h-s12h3-15.html"
driver.get(url)
time.sleep(3)

# Tìm tất cả các vùng biển được dự báo
items = driver.find_elements(By.CSS_SELECTOR, ".grp-list-item li")

date_block = driver.find_element(By.CSS_SELECTOR, "h1.tt-news").text.strip() # Lấy ngày tháng từ tiêu đề
# Dùng regex để trích ngày/tháng/năm (VD: 07/05/2025)
match = re.search(r"(\d{2})/(\d{2})/(\d{4})", date_block)
if match:
    ngay, thang, nam = match.groups()
    file_name = f"data_{ngay}{thang}{nam}.csv"
else:
    file_name = "data_khong_xac_dinh_duoc_ngay.csv"
data = []


for item in items:
    try:
        desc_block = item.find_element(By.CSS_SELECTOR, ".text-weather-location").text.strip()
        lines = desc_block.split("\n")

        khu_vuc = lines[0].strip() if len(lines) >= 1 else ""
        mua = lines[1].strip() if len(lines) >= 2 else ""
        tam_nhin = lines[2].replace("Tầm nhìn xa :", "").strip() if len(lines) >= 3 else ""

        gio = ""
        song = ""
        if len(lines) >= 4:
            gio_song = lines[3]
            parts = gio_song.split(".")
            if len(parts) >= 2:
                gio = parts[0].strip() + "."
                song = parts[1].strip() + "."

        desc_text = item.find_element(By.TAG_NAME, "p").text.strip()
        desc_lines = desc_text.split("\n")

        if len(desc_lines) == 3:
            mưa = desc_lines[0].strip()
            tầm_nhìn = desc_lines[1].replace("Tầm nhìn xa :", "").strip()

            # Tách gió và sóng
            gio_part = ""
            song_part = ""
            gio_song_parts = desc_lines[2].split(".")
            if len(gio_song_parts) >= 2:
                gio_part = gio_song_parts[0].strip() + "."
                song_part = gio_song_parts[1].strip() + "."

        else:
            mưa, tầm_nhìn, gio_part, song_part = "", "", "", ""

        data.append({
            "Khu vực": khu_vuc,
            "Mưa": mưa,
            "Tầm nhìn xa": tầm_nhìn,
            "Gió": gio_part,
            "Sóng": song_part
        })


    except Exception as e:
        print(f"Lỗi: {e}")

# Xuất dữ liệu ra file CSV
df = pd.DataFrame(data)
data_ocean_weather_folder = "D:/Year 3/IT4930/project ETL/extract_data/data_weather/data_weather_ocean_vn_listdate"
df.to_csv(os.path.join(data_ocean_weather_folder, file_name), index=False)

driver.quit()
print("[✓] Đã lưu dữ liệu dự báo thời tiết biển.")

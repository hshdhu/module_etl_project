import os
import subprocess
from datetime import datetime

scrapy_project_dir = os.path.join(os.path.dirname(__file__), "thoitiet")
output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_input", "data_weather")
#current_dir = os.getcwd()
os.chdir(scrapy_project_dir)
# Lấy ngày hôm nay
current_date = datetime.now()
date_str = current_date.strftime("%d_%m_%Y")  # ví dụ 28_5_2025

#output_dir = "data_input/data_weather"
file_name = f"data_{date_str}.csv"
output_file = os.path.join(output_dir, file_name)

if not os.path.exists(output_dir):
    os.makedirs(output_dir)
# Chạy lệnh scrapy
subprocess.run(["scrapy", "crawl", "weather", "-o", output_file])
print(f"Đã lưu dữ liệu vào: {output_file}")
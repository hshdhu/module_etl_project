from pyspark.sql import SparkSession
import os
import shutil

# Định nghĩa các đường dẫn sử dụng os.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_input_dir = os.path.join(BASE_DIR, "data_input", "data_sanluongthuysan")
output_dir = os.path.join(BASE_DIR, "output", "san_luong_thuy_san", "san_luong_thuy_san_cleaned")
final_csv = os.path.join(BASE_DIR, "output", "san_luong_thuy_san", "san_luong_thuy_san_cleaned.csv")

aquaculture_path = os.path.join(data_input_dir, "aquaculture_farmed_fish_production_data.csv")
capture_path = os.path.join(data_input_dir, "capture_fishery_production_data.csv")
consumption_path = os.path.join(data_input_dir, "fish_and_seafood_consumption_per_capita_data.csv")
production_path = os.path.join(data_input_dir, "fish_seafood_production_data.csv")

# Khởi tạo Spark session
spark = SparkSession.builder.appName("MergeFisheryData").getOrCreate()

# Đọc các file CSV
aquaculture_df = spark.read.option("header", True).csv(aquaculture_path)
capture_df = spark.read.option("header", True).csv(capture_path)
consumption_df = spark.read.option("header", True).csv(consumption_path)
production_df = spark.read.option("header", True).csv(production_path)

# Đổi tên cột
aquaculture_df = aquaculture_df.withColumnRenamed("er_fsh_aqua_mt", "Aquaculture_Fish_Production_MT")
capture_df = capture_df.withColumnRenamed("er_fsh_capt_mt", "Capture_Fish_Production_MT")
consumption_df = consumption_df.withColumnRenamed(
    "fish_and_seafood__00002960__food_available_for_consumption__0645pc__kilograms_per_year_per_capita",
    "Fish_Seafood_Consumption_KG_Per_Capita"
)
production_df = production_df.withColumnRenamed(
    "fish_and_seafood__00002960__production__005511__tonnes",
    "Fish_Seafood_Production_Tonnes"
)

# Join theo Entity, Code, Year
from functools import reduce

dfs = [aquaculture_df, capture_df, consumption_df, production_df]
combined_df = reduce(
    lambda df1, df2: df1.join(df2, on=["Entity", "Code", "Year"], how="outer"),
    dfs
)

# ✅ Bỏ các dòng chứa bất kỳ giá trị null nào
combined_df = combined_df.dropna()

# Sắp xếp
combined_df = combined_df.orderBy("Entity", "Year")

# Ghi ra thư mục tạm
os.makedirs(os.path.dirname(output_dir), exist_ok=True)
combined_df.coalesce(1).write.option("header", True).csv(output_dir)

# Tìm file CSV trong thư mục tạm và đổi tên
for filename in os.listdir(output_dir):
    if filename.endswith(".csv"):
        temp_path = os.path.join(output_dir, filename)
        shutil.move(temp_path, final_csv)
        break

# Xóa thư mục tạm sau khi rename
shutil.rmtree(output_dir)

# Dừng Spark session
spark.stop()
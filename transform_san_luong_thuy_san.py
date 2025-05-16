from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from functools import reduce

# Khởi tạo Spark session
spark = SparkSession.builder.appName("MergeFisheryData").getOrCreate()

# Đọc các file CSV
aquaculture_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/aquaculture_farmed_fish_production_data.csv")
capture_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/capture_fishery_production_data.csv")
consumption_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/fish_and_seafood_consumption_per_capita_data.csv")
production_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/fish_seafood_production_data.csv")

# Đổi tên cột cho rõ ràng
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

# Danh sách các DataFrame
dfs = [aquaculture_df, capture_df, consumption_df, production_df]

# Gộp bằng outer join
combined_df = reduce(
    lambda df1, df2: df1.join(df2, on=["Entity", "Code", "Year"], how="outer"),
    dfs
)

# Sắp xếp lại
combined_df = combined_df.orderBy("Entity", "Year")

# Ghi ra một file duy nhất với tên cụ thể, các ô null giữ nguyên
import os
import shutil

output_dir = "output/san_luong_thuy_san/san_luong_thuy_san_cleaned"
final_file = "output/san_luong_thuy_san/san_luong_thuy_san_cleaned.csv"

# Xóa folder cũ nếu có
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

# Ghi dữ liệu ra folder tạm
combined_df.coalesce(1).write.option("header", True).csv(output_dir)

# Tìm file .csv vừa tạo và đổi tên
import glob
import pathlib

temp_file = glob.glob(f"{output_dir}/part-*.csv")[0]
pathlib.Path(temp_file).rename(final_file)

# Xoá folder tạm
shutil.rmtree(output_dir)

spark.stop()

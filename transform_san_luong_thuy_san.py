from pyspark.sql import SparkSession
import os
import shutil

# Khởi tạo Spark session
spark = SparkSession.builder.appName("MergeFisheryData").getOrCreate()

# Đọc các file CSV
aquaculture_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/aquaculture_farmed_fish_production_data.csv")
capture_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/capture_fishery_production_data.csv")
consumption_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/fish_and_seafood_consumption_per_capita_data.csv")
production_df = spark.read.option("header", True).csv("crawl_sanluong_thuysan/data_sanluong_thuysan/fish_seafood_production_data.csv")

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
combined_df = combined_df.orderBy("Entity", "Year")

# Ghi ra thư mục tạm
output_dir = "output/san_luong_thuy_san/san_luong_thuy_san_cleaned"
final_csv = "output/san_luong_thuy_san/san_luong_thuy_san_cleaned.csv"
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

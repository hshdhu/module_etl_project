from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import FloatType
import os
import shutil

# === 1. Khởi tạo Spark session ===
spark = SparkSession.builder.appName("FAO Wide to Long").getOrCreate()

# === 2. Đọc tất cả các file CSV trong thư mục ===
data_path = "../data_input/data_fao/"
csv_files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith(".csv")]

# === 3. Gộp dữ liệu từ tất cả file CSV ===
df_all = None
for file_path in csv_files:
    df = spark.read.option("header", True).csv(file_path)

    # Tìm tất cả các năm có trong tên cột
    year_cols = [c for c in df.columns if c.isdigit()]
    flag_cols = [f"{y} Flag" for y in year_cols]

    # Ép kiểu các cột số thành float
    for y in year_cols:
        df = df.withColumn(y, col(y).cast(FloatType()))

    # Tạo DataFrame long từ wide cho mỗi năm
    df_long_list = []
    for y in year_cols:
        df_y = df.select(
            col("Country Name En").alias("country"),
            col("Unit Name").alias("unit"),
            lit(int(y)).alias("year"),
            col(y).alias("value"),
            col(f"{y} Flag").alias("flag")
        )
        df_long_list.append(df_y)

    # Union tất cả các năm thành 1 dataframe
    df_long = df_long_list[0]
    for d in df_long_list[1:]:
        df_long = df_long.unionByName(d)

    # Bỏ các dòng có bất kỳ null nào
    df_long_clean = df_long.dropna()

    # Thêm vào df_all
    if df_all is None:
        df_all = df_long_clean
    else:
        df_all = df_all.unionByName(df_long_clean)

# === 4. Ghi file tạm theo từng năm ===
output_dir = "../output/fao_long_cleaned"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir, exist_ok=True)

# Lấy tất cả các năm
years = [row['year'] for row in df_all.select("year").distinct().collect()]

for year in years:
    year_str = str(year)
    year_df = df_all.filter(col("year") == year)

    temp_dir = f"{output_dir}/temp_{year_str}"
    final_csv = f"{output_dir}/{year_str}.csv"

    year_df.repartition(1).write.option("header", True).mode("overwrite").csv(temp_dir)

    # Tìm và đổi tên file .csv
    for f in os.listdir(temp_dir):
        if f.endswith(".csv"):
            shutil.move(os.path.join(temp_dir, f), final_csv)

    # Xoá thư mục tạm chứa file .crc
    shutil.rmtree(temp_dir)

# === 5. Dừng Spark session ===
spark.stop()

print("✅ Đã xử lý và xuất từng file theo năm thành công.")

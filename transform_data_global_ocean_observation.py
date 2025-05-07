import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import DoubleType


def get_processed_files(log_path):
    if not os.path.exists(log_path):
        return set()
    with open(log_path, "r") as f:
        return set(line.strip() for line in f.readlines())


def save_processed_file(log_path, filename):
    with open(log_path, "a") as f:
        f.write(filename + "\n")


def transform_incremental(input_folder, output_folder, log_path):
    # Đảm bảo thư mục output tồn tại
    os.makedirs(output_folder, exist_ok=True)

    spark = SparkSession.builder.appName("Incremental Transform").getOrCreate()

    processed_files = get_processed_files(log_path)
    all_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    new_files = [f for f in all_files if f not in processed_files]

    if not new_files:
        print("✅ Không có file mới để xử lý.")
        return

    for filename in new_files:
        file_path = os.path.join(input_folder, filename)
        df = spark.read.option("header", True).csv(file_path)

        df = df.drop("DEPTH", "DEPH")
        df_clean = df.withColumn("TEMP", col("TEMP").cast(DoubleType())) \
            .withColumn("TRAJECTORY", trim(lower(col("TRAJECTORY")))) \
            .withColumn("DC_REFERENCE", trim(col("DC_REFERENCE")))

        df_clean = df_clean.filter(
            (col("TIME_QC") == 1.0) &
            (col("POSITION_QC") == 1.0) &
            (col("TEMP_QC") == 1.0)
        )

        # Thay vì sử dụng Spark để ghi file, chuyển đổi thành pandas DataFrame và ghi bằng pandas
        output_file = os.path.join(output_folder, f"cleaned_{filename}")

        # Chuyển đổi sang pandas DataFrame
        pandas_df: pd.DataFrame = df_clean.toPandas()

        # Ghi dữ liệu bằng pandas
        pandas_df.to_csv(output_file, index=False)

        save_processed_file(log_path, filename)
        print(f"✅ Đã xử lý file: {filename}")

    spark.stop()


if __name__ == "__main__":
    transform_incremental(
        "data_global_ocean_observation/data_global_ocean_observation_csv_files",
        "output_cleaned",
        "processed_files.txt"
    )
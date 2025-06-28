from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
from datetime import datetime, timedelta


def read_csv(path):
    return spark.read.csv(path, header=True, inferSchema=True)


def get_previous_data(hdfs_base_path):
    """Get ALL previously saved data from HDFS by reading all existing folders"""
    try:
        # Try to read all existing parquet data from all date folders
        all_data_df = spark.read.parquet(hdfs_base_path + "*")
        record_count = all_data_df.count()
        print(f"Found existing data with {record_count} records")
        return all_data_df

    except Exception as e:
        print("No existing data found - this is the first save")
        return None


def save_as_parquet(new_df, hdfs_base_path):
    """Save DataFrame incrementally - combines with ALL previous data"""

    # Get the date from current dataframe for folder naming
    latest_date_value = new_df.agg(F.max("situation_date")).collect()[0][0]
    folder_name = datetime.strptime(latest_date_value, "%d-%m-%Y").strftime("%Y-%m-%d")
    previous_day = datetime.strptime(folder_name,"%Y-%m-%d") - timedelta(days=1)
    save_path = hdfs_base_path + folder_name + "/"

    print(f"\n=== Incremental Save for {latest_date_value} ===")
    print(f"New data records: {new_df.count()}")

    # Get ALL previously saved data
    previous_data = get_previous_data(f"{hdfs_base_path}/{previous_day.strftime('%Y-%m-%d')}/")

    if previous_data is not None:
        # Combine ALL previous data with new data
        print("Combining with ALL previous data...")
        combined_df = previous_data.union(new_df)
        total_records = combined_df.count()
        print(f"Combined total records: {total_records}")

    else:
        # First save - no previous data
        print("First save - no previous data to combine")
        combined_df = new_df
        total_records = combined_df.count()
        print(f"Initial records: {total_records}")

    # Save the combined dataset to the new date folder
    (combined_df.write
     .format("parquet")
     .mode("overwrite")
     .option("compression", "snappy")
     .save(save_path))

    print(f"Saved incremental data to: {save_path}")
    print(f"This folder now contains ALL data up to {latest_date_value}")

    return save_path


if __name__ == "__main__":
    current_dir = os.getcwd()

    spark = (SparkSession.builder.appName("feeder")
             .getOrCreate())

    # Read CSV files using absolute paths
    base_path = f"file://{current_dir}/data"
    df1 = read_csv(f"{base_path}/US_Accidents_March23-part1.csv")
    df2 = read_csv(f"{base_path}/US_Accidents_March23-part2.csv")
    df3 = read_csv(f"{base_path}/US_Accidents_March23-part3.csv")

    # Save to HDFS as Parquet
    hdfs_output_path = "hdfs://localhost:9000/data/accidents/"
    save_as_parquet(df1, hdfs_output_path)
    save_as_parquet(df2, hdfs_output_path)
    save_as_parquet(df3, hdfs_output_path)

    print(f"Successfully saved CSV data as Parquet to: {hdfs_output_path}")

    spark.stop()
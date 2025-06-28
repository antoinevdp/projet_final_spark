from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, to_date



def read_parquet(path):
    return spark.read.parquet(path, inferSchema=True)

def add_partition_columns(df):
    # Convert situation_date to date format and extract year, month, day
    df = df.withColumn("year", year(df["situation_date"]))
    df = df.withColumn("month", month(df["situation_date"]))
    df = df.withColumn("day", dayofmonth(df["situation_date"]))
    return df


def split_datasets(df):
    weather_cols = ['ID','situation_date','Weather_Timestamp', 'Temperature(F)', 'Wind_Chill(F)', 'Humidity(%)',
                    'Pressure(in)', 'Visibility(mi)', 'Wind_Direction', 'Wind_Speed(mph)',
                    'Precipitation(in)', 'Weather_Condition']

    accident_cols = ['ID','situation_date','Source', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng',
                     'End_Lat', 'End_Lng', 'Distance(mi)', 'Description', 'Street', 'City', 'County',
                     'State', 'Zipcode', 'Country', 'Timezone', 'Airport_Code', 'Amenity', 'Bump',
                     'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station',
                     'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop', 'Sunrise_Sunset',
                     'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight']

    accident_df = df.select(accident_cols)
    weather_df = df.select(weather_cols)

    return accident_df, weather_df


def save_to_hive(df, table_name):
    df.write.mode("overwrite") \
      .partitionBy("year", "month", "day") \
      .saveAsTable(table_name)



if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("preprocessor")
             .enableHiveSupport()
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    path = "hdfs://localhost:9000/data/accidents/2025-01-03"
    df = read_parquet(path)

    accident_df, weather_df = split_datasets(df)

    accident_df = add_partition_columns(accident_df)
    weather_df = add_partition_columns(weather_df)

    save_to_hive(accident_df, "accidents")
    save_to_hive(weather_df, "weather")

    print(f"Saved {accident_df.count()} accident records")
    print(f"Saved {weather_df.count()} weather records")

    spark.stop()

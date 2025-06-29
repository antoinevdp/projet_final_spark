from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
def read_hive(table_name):
    return spark.read.table(table_name)

load_dotenv()


jdbc_url = "jdbc:mysql://localhost:3306/accidents"
properties = {
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "driver": "com.mysql.cj.jdbc.Driver",
}

def top_accident_by_day(spark, df):
    df.createOrReplaceTempView("accidents")
    query = """SELECT DATE(Start_Time) as accident_date, COUNT(DISTINCT ID)
               FROM accidents
               GROUP BY DATE(Start_Time)
               ORDER BY COUNT(DISTINCT ID) DESC"""
    df = spark.sql(query)
    return df

def accidents_by_severity(spark, df):
    """Aggregate accidents by severity level"""
    df.createOrReplaceTempView("accidents")
    query = """SELECT Severity, COUNT(DISTINCT ID) as accident_count,
                      ROUND(COUNT(DISTINCT ID) * 100.0 / SUM(COUNT(DISTINCT ID)) OVER(), 2) as percentage
               FROM accidents
               WHERE Severity IS NOT NULL
               GROUP BY Severity
               ORDER BY Severity"""
    return spark.sql(query)

def accidents_by_state_city(spark, df):
    """Top accident hotspots by state and city"""
    df.createOrReplaceTempView("accidents")
    query = """SELECT State, City, COUNT(DISTINCT ID) as accident_count
               FROM accidents
               WHERE State IS NOT NULL AND City IS NOT NULL
               GROUP BY State, City
               ORDER BY COUNT(DISTINCT ID) DESC
               LIMIT 50"""
    return spark.sql(query)

def accidents_by_weather_condition(spark, accidents_df, weather_df):
    """Accidents grouped by weather conditions"""
    accidents_df.createOrReplaceTempView("accidents")
    weather_df.createOrReplaceTempView("weather")
    query = """SELECT w.Weather_Condition, COUNT(DISTINCT a.ID) as accident_count,
                      AVG(a.Severity) as avg_severity
               FROM accidents a
               JOIN weather w ON a.ID = w.ID
               WHERE w.Weather_Condition IS NOT NULL
               GROUP BY w.Weather_Condition
               ORDER BY COUNT(DISTINCT a.ID) DESC"""
    return spark.sql(query)

def accidents_by_time_patterns(spark, df):
    """Accident patterns by hour of day and day of week"""
    df.createOrReplaceTempView("accidents")
    query = """SELECT HOUR(Start_Time) as hour_of_day,
                      DAYOFWEEK(Start_Time) as day_of_week,
                      COUNT(DISTINCT ID) as accident_count
               FROM accidents
               WHERE Start_Time IS NOT NULL
               GROUP BY HOUR(Start_Time), DAYOFWEEK(Start_Time)
               ORDER BY hour_of_day, day_of_week"""
    return spark.sql(query)

def road_infrastructure_accidents(spark, df):
    """Accidents by road infrastructure features"""
    df.createOrReplaceTempView("accidents")
    query = """SELECT 
                   SUM(CASE WHEN Traffic_Signal = true THEN 1 ELSE 0 END) as traffic_signal_accidents,
                   SUM(CASE WHEN Stop = true THEN 1 ELSE 0 END) as stop_sign_accidents,
                   SUM(CASE WHEN Junction = true THEN 1 ELSE 0 END) as junction_accidents,
                   SUM(CASE WHEN Roundabout = true THEN 1 ELSE 0 END) as roundabout_accidents,
                   SUM(CASE WHEN Railway = true THEN 1 ELSE 0 END) as railway_accidents,
                   SUM(CASE WHEN Crossing = true THEN 1 ELSE 0 END) as crossing_accidents,
                   COUNT(DISTINCT ID) as total_accidents
               FROM accidents"""
    return spark.sql(query)

def weather_impact_analysis(spark, accidents_df, weather_df):
    """Analyze weather impact on accident severity"""
    accidents_df.createOrReplaceTempView("accidents")
    weather_df.createOrReplaceTempView("weather")
    query = """SELECT 
                   CASE 
                       WHEN w.`Temperature(F)` < 32 THEN 'Freezing'
                       WHEN w.`Temperature(F)` BETWEEN 32 AND 50 THEN 'Cold'
                       WHEN w.`Temperature(F)` BETWEEN 50 AND 70 THEN 'Moderate'
                       WHEN w.`Temperature(F)` BETWEEN 70 AND 85 THEN 'Warm'
                       ELSE 'Hot'
                   END as temperature_range,
                   CASE
                       WHEN w.`Visibility(mi)` < 1 THEN 'Poor'
                       WHEN w.`Visibility(mi)` BETWEEN 1 AND 5 THEN 'Moderate'
                       ELSE 'Good'
                   END as visibility_condition,
                   COUNT(DISTINCT a.ID) as accident_count,
                   AVG(a.Severity) as avg_severity,
                   AVG(w.`Wind_Speed(mph)`) as avg_wind_speed
               FROM accidents a
               JOIN weather w ON a.ID = w.ID
               WHERE w.`Temperature(F)` IS NOT NULL AND w.`Visibility(mi)` IS NOT NULL
               GROUP BY 1, 2
               ORDER BY accident_count DESC"""
    return spark.sql(query)



def save_to_mysql(spark, df, table_name, properties):
    df.write \
        .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("datamart")
             .enableHiveSupport()
             .getOrCreate())

    print("Loading data from Hive...")
    accidents_df = read_hive("accidents")
    weather_df = read_hive("weather")

    # Cache datasets for better performance
    accidents_df.cache()
    weather_df.cache()

    print("Generating aggregations...")

    # Original aggregation
    df1 = top_accident_by_day(spark, accidents_df)
    save_to_mysql(spark, df1, "top_accidents_by_day", properties)

    # New aggregations
    df2 = accidents_by_severity(spark, accidents_df)
    save_to_mysql(spark, df2, "accidents_by_severity", properties)

    df3 = accidents_by_state_city(spark, accidents_df)
    save_to_mysql(spark, df3, "accidents_by_location", properties)

    df4 = accidents_by_weather_condition(spark, accidents_df, weather_df)
    save_to_mysql(spark, df4, "accidents_by_weather", properties)

    df5 = accidents_by_time_patterns(spark, accidents_df)
    save_to_mysql(spark, df5, "accidents_time_patterns", properties)

    df6 = road_infrastructure_accidents(spark, accidents_df)
    save_to_mysql(spark, df6, "road_infrastructure_summary", properties)

    df7 = weather_impact_analysis(spark, accidents_df, weather_df)
    save_to_mysql(spark, df7, "weather_impact_analysis", properties)

    save_to_mysql(spark, accidents_df, "accidents", properties)
    save_to_mysql(spark, weather_df, "weather", properties)

    print("All aggregations completed and saved to MySQL!")

    spark.stop()
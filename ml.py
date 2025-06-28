from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.sql.functions import when, col, hour, dayofweek, month, desc
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import IntegerType


def read_hive(table_name):
    return spark.read.table(table_name)


def create_ml_pipeline(accidents_df, weather_df):
    accidents_df.createOrReplaceTempView("accidents")
    weather_df.createOrReplaceTempView("weather")

    query = """
            SELECT a.id, \
                   a.Severity, \
                   a.Start_Time, \
                   a.Start_Lng, \
                   a.Start_Lat, \
                   a.State, \
                   a.City, \
                   w.`Temperature(F)`    as temperature, \
                   w.`Humidity(%)`       as humidity, \
                   w.`Visibility(mi)`    as visibility, \
                   w.`Wind_Chill(F)`     as wind_chill, \
                   w.`Precipitation(in)` as precipitation, \
                   w.`Wind_Speed(mph)`   as wind_speed, \
                   w.Weather_Condition, \
                   a.Traffic_Signal, \
                   a.Stop, \
                   a.Junction, \
                   a.Bump, \
                   a.Roundabout, \
                   a.Railway, \
                   a.Crossing, \
                   a.Sunrise_Sunset, \
                   a.Civil_Twilight, \
                   a.Nautical_Twilight, \
                   a.Astronomical_Twilight
            FROM accidents as a
                     INNER JOIN weather as w
                                ON a.ID = w.ID \
            """

    df = spark.sql(query)
    print(f"Total records after join: {df.count()}")

    # Feature engineering
    df = df.withColumn("hour", hour(col("Start_Time"))) \
        .withColumn("day_of_week", dayofweek(col("Start_Time"))) \
        .withColumn("month", month(col("Start_Time"))) \
        .withColumn("is_rush_hour",
                    when((col("hour").between(7, 9)) | (col("hour").between(17, 19)), 1).otherwise(0)) \
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0))

    # Handle nulls and convert boolean columns to integers
    boolean_cols = ['Traffic_Signal', 'Stop', 'Junction', 'Bump', 'Roundabout',
                    'Railway', 'Crossing', 'Sunrise_Sunset', 'Civil_Twilight',
                    'Nautical_Twilight', 'Astronomical_Twilight']

    for col_name in boolean_cols:
        df = df.withColumn(col_name, when(col(col_name) == True, 1).otherwise(0))

    # Fill nulls in numeric columns
    numeric_cols = ['temperature', 'humidity', 'visibility', 'wind_chill',
                    'precipitation', 'wind_speed']

    for col_name in numeric_cols:
        mean_val = df.select(col(col_name)).na.drop().agg({col_name: "mean"}).collect()[0][0]
        df = df.na.fill({col_name: mean_val})

    # Fill nulls in categorical columns
    df = df.na.fill({'Weather_Condition': 'Unknown', 'State': 'Unknown', 'City': 'Unknown'})

    # Convert severity to binary classification (High: 3-4, Low: 1-2)
    df = df.withColumn("severity_binary", when(col("Severity") >= 3, 1).otherwise(0))

    print("Sample data after preprocessing:")
    df.select("Severity", "severity_binary", "temperature", "humidity", "is_rush_hour").show(5)

    # String indexing for categorical features
    state_indexer = StringIndexer(inputCol="State", outputCol="state_indexed", handleInvalid="keep")
    weather_indexer = StringIndexer(inputCol="Weather_Condition", outputCol="weather_indexed", handleInvalid="keep")

    # One-hot encoding
    state_encoder = OneHotEncoder(inputCol="state_indexed", outputCol="state_encoded")
    weather_encoder = OneHotEncoder(inputCol="weather_indexed", outputCol="weather_encoded")

    # Feature assembly
    feature_cols = [
                       'Start_Lng', 'Start_Lat', 'temperature', 'humidity', 'visibility',
                       'wind_chill', 'precipitation', 'wind_speed', 'hour', 'day_of_week',
                       'month', 'is_rush_hour', 'is_weekend'
                   ] + boolean_cols + ['state_encoded', 'weather_encoded']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

    # Model
    rf = RandomForestClassifier(featuresCol="scaled_features", labelCol="severity_binary",
                                numTrees=100, maxDepth=10, seed=42)

    # Pipeline
    pipeline = Pipeline(stages=[
        state_indexer, weather_indexer,
        state_encoder, weather_encoder,
        assembler, scaler, rf
    ])

    # Split data
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")

    # Train model
    print("Training model...")
    model = pipeline.fit(train_df)

    # Make predictions
    print("Making predictions...")
    predictions = model.transform(test_df)

    # Evaluate model
    evaluator = MulticlassClassificationEvaluator(
        labelCol="severity_binary", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)

    # F1 score
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="severity_binary", predictionCol="prediction", metricName="f1"
    )
    f1_score = f1_evaluator.evaluate(predictions)

    print(f"\n=== MODEL PERFORMANCE ===")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1 Score: {f1_score:.4f}")

    # Show sample predictions
    print("\nSample Predictions:")
    predictions.select("Severity", "severity_binary", "prediction", "probability").sort(desc("prediction")).show(20)

    # Feature importance (if available)
    try:
        rf_model = model.stages[-1]
        feature_importance = rf_model.featureImportances
        print(f"\nTop 10 Feature Importances:")
        for i, importance in enumerate(feature_importance.toArray()[:10]):
            print(f"Feature {i}: {importance:.4f}")
    except:
        print("Feature importance not available")

    return model, predictions


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ML").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading data from Hive...")
    accidents_df = read_hive("accidents")
    weather_df = read_hive("weather")

    print("Creating ML pipeline...")
    model, predictions = create_ml_pipeline(accidents_df, weather_df)

    spark.stop()
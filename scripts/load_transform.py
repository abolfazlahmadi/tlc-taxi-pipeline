import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_format, year
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType, StringType, IntegerType


def get_yellow_schema():
    return StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True)
    ])


def get_green_schema():
    return StructType([
        StructField("VendorID", LongType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("ehail_fee", IntegerType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("trip_type", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True)
    ])


def read_and_transform_spark_data(raw_dir="data/raw") -> 'DataFrame':
    spark = SparkSession.builder.appName("TLC_ETL_ReadTransform").getOrCreate()
    df_all = None

    for taxi_type in ["yellow", "green"]:
        taxi_path = os.path.join(raw_dir, taxi_type)
        if not Path(taxi_path).exists():
            print(f"[SKIP] Missing folder: {taxi_path}")
            continue

        schema = get_yellow_schema() if taxi_type == "yellow" else get_green_schema()
        df = spark.read.schema(schema).parquet(taxi_path)

        # Rename pickup column to unify
        pickup_col = "tpep_pickup_datetime" if taxi_type == "yellow" else "lpep_pickup_datetime"
        df = df.withColumnRenamed(pickup_col, "pickup_datetime")

        # Enrich with time and type
        df = df.withColumn("pickup_hour", hour("pickup_datetime")) \
               .withColumn("pickup_weekday", date_format("pickup_datetime", "E")) \
               .withColumn("pickup_year", year("pickup_datetime")) \
               .withColumn("taxi_type", col("VendorID") * 0 + (1 if taxi_type == "yellow" else 2))

        # Merge with final DataFrame
        df_all = df if df_all is None else df_all.unionByName(df, allowMissingColumns=True)

    if df_all is None:
        raise ValueError("No files loaded. Check data/raw folders and schemas.")
    
    return df_all

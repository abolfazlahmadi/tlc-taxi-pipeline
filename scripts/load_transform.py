import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_format, year
from pyspark.sql.types import DoubleType, LongType, IntegerType


def read_and_transform_spark_data(raw_dir="data/raw") -> 'DataFrame':
    """
    Reads all yellow and green taxi Parquet files, standardizes schema,
    enriches with time-based features, and merges into one Spark DataFrame.
    """
    spark = SparkSession.builder \
        .appName("TLC_ETL_ReadTransform") \
        .getOrCreate()

    df_all = None

    for taxi_type in ["yellow", "green"]:
        taxi_path = os.path.join(raw_dir, taxi_type)

        if not Path(taxi_path).exists():
            print(f"[WARNING] Skipping: {taxi_type} folder does not exist")
            continue

        print(f"[READING] Loading data from: {taxi_path}")
        df = spark.read.parquet(taxi_path)

        # Standardize pickup datetime column
        if taxi_type == "yellow" and "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        elif taxi_type == "green" and "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        else:
            print(f"[ERROR] No recognized pickup datetime in {taxi_type} data")
            continue

        # Fix schema inconsistencies by casting key columns
        df = df.withColumn("VendorID", col("VendorID").cast(LongType()))
        df = df.withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
        df = df.withColumn("trip_distance", col("trip_distance").cast(DoubleType()))
        df = df.withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
        df = df.withColumn("tip_amount", col("tip_amount").cast(DoubleType()))
        df = df.withColumn("total_amount", col("total_amount").cast(DoubleType()))

        # Add enrichment columns
        df = df.withColumn("pickup_hour", hour("pickup_datetime")) \
               .withColumn("pickup_weekday", date_format("pickup_datetime", "E")) \
               .withColumn("pickup_year", year("pickup_datetime")) \
               .withColumn("taxi_type", col("VendorID") * 0 + (1 if taxi_type == "yellow" else 2))

        # Combine with overall DataFrame
        df_all = df if df_all is None else df_all.unionByName(df, allowMissingColumns=True)

    if df_all is None:
        raise ValueError("No data loaded. Check raw folder structure and file availability.")

    return df_all

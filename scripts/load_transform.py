from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_format, year
from pathlib import Path
import os


def read_and_transform_spark_data(raw_dir="data/raw") -> 'DataFrame':
    spark = SparkSession.builder \
        .appName("TLC_ETL_ReadTransform") \
        .getOrCreate()

    df_all = None

    for taxi_type in ["yellow", "green"]:
        taxi_path = os.path.join(raw_dir, taxi_type)

        if not Path(taxi_path).exists():
            print(f"[WARNING] Skipping: {taxi_type} folder does not exist")
            continue

        df = spark.read.parquet(taxi_path)

        # Standardize pickup datetime column
        if taxi_type == "yellow" and "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        elif taxi_type == "green" and "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        else:
            print(f"[ERROR] No pickup column found in {taxi_type} data")
            continue

        # Enrich time-based columns
        df = df.withColumn("pickup_hour", hour("pickup_datetime")) \
               .withColumn("pickup_weekday", date_format("pickup_datetime", "E")) \
               .withColumn("pickup_year", year("pickup_datetime")) \
               .withColumn("taxi_type", col("VendorID") * 0 + (1 if taxi_type == "yellow" else 2))

        # Combine both DataFrames with different schemas
        df_all = df if df_all is None else df_all.unionByName(df, allowMissingColumns=True)

    return df_all

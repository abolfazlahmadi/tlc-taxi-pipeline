from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, hour, date_format, year
from pyspark.sql.types import LongType, DoubleType, IntegerType


def read_and_transform_spark_data(raw_dir="data/raw") -> DataFrame:
    spark = SparkSession.builder.appName("elc_etl)").getOrCreate()
    df_all = None
    for taxi_type in ["yellow", "green"]:
        folder = Path(raw_dir/taxi_type)
        if not folder.exists():
            continue
        for file_path in folder.glob("*.parquet"): 
            print(f"[READING] {file_path.name}")
            df = spark.read.parquet(str(file_path))

            if taxi_type == "yellow" and "tpep_pickup_datetime" in df.columns:
                df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")    
            elif taxi_type == "green" and "lpep_pickup_datetime" in df.columns:
                df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
            else:
                print(f"[SKIP] No pickup column found in: {file_path.name}")
                continue
            df = df.withColumn("VendorID", col("VendorID").cast(LongType())) \
                    .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
                    .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
                    .withColumn("fare_amount", col("fare_amount").cast(DoubleType())) \
                    .withColumn("tip_amount", col("tip_amount").cast(DoubleType())) \
                    .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
                    .withColumn("pickup_hour", hour("pickup_datetime")) \
                    .withColumn("pickup_weekday", date_format("pickup_datetime", "E")) \
                    .withColumn("pickup_year", year("pickup_datetime")) \
                    .withColumn("taxi_type", col("VendorID") * 0 + (1 if taxi_type == "yellow" else 2))
            df_all = df if df_all is None else df_all.unionByName(df, allowMissingColumns=True)
            if df_all is None:
                raise ValueError("No files could be read. Check data/raw/ folders.")
    return df_all
from scripts.load_transform import read_and_transform_spark_data

df = read_and_transform_spark_data()

# Save Parquet
df.write.mode("overwrite").parquet("data/processed/taxi_data_parquet")

# Save Avro
df.write.mode("overwrite").format("avro").save("data/avro/taxi_data_avro")

# Save schema to JSON
with open("etl_logs/taxi_data_schema.json", "w") as f:
    f.write(df.schema.json())

import sys
from pyspark.sql import SparkSession

date = sys.argv[1]
input_path = f"gs://hdfs-datalake-brewery/bronze/{date}/breweries.json"
output_path = f"gs://hdfs-datalake-brewery/silver/{date}/"

spark = SparkSession.builder.appName("Transform").getOrCreate()
df = spark.read.json(input_path)
df_filtered = df.filter(df.country == "United States")
df_selected = df_filtered.select("id", "name", "brewery_type", "state", "city")

df_selected.write.mode("overwrite").parquet(output_path)

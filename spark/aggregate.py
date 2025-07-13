import sys
from pyspark.sql import SparkSession

date = sys.argv[1]
input_path = f"gs://hdfs-datalake-brewery/silver/{date}/"
output_path = f"gs://hdfs-datalake-brewery/gold/{date}/"

spark = SparkSession.builder.appName("Aggregate").getOrCreate()
df = spark.read.parquet(input_path)

agg_df = df.groupBy("state", "brewery_type").count()
agg_df.write.mode("overwrite").partitionBy("state").parquet(output_path)

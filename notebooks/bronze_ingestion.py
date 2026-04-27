from pyspark.sql.functions import current_timestamp, input_file_name
from src.smart_json_reader import read_json_smart

# Input path (adjust in Databricks)
input_path = "/FileStore/data/"

# Output path
output_path = "/FileStore/bronze/json_data"

df = read_json_smart(input_path)

df_final = (
    df.withColumn("ingestion_time", current_timestamp())
      .withColumn("source_file", input_file_name())
)

# Write as Delta
df_final.write.format("delta").mode("append").save(output_path)

# Create table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze_json
USING DELTA
LOCATION '{output_path}'
""")

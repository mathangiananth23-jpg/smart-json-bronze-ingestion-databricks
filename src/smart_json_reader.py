from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import pandas as pd

spark = SparkSession.builder.getOrCreate()

def read_json_smart(path: str):
    """
    Smart JSON reader:
    - Detects structure
    - Uses Spark or flatten logic
    - Returns Spark DataFrame
    """

    df_sample = spark.read.json(path)
    schema = df_sample.schema

    # Case 1: Nested array inside object
    if len(schema.fields) == 1 and "array" in str(schema.fields[0].dataType).lower():
        col_name = schema.fields[0].name

        try:
            df = spark.read.json(path)
            df = df.select(explode(col(col_name)).alias("data")).select("data.*")
            print("Used Spark explode (distributed)")
            return df

        except Exception as e:
            print("Fallback to Pandas:", e)
            pdf = pd.read_json(path)
            flattened = pd.json_normalize(pdf[col_name])
            return spark.createDataFrame(flattened)

    # Case 2: Normal JSON
    else:
        print("Used Spark standard read")
        return spark.read.json(path)

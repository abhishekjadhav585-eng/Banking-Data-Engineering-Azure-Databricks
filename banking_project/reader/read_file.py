from pyspark.sql import DataFrame 

def read_csv(spark, file_path):
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        return df
    except Exception as e:
        raise Exception(f"Error while reading csv files:{e}")


def read_parquet(spark, file_path):
    try:
        df = spark.read.parquet(file_path)
        return df
    except Exception as e:
        raise Exception(f"Error while reading parquet files: {e}")


def read_delta(spark, file_path):
    try:
        df = spark.read.format("delta").load(file_path)
        return df
    except Exception as e:
        raise Exception(f"Error while reading delta files: {e}")
       
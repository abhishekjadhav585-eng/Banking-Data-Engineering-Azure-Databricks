# Databricks notebook source
from reader.read_file import read_parquet
from output.write_file import write_delta

# COMMAND ----------

file_path = r'/Volumes/project/banking/bronze/banking.accounts.parquet'
df = read_parquet(spark, file_path)
df.show()

# COMMAND ----------

output_path = r'/Volumes/project/banking/silver/accounts'
write_delta(df, output_path)

# COMMAND ----------

# DBTITLE 1,Cell 4
path = r'/Volumes/project/banking/silver/transactions'
df = spark.read.format("delta").load(path)
df.display()
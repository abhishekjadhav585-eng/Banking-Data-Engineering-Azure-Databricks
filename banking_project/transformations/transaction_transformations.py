from pyspark.sql.functions import col, lit, upper, trim, initcap, round, to_date, when

def clean_transaction_id(df):
    return df.withColumn(
         "transaction_id",
         upper(trim(col("transaction_id")))
    )

def clean_account_id(df):
    return df.withColumn(
         "account_id",
         upper(trim(col( "account_id")))
    )   

def standardize_transaction_type(df):
    return df.withColumn(
        "transaction_type",
        initcap(trim(col("transaction_type")))
    ) 

def clean_transaction_amount(df):
    return df.withColumn(
        "amount_usd",
        round(
            col("amount_usd").cast("decimal(18, 2)"),
            2
        )
    ) 

def convert_transaction_date(df):
    return df.withColumn(
        "transaction_date",
        when(
            col("transaction_date").isNull() |
            (trim(col("transaction_date").cast("string")) == ""),
            None
        ).otherwise(
            to_date(col("transaction_date"), "yyyy-MM-dd")
        )
    )

def clean_merchant_name(df):
    return df.withColumn(
         "merchant_name",
         initcap(trim(col( "merchant_name")))
    ) 

def standardize_channel(df):
    return df.withColumn(
         "channel",
         initcap(trim(col( "channel")))
    )

def standardize_transaction_status(df):
    return df.withColumn(
          "status",
          initcap(trim(col("status")))
    )  

def standardize_merchant_category(df):
    return df.withColumn(
         "merchant_category",
         initcap(trim(col("merchant_category")))
    ) 

def remove_duplicate_transactions(df):
    return df.dropDuplicates(["transaction_id"])         
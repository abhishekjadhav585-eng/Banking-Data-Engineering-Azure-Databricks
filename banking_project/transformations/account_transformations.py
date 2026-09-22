from pyspark.sql.functions import col, trim, lit, upper, lower, initcap,  regexp_replace, to_date, round, when, expr,concat, length

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def clean_account(df):
    return df.withColumn(
         "account_id",
         upper(trim(col( "account_id")))
    )

def clean_cutomer_id(df):
    return df.withColumn(
        "customer_id",
        upper(trim(col("customer_id")))
    ) 

def mask_account_number(df):

    return df.withColumn(
        "account_number",
        when(
            col("account_number").isNull(),
            None
        ).when(
            length(col("account_number").cast("string")) > 4,
            concat(
                expr(
                    "repeat('*', length(CAST(account_number AS STRING)) - 4)"
                ),
                expr(
                    "right(CAST(account_number AS STRING), 4)"
                )
            )
        ).otherwise(
            col("account_number").cast("string")
        )
    )

def standardize_account_type(df):
    return df.withColumn(
        "account_type",
        initcap(trim(col("account_type")))
    ) 

def clean_balence(df):
    return df.withColumn(
            "balance_usd",
            round(col("balance_usd").cast("decimal(18, 2)"), 2)
    )  

def standardize_account_status(df):
    return df.withColumn(
          "account_status",
          initcap(trim(col("account_status")))
    ) 

def standardize_currency(df):
    return df.withColumn(
         "currency",
         upper(trim(col( "currency")))
    )

def convert_date_opened(df):
    return df.withColumn(
        "date_opened",
        when(
            col("date_opened").isNull() |
            (trim(col("date_opened").cast("string")) == ""),
            None
        ).otherwise(
            to_date(col("date_opened"), "yyyy-MM-dd")
        )
    )

def clean_interest_rate(df):
    return df.withColumn(
        "interest_rate",
        F.round(F.col("interest_rate").cast(DecimalType(5, 2)), 2)
    )   

def remove_duplicate_accounts(df):
    return df.dropDuplicates(["account_id"])
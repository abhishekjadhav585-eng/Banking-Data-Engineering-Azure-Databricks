from pyspark.sql.functions import col, trim, upper, lit, to_date, initcap, round, when
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def clean_loan_id(df):
    return df.withColumn(
         "loan_id",
         upper(trim(col("loan_id")))
    )

def clean_customer_id(df):
    return df.withColumn(
         "customer_id",
         upper(trim(col("customer_id")))
    )

def standardize_loan_type(df):
    return df.withColumn(
         "loan_type",
         initcap(trim(col( "loan_type")))
    )

def  clean_principal_amount(df):
    return df.withColumn (
         "principal_amount",
         round(
             col("principal_amount").cast("decimal(18,2)"),
             2
         )
    ) 

def clean_outstanding_balance(df):
    return (
        df.withColumn(
            "outstanding_balance",
            F.round(F.col("outstanding_balance").cast(DecimalType(18, 2)), 2)
        )
        
    )

def clean_intrest_rate(df):
    return df.withColumn(
        "interest_rate",
        F.round(F.col("interest_rate").cast(DecimalType(5, 2)), 2)
    )    

def  clean_term_months(df):
    return df.withColumn(
           "term_months",
           col("term_months").cast("integer")
    )    

def convert_loan_dates(df):
    return (
        df.withColumn(
            "start_date",
            when(
                col("start_date").isNull() |
                (trim(col("start_date").cast("string")) == ""),
                None
            ).otherwise(
                to_date(col("start_date"), "yyyy-MM-dd")
            )
        )
        .withColumn(
            "next_payment_date",
            when(
                col("next_payment_date").isNull() |
                (trim(col("next_payment_date").cast("string")) == ""),
                None
            ).otherwise(
                to_date(col("next_payment_date"), "yyyy-MM-dd")
            )
        )
    )

def standardize_loan_status(df):
    return df.withColumn(
         "loan_status",
         initcap(trim(col( "loan_status"))) 
    ) 

def remove_duplicate_loans(df):
    return df.dropDuplicates(["loan_id"])      
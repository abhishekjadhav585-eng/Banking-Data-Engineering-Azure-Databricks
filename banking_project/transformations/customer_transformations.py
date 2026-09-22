from pyspark.sql.functions import col, trim, upper, lower, initcap, lit,  regexp_replace, to_date, when

def clean_customer_id(df):
    return df.withColumn(
         "customer_id",
         upper(trim(col("customer_id")))
    )

def standardize_names(df):
    return (
        df.withColumn("first_name", initcap(trim(col("first_name"))))
          .withColumn("last_name", initcap(trim(col("last_name"))))
    )

def standardize_email(df):
    return df.withColumn(
        "email",
        lower(trim(col("email")))
    ) 

def  standardize_phone(df):
    return df.withColumn(
         "phone_number",
         regexp_replace(trim(col("phone_number")), r"[^0-9+]", "")
    ) 

def mask_phone_number(df):
    return df.withColumn(
        "phone_number",
        regexp_replace(
            col("phone_number"),
            r"(\+\d{1,3})\d{6}(\d{4})",
            r"$1******$2"
        )
    )

def convert_date_of_birth(df):
    return df.withColumn(
        "date_of_birth",
        when(
            col("date_of_birth").isNull() |
            (trim(col("date_of_birth").cast("string")) == ""),
            None
        ).otherwise(
            to_date(col("date_of_birth"), "yyyy-MM-dd")
        )
    )

    
def standardize_location(df):
    return (
        df.withColumn("city", initcap(trim(col("city"))))
          .withColumn("state", upper(trim(col("state"))))
    )

def clean_postal_code(df):
    return df.withColumn(
        "postal_code",
        col("postal_code").cast("string")
    )

def standardize_risk_profile(df):
    return df.withColumn(
         "risk_profile",
         initcap(trim(col( "risk_profile")))
    )

 def remove_duplicate_customers(df):
     return df.dropDuplicates(["customer_id"])  
       
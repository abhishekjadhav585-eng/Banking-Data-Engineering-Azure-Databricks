from pyspark.sql.functions import (
    col,
    floor,
    months_between,
    current_date,
    when,
    round
)

def create_account_gold_features(df, logger):

    try:

        logger.info("starting account gold transformations")

        df = df.withColumn(
             "balance_category",
             when(col("balance_usd").isNull(), "Unknown")
             .when(col("balance_usd") < 1000, "Low Balance")
             .when(col("balance_usd") < 10000, "Medium Balance")
             .when(col("balance_usd") < 100000, "Hign Balance")
             .otherwise("Very High Balance")
        )

        df = df.withColumn(
             "estimated_annual_interest",
             when(
                 col("balance_usd").isNotNull() &
                 col("interest_rate").isNotNull(),
                 round(
                     col("balance_usd") *
                     col("interest_rate") / 100,
                     2
                 )
             ).otherwise(None)
        )

        df = df.withColumn(
             "account_age_years",
             when(
                 col("date_opened").isNotNull(),
                 floor(
                     months_between(
                         current_date(),
                         col("date_opened")
                     ) / 12
                 )
             ).otherwise(None)
        )

        df = df.withColumn(
            "account_tenure_category",
            when(col("account_age_years").isNull(), "Unknown")
            .when(col("account_age_years") < 1, "New Account")
            .when(col("account_age_years") < 5, "Established")
            .when(col("account_age_years") < 10, "Long Term")
            .otherwise("Very Long Term")
        )

        df = df.withColumn(
             "account_status_category",
             when(col("account_status").isNull(), "unknown")
             .when(
                 col("account_status").isin("Active", "ACTIVE", "active"),
                "Active"
            )
             .when(
                col("account_status").isin("Closed", "CLOSED", "closed"),
                "Closed"
            )
            .otherwise("Other")
        )

        df = df.withColumn(
            "account_value_segment",
            when(col("balance_usd").isNull(), "Unknown")
            .when(col("balance_usd") >= 100000, "Premium")
            .when(col("balance_usd") >= 25000, "Hign Value")
            .when(col("balance_usd") >= 5000, "Standard")
            .otherwise("Basic")
        )

        logger.info("Account gold transformation addded succesfully")

        return df
    
    except Exception as e:
        logger.error(f"Error in account gold feature creation: {e}")
        raise 

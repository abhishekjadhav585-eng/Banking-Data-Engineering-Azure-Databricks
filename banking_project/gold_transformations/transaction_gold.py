from pyspark.sql.functions import col, when, year, month, date_format, round

def create_transaction_gold_features(df, logger):

    try:

        logger.info("starting transformations gold transformations")

        df = df.withColumn(
            "transaction_value_category",
            when(col("amount_usd").isNull(), "Unknown")
            .when(col("amount_usd") < 100, "Low")
            .when(col("amount_usd") < 1000, "Medium")
            .when(col("amount_usd") < 5000, "High")
            .otherwise("Very High Value")
        )

        df = df.withColumn(
            "transaction_risk_flag",
            when(
                col("status").isin("Failed", "FAILED", "failed"),
                "High Risk"
            )
            .when(
                col("amount_usd") >= 5000,
                "High Value Transaction"
            )
            .when(
                col("status").isin("Pending", "PENDING", "pending"),
                "Pending Review"
            )
            .otherwise("Normal")
        )

        df = df.withColumn(
            "transaction_status_category",
            when(col("status").isNull(), "Unknown")
            .when(
                col("status").isin("Success", "SUCCESS", "success"),
                "Successful"
            )
            .when(
                col("status").isin("Failed", "FAILED", "failed"),
                "Failed"
            )
            .when(
                col("status").isin("Pending", "PENDING", "pending"),
                "Pending"
            )
            .otherwise("Other")
        )


        df = df.withColumn(
             "transaction_year",
             when(
                 col("transaction_date").isNotNull(),
                 year(col("transaction_date"))
             ).otherwise(None)
        )

        df = df.withColumn(
            "transaction_month",
            when(
                col("transaction_date").isNotNull(),
                month(col("transaction_date"))
            ).otherwise(None)
        )

        

        logger.info("transaction gold transformation succesfull")
        return df
    
    except Exception as e:

        logger.error(
            f"Error during transaction Gold transformations: {str(e)}"
        )

        raise


         

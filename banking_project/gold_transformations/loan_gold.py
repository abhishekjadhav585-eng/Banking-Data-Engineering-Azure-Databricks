from pyspark.sql.functions import col, round, when, datediff, current_date

def create_loan_gold_features(df, logger):

    try:

        logger.info("starting loan gold transformations")

        df = df.withColumn(
            "outstanding_percentage",
            when(
                col("principal_amount").isNotNull() &
                (col("principal_amount") != 0) &
                col("outstanding_balance").isNotNull(),

                round(
                    (
                        col("outstanding_balance") / 
                        col("principal_amount")
                    ) * 100,
                    2
                )
            ).otherwise(None)
        )   

        df = df.withColumn(
             "paid_percentage",
             when(
                 col("principal_amount").isNotNull() &
                 (col("principal_amount") != 0) &
                 col("outstanding_balance").isNotNull(),

                 round(
                     (
                         1 -
                         (
                            col("outstanding_balance") /
                            col("principal_amount")

                         )
                     ) * 100,
                     2
                 )
             ).otherwise(None)
        )


        df = df.withColumn(
                "loan_size_category",
                when(col("principal_amount").isNull(), "Unknown")
                .when(col("principal_amount") < 10000, "Small")
                .when(col("principal_amount") < 50000, "Medium")
                .when(col("principal_amount") < 100000, "Large")
                .otherwise("Very Large")
        )

        df = df.withColumn(
             "loan_risk_category",
             when(
                col("outstanding_percentage").isNull(), "Unknown"
             )
             .when(col("outstanding_percentage") >= 80, "Hign Risk")
             .when(col("outstanding_percentage") >= 50, "Medium Risk")
             .otherwise("Low Risk")
        )

        df = df.withColumn(
             "loan_term_category",
             when(col("term_months").isNull(), "Unknown")
             .when(col("term_months") <= 12, "Short Term")
             .when(col("term_months") <= 60, "Medium Term")
             .otherwise("Long Term")
        )


        df = df.withColumn(
            "payment_status",
            when(
                col("next_payment_date").isNull(),
                "Unknown"
            )
            .when(
                col("next_payment_date") < current_date(),
                "Payment Overdue"
            )
            .when(
                datediff(
                    col("next_payment_date"),
                    current_date()
                ) <= 7,
                "Payment Due Soon"
            )
            .otherwise("Upcoming Payment")
        )

        logger.info("loan gold transformation completed succesfully")
        return df
    
    except Exception as e:
        logger.exception("loan gold transformation failed: {e}")
        raise e
    

    




              
    
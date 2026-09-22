from pyspark.sql.functions import col, count, sum, avg, max, min, round, lit,  coalesce

def create_account_summary(accounts_df, logger):
    try:
        logger.info("creating account sumamry aggregation")

        accounts_summary = (
            accounts_df
            .groupBy("customer_id")
            .agg(
                count("account_id").alias("total_accounts"),
                round(sum("balance_usd"), 2).alias("total_balance_usd"),
                round(avg("balance_usd"), 2).alias("average_balance_usd"),
                round(max("balance_usd"), 2).alias("max_balance_usd"),
                round(min("balance_usd"), 2).alias("min_balance_usd")
            )
        )

        account_summary = (
            account_summary
            .withColumn(
                "total_balance_usd",
                coalesce(col("total_balance_usd"), lit(0))
            )
            .withColumn(
                "average_balance_usd",
                coalesce(col("average_balance_usd"), lit(0))
            )
            .withColumn(
                "max_balance_usd",
                coalesce(col("max_balance_usd"), lit(0))
            )
            .withColumn(
                "min_balance_usd",
                coalesce(col("min_balance_usd"), lit(0))
            )
        ) 

        logger.info("Account summary aggrigation completed succesfully")
        return accounts_summary

    except Exception as e:
        logger.error(f"error while creating account summary:{e}")
        raise


def loan_summary(loans_df, logger):

    try:
        logger.info("starting loan portfolio summary aggregation")

        loan_summary = (
            loans_df
            .groupBy("customer_id")
            .agg(
                count("loan_id").alias("total_loans"),

                round(sum("principal_amount"), 2).alias("total_loan_amount_usd"),
                round(sum("outstanding_balance"), 2).alias("total_outstanding_balance_usd"),
                round(avg("interest_rate"), 2).alias("average_loan_interest_rate"),
                round(avg("term_months"), 2).alias("average_loan_term_months")

                
            )
        )

        logger.info("Loan portfolio summary aggregation completed successfully")

        return loan_summary
    
    except Exception as e:
        logger.error(f"Error while creating loan summary:{e}")
        raise


def create_transaction_summary(transactions_df, accounts_df, logger):

    try:
        logger.info("starting transaction summary aggregation")

        transaction_customer_df = (
               transactions_df.join(
                   accounts_df.select(
                       "account_id",
                         "customer_id"
                   ),
                   on="account_id",
                   how="left"
               )
        )

        transaction_summary = (
               transaction_customer_df
               .groupBy("customer_id")
               .agg(
                   count("transaction_id").alias("total_transactions"),
                   round(sum("amount_usd"), 2).alias("total_transaction_amount_usd"),
                   round(avg("amount_usd"), 2).alias( "average_transaction_amount_usd"),
                   round(max("amount_usd"), 2).alias( "maximum_transaction_amount_usd"),
                   round(min("amount_usd"), 2).alias(  "minimum_transaction_amount_usd")
               )
                   
               
        )

        logger.info("Transaction summary aggregation completed successfully")
        return transaction_summary
    
    except Exception as e:
        logger.error(f"Error while creating transaction summary:{e}")
        raise


def create_customer_360(
    customers_df,
    account_summary_df,
    loan_summary_df,
     transaction_summary_df,
     logger
):
    
    try:
        logger.info("Starting Customer 360 creation")

        customer_360 = (
            customers_df
            .join(
                 account_summary_df,
                 on="customer_id",
                 how="left"
            )
            .join(
                 loan_summary_df,
                 on="customer_id",
                 how="left"
            )
            .join(
                transaction_summary_df,
                on="customer_id",
                how="left"
            )
        )

        metric_columns =[
              "total_accounts",
              "total_balance_usd",
              "average_balance_usd",
              "max_balance_usd",
              "min_balance_usd",
               "total_loans",
               "total_loan_amount_usd",
               "total_outstanding_balance_usd",
               "average_loan_interest_rate",
               "average_loan_term_months",
                "total_transactions",
                "total_transaction_amount_usd",
                "average_transaction_amount_usd",
                "maximum_transaction_amount_usd",
                "minimum_transaction_amount_usd"

                


        ]

        for column_name in metric_columns:
            if column_name in customer_360.columns:
                customer_360 = customer_360.withColumn(
                    column_name,
                    coalesce(
                        col(column_name),
                        lit(0)
                    )
                )

        logger.info("Customer 360 creation completed successfully") 
        return customer_360  

    except Exception as e:
        logger.error(f"Error while creating Customer 360:{e}")  
        raise   



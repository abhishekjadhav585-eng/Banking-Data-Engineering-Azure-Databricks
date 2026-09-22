# Databricks notebook source
dbutils.widgets.text(
    "root_path",
    "/Workspace/Users/abhijadhav931@gmail.com/banking_project",
    "Root Path"
)

root_path = dbutils.widgets.get("root_path")

print(f"Root Path is: {root_path}")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import sys
try:
    if root_path not in sys.path:
        sys.path.insert(0, root_path)
except Exception as e:
    raise Exception(f"Failed to add project root path: {e}")        

# COMMAND ----------

from utils.logger import get_logger
logger = get_logger("silver to gold")

# COMMAND ----------

try:
    from reader.read_file import read_delta
    from output.write_file import  write_delta
    from gold_transformations.account_gold import create_account_gold_features
    from gold_transformations.customer_gold import create_customer_gold_feature
    from gold_transformations.loan_gold import create_loan_gold_features
    from gold_transformations.transaction_gold import create_transaction_gold_features
    from gold_transformations.business_gold_transformations import create_account_summary, loan_summary, create_transaction_summary, create_customer_360 

    logger.info("all modules imported succesfully")

except Exception as e:
    logger.error(f"Failed to import all modules: {e}")
    raise    

# COMMAND ----------

customer_silver_path = r'/Volumes/project/banking/silver/customers/'
account_silver_path = r'/Volumes/project/banking/silver/accounts/'
loans_silver_path = r'/Volumes/project/banking/silver/loans/'
transactions_silver_path = r'/Volumes/project/banking/silver/transactions/'


customer_gold_path = r'/Volumes/project/banking/gold/customers'
accounts_gold_path = r'/Volumes/project/banking/gold/accounts'
loans_gold_path = r'/Volumes/project/banking/gold/loans'
transactions_gold_path = r'/Volumes/project/banking/gold/transactions'

customer_360_gold_path = r'/Volumes/project/banking/gold/customer_360'
account_summary_gold_path = r'/Volumes/project/banking/gold/account_summary'
loan_summary_gold_path = r'/Volumes/project/banking/gold/loan_summary'
transaction_summary_gold_path = r'/Volumes/project/banking/gold/transaction_summary'

# COMMAND ----------


try:
    logger.info("Starting customer silver to gold trasformations")

    customer_df = read_delta(spark, customer_silver_path)

    logger.info("silver data read succesfully")

    customer_gold_df =  create_customer_gold_feature(customer_df, logger)
    
    logger.info(" customer gold data created succesfully")

    logger.info(f"customer gold layer data count:{customer_gold_df.count()}")

    write_delta(customer_gold_df, customer_gold_path)

    logger.info("customer gold data written succesfully")

except Exception as e:
    logger.error(f"Customers Silver to Gold failed: {str(e)}")
    raise


# COMMAND ----------

try:
    logger.info("Starting accounts silver to gold")

    accounts_df = read_delta(
        spark, account_silver_path
    )


    logger.info("accounts silver data read succesfully")

    accounts_gold_df = create_account_gold_features(accounts_df, logger)

    logger.info("accounts gold data created succesfully")

    logger.info(f"accounts gold layer data count {accounts_gold_df.count()}")

    write_delta(accounts_gold_df, accounts_gold_path)

    logger.info("accounts gold data written succesfully")

except Exception as e:

    logger.error(
        f"Accounts Silver to Gold failed: {str(e)}"
    )

    raise





# COMMAND ----------

try:
    logger.info("started loans silver to gold transformations")

    loans_df = read_delta(spark, loans_silver_path)

    logger.info("loans silver data read succesfully")

    loans_gold_df = create_loan_gold_features(loans_df, logger)

    logger.info("loans gold transformation completed")

    logger.info(f"loans gold record count after transformation:{loans_gold_df.count()}")

    write_delta(loans_gold_df, loans_gold_path)

    logger.info("loans gold data written succesfully")

except Exception as e:
     logger.error(f"Loans Silver to Gold failed: {e}")
     raise
      
    






# COMMAND ----------

try:
    logger.info("starting transactions silver to gold")

    transaction_df = read_delta(spark, transactions_silver_path)

    logger.info("transaction silver data read succesfully")

    transaction_gold_df = create_transaction_gold_features(transaction_df, logger)
    
    logger.info("transaction gold transformation completed succesfully")

    logger.info(f"transaction gold record count{transaction_gold_df.count()}")

    write_delta(transaction_gold_df, transactions_gold_path)

    logger.info("transaction gold data written succesfully")

except Exception as e:
    logger.error(f"transaction silver to gold failed:{e}")
    raise
        

# COMMAND ----------

# DBTITLE 1,Cell 11
try:

    account_summary_df = create_account_summary(accounts_gold_df, logger)
    

    logger.info(f"account summary count are: {account_summary_df.count()}")

except Exception as e:
    logger.error(f"account summary failed failed at: {e}")
    raise


# COMMAND ----------

try:
    loan_summary_df = loan_summary(loans_gold_df, logger)
    

    logger.info(f"loans summary count: {loan_summary_df.count()}")

except Exception as e:
    logger.error("loan summary creation failed{e}")
    raise    

# COMMAND ----------

try:

    transaction_summary_df = create_transaction_summary(transaction_gold_df, accounts_gold_df, logger)
    

    logger.info(f"transaction summary count:{transaction_summary_df.count()}")

except Exception as e:
    logger.error(f"transaction summary creation failed at: {e}")
    raise    

# COMMAND ----------

try:

    customer_360_df = create_customer_360(
        customer_gold_df,
        account_summary_df,
        loan_summary_df,
        transaction_summary_df,
        logger

    )

    logger.info(f"customer 360 record count: {customer_360_df.count()}")
    

except Exception as e:
    logger.error(f"customer 360 creation failed:{e}")
    raise    

# COMMAND ----------

try:
    
    write_delta(account_summary_df, account_summary_gold_path)

    write_delta(loan_summary_df, loan_summary_gold_path)

    write_delta(transaction_summary_df, transaction_summary_gold_path)

    write_delta(customer_360_df, customer_360_gold_path)

    logger.info("individual gold tables written succesfully")

except Exception as e:
    logger.error(f"failed to write individual gold tables: {e}")
    raise    
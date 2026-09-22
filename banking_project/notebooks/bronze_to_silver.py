# Databricks notebook source
# DBTITLE 1,Set project root path
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
logger = get_logger("bronze_to_silver")

# COMMAND ----------

try:
    from reader.read_file import read_parquet
    from output.write_file import write_delta

    logger.info("reader and writer modules imported succesfully")
except Exception as e:
    logger.exception(f"Failed to import reader/writer modules: {e}")
    raise


# COMMAND ----------

try:
    from transformations.customer_transformations import (
        clean_customer_id,
        standardize_names,
        standardize_email,
        standardize_phone,
        mask_phone_number,
        convert_date_of_birth,
        standardize_location,
        clean_postal_code,
        standardize_risk_profile,
        remove_duplicate_customers

    )
    logger.info("customer transformation added succesfully")


except Exception as e:
    logger.exception(f"Failed to import customer transformations: {e}")
    raise    

# COMMAND ----------

try:
    logger.info("started customer bronze to silver processing")

    customer_bronze_path = r'/Volumes/project/banking/bronze/banking.customers.parquet'
    customer_silver_path = r'/Volumes/project/banking/silver/customers'

    logger.info(f"reading customers data from: {customer_bronze_path}")

    df_customers = read_parquet(spark, customer_bronze_path)
    
    logger.info("customer bronze data read succesfully")

    logger.info("applying customer transformations")

    df_customers =  clean_customer_id(df_customers)

    df_customers = standardize_names(df_customers)

    df_customers = standardize_email(df_customers)

    df_customers =  standardize_phone(df_customers)

    df_customers = mask_phone_number(df_customers)

    df_customers = convert_date_of_birth(df_customers)

    df_customers = standardize_location(df_customers)

    df_customers = clean_postal_code(df_customers)

    df_customers = standardize_risk_profile(df_customers)

    df_customers =  remove_duplicate_customers(df_customers)

    logger.info("all customers transformed succesfully")

    logger.info(f"customer data count after transformations are:{df_customers.count()}")

    logger.info(f"writing customers data to silver path:{customer_silver_path}")

    write_delta(df_customers, customer_silver_path, mode="overwrite")

    logger.info("cutomer data written into silver layer succesfully")


except Exception as e:
    logger.exception(f"bronze to silver customer processing failed: {e}")
    raise
  


# COMMAND ----------

# DBTITLE 1,Cell 7
import importlib, sys
if 'transformations.account_transformations' in sys.modules:
    importlib.reload(sys.modules['transformations.account_transformations'])

try:
    from transformations.account_transformations import (
        clean_account,
        clean_cutomer_id,
        mask_account_number,
        standardize_account_type,
        clean_balence,
        standardize_account_status,
        standardize_currency,
        convert_date_opened,
        clean_interest_rate,
        remove_duplicate_accounts


    )
    logger.info("account transformations added succesfully")

except Exception as e:
    logger.exception(f"failed to import account transformation:{e}")
    raise    

# COMMAND ----------

try:
    logger.info("started account bronze to silver processing")
    account_bronze_path = r'/Volumes/project/banking/bronze/banking.accounts.parquet'
    account_silver_path = r'/Volumes/project/banking/silver/accounts'

    logger.info(f"reading accounts data from {account_bronze_path}")

    account_df = read_parquet(spark, account_bronze_path)

    logger.info("account bronze data read succesfully")

    logger.info("applying account transformations")

    account_df = clean_account(account_df)

    account_df = clean_cutomer_id(account_df)

    account_df = mask_account_number(account_df)

    account_df = standardize_account_type(account_df)

    account_df = clean_balence(account_df)

    account_df = standardize_account_status(account_df)

    account_df = standardize_currency(account_df)

    account_df = convert_date_opened(account_df)

    account_df = clean_interest_rate(account_df)

    account_df = remove_duplicate_accounts(account_df)

    logger.info("all accounts transformed sucessfully")

    logger.info(f"account data count after transformations are:{account_df.count()}")

    logger.info(f"writing accounts data into silver layer:{account_silver_path}")

    write_delta(account_df, account_silver_path, mode="overwrite")

    logger.info("account data written into silver layer sucessfully")

   

except Exception as e:
    logger.exception(f"accounts bronze to silver transformation failed: {e}")
    raise    

# COMMAND ----------

try:
    from transformations.loan_transformations import (
        clean_loan_id,
        clean_customer_id,
        standardize_loan_type,
        clean_principal_amount,
        clean_term_months,
        convert_loan_dates,
        standardize_loan_status,
        remove_duplicate_loans,
        clean_outstanding_balance,
        clean_intrest_rate
    )
    logger.info("loan transformations added succesfully")

except Exception as e:
    logger.error(f"failed to import loan transformations: {e}")
    raise    

# COMMAND ----------

try:

    logger.info(" loan bronze to silver processing")
    loan_bronze_path = r'/Volumes/project/banking/bronze/banking.loans.parquet'
    loan_silver_path = r'/Volumes/project/banking/silver/loans'

    logger.info(f"reading loans data from: {loan_bronze_path}")

    loan_df = read_parquet(spark, loan_bronze_path)

    logger.info("loan data read succesfully")

    logger.info("applying loan transformation")

    loan_df = clean_loan_id(loan_df)

    loan_df = clean_customer_id(loan_df)

    loan_df = standardize_loan_type(loan_df)

    loan_df = clean_principal_amount(loan_df)

    loan_df = clean_outstanding_balance(loan_df)

    loan_df = clean_intrest_rate(loan_df)

    loan_df = clean_term_months(loan_df)

    loan_df = convert_loan_dates(loan_df)

    loan_df = standardize_loan_status(loan_df)

    loan_df = remove_duplicate_loans(loan_df)

    logger.info("all loans data transformed succesfully")

    logger.info(f"loan data count after transformation: {loan_df.count()}")

    logger.info(f"writing loans data into silver layer:{loan_silver_path}")

    write_delta(loan_df, loan_silver_path, mode="overwrite")

    logger.info("loan data written into silver layer sucesfully")

except Exception as e:
    logger.error(f"loan bronze to silver transformation failed: {e}")
    raise




# COMMAND ----------

try:
    from transformations.transaction_transformations import (
        clean_transaction_id,
        clean_account_id,
        standardize_transaction_type,
        clean_transaction_amount,
        convert_transaction_date,
        clean_merchant_name,
        standardize_channel,
        standardize_transaction_status,
        standardize_merchant_category,
        remove_duplicate_transactions

    )
    logger.info("transactions transformations addded succesfully")

except Exception as e:
    logger.exception(f"failed to import transaction transformations: {e}")
    raise    
  

# COMMAND ----------

try:

    logger.info("transactions bronze to silver processing")

    transactions_bronze_path = r'/Volumes/project/banking/bronze/banking.transactions.parquet'
    transactions_silver_path = r'/Volumes/project/banking/silver/transactions'

    logger.info(f"reading transactions data from: {transactions_bronze_path}")

    transaction_df = read_parquet(spark, transactions_bronze_path)

    logger.info("transaction data read succesfully")

    logger.info("applying transactions transformations")

    transaction_df = clean_transaction_id(transaction_df)

    transaction_df = clean_account_id(transaction_df)

    transaction_df = standardize_transaction_type(transaction_df)

    transaction_df = clean_transaction_amount(transaction_df)

    transaction_df = convert_transaction_date(transaction_df)

    transaction_df = clean_merchant_name(transaction_df)

    transaction_df = standardize_channel(transaction_df)

    transaction_df = standardize_transaction_status(transaction_df)

    transaction_df = standardize_merchant_category(transaction_df)

    transaction_df = remove_duplicate_transactions(transaction_df)

    logger.info("all transaction data transformed succesfully")

    logger.info(f"transaction data count after transformation: {transaction_df.count()}")

    logger.info(f"writing transaction data into silver path: {transactions_silver_path}")

    write_delta(transaction_df, transactions_silver_path, mode="overwrite")

    logger.info("transaction data written into silver layer succesfully")

except Exception as e:
    logger.exception(f"transactions bronze to silver transformation failed: {e}")
    raise


# COMMAND ----------

df_customers.columns

# COMMAND ----------

account_df.columns

# COMMAND ----------

loan_df.columns

# COMMAND ----------

transaction_df.columns
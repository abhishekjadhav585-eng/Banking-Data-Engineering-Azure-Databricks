from pyspark.sql.functions import (
    col, when, datediff, current_date, lower, 
    trim, concat_ws, floor, months_between, lit
)

def create_customer_gold_feature(df, logger):
    try:
        logger.info("started customer gold transformations")

        
        df = df.withColumn(
            "full_name",
            concat_ws(
                " ",
                trim(col("first_name")),
                trim(col("last_name"))
            )
        )

        
        df = df.withColumn(
            "customer_age",
            when(
                col("date_of_birth").isNotNull(),
                floor(
                    months_between(
                        current_date(),
                        col("date_of_birth")
                    ) / 12
                )
            ).otherwise(lit(None))
        )

      
        df = df.withColumn(
            "customer_age_group",
            when(col("customer_age").isNull(), lit("Unknown"))
            .when(col("customer_age") < 25, lit("Young"))
            .when((col("customer_age") >= 25) & (col("customer_age") < 40), lit("Adult"))
            .when((col("customer_age") >= 40) & (col("customer_age") < 60), lit("Middle Age"))
            .otherwise(lit("Senior"))
        )

     

        
        df = df.withColumn(
            "customer_segment",
            when(col("risk_profile") == "High", lit("Premium Monitoring"))
            .when(col("risk_profile") == "Medium", lit("Standard"))
            .when(col("risk_profile") == "Low", lit("Low Risk Customer"))
            .otherwise(lit("Unclassified"))
        )

      
        df = df.withColumn(
            "customer_location",
            concat_ws(
                ", ",
                trim(col("city")),
                trim(col("state"))
            )
        )

        logger.info("customer gold transformation completed successfully")
        return df

    except Exception as e:
        logger.exception("failed customer gold transformation")
        raise e
         
        
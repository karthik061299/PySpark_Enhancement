# ====================================================================
# Author: Ascendion AVA+
# Date: 
# Description: Enhanced PySpark ETL integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
# Updated by: ASCENDION AVA+
# Updated on: 
# Description: Adds join with BRANCH_OPERATIONAL_DETAILS and conditional population of REGION and LAST_AUDIT_DATE for BRANCH_SUMMARY_REPORT. All changes are annotated and commented for traceability.
# ====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()
        # Set log level for Spark logs
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
    """
    Reads a table from a JDBC source into a DataFrame.
    """
    logger.info(f"Reading table: {table_name}")
    try:
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        return df
    except Exception as e:
        logger.error(f"Failed to read table {table_name}: {e}")
        raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    return customer_df.join(account_df, "CUSTOMER_ID") \
                      .join(transaction_df, "ACCOUNT_ID") \
                      .select(
                          col("CUSTOMER_ID"),
                          col("NAME"),
                          col("ACCOUNT_ID"),
                          col("TRANSACTION_ID"),
                          col("AMOUNT"),
                          col("TRANSACTION_TYPE"),
                          col("TRANSACTION_DATE")
                      )

# [MODIFIED] Enhanced function to support integration of BRANCH_OPERATIONAL_DETAILS
# [ADDED] Accepts branch_operational_details_df and conditionally populates REGION and LAST_AUDIT_DATE

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_details_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and integrating operational details.

    [MODIFIED]: Joins BRANCH_OPERATIONAL_DETAILS and conditionally populates REGION and LAST_AUDIT_DATE.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details.")
    # [DEPRECATED] Previous aggregation logic without operational details
    # deprecated_branch_summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
    #     .join(branch_df, "BRANCH_ID") \
    #     .groupBy("BRANCH_ID", "BRANCH_NAME") \
    #     .agg(
    #         count("*").alias("TOTAL_TRANSACTIONS"),
    #         sum("AMOUNT").alias("TOTAL_AMOUNT")
    #     )
    # [ADDED] New logic: aggregate and join operational details
    branch_summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )

    branch_summary_df = branch_summary_df.join(branch_operational_details_df, "BRANCH_ID", "left")
    # [ADDED] Conditionally populate REGION and LAST_AUDIT_DATE based on IS_ACTIVE
    branch_summary_df = branch_summary_df \
        .withColumn("REGION", when(col("IS_ACTIVE") == 'Y', col("REGION"))) \
        .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") == 'Y', col("LAST_AUDIT_DATE")))
    # [MODIFIED] Output now includes REGION and LAST_AUDIT_DATE
    return branch_summary_df


def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Delta table.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    try:
        df.write.format("delta") \
          .mode("overwrite") \
          .saveAsTable(table_name)
        logger.info(f"Successfully written data to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise


def main():
    """
    Main ETL execution function.
    """
    spark = None
    try:
        spark = get_spark_session()

        # JDBC connection properties
        # TODO: Replace with secure credential management (e.g., Databricks Secrets, Azure Key Vault)
        jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        connection_properties = {
            "user": "your_user",
            "password": "your_password",
            "driver": "oracle.jdbc.driver.OracleDriver" # Ensure the driver is available in the classpath
        }

        # Read source tables
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        # [ADDED] Read BRANCH_OPERATIONAL_DETAILS table
        branch_operational_details_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # Create and write BRANCH_SUMMARY_REPORT
        # [MODIFIED] Pass branch_operational_details_df to function
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df)
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        # In a production environment, you might want to exit with a non-zero status code
        # import sys
        # sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()

# ====================================================================
# Author: Ascendion AVA+
# Date: 
# Description: Enhanced PySpark ETL for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT with conditional logic and schema evolution support.
# Updated by: ASCENDION AVA+
# Updated on: 
# Description: Adds join with BRANCH_OPERATIONAL_DETAILS, conditional REGION and LAST_AUDIT_DATE population, and change annotations for traceability.
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

    :param spark: The SparkSession object.
    :param jdbc_url: The JDBC URL for the database connection.
    :param table_name: The name of the table to read.
    :param connection_properties: A dictionary of connection properties (e.g., user, password).
    :return: A Spark DataFrame containing the table data.
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

    :param customer_df: DataFrame with customer data.
    :param account_df: DataFrame with account data.
    :param transaction_df: DataFrame with transaction data.
    :return: A DataFrame ready for the AML customer transactions report.
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

# [DEPRECATED] Old create_branch_summary_report without BRANCH_OPERATIONAL_DETAILS integration
# def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
#     """
#     Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
#     [DEPRECATED]: Does not include REGION and LAST_AUDIT_DATE from BRANCH_OPERATIONAL_DETAILS.
#     """
#     logger.info("Creating Branch Summary Report DataFrame.")
#     return transaction_df.join(account_df, "ACCOUNT_ID") \
#                          .join(branch_df, "BRANCH_ID") \
#                          .groupBy("BRANCH_ID", "BRANCH_NAME") \
#                          .agg(
#                              count("*").alias("TOTAL_TRANSACTIONS"),
#                              sum("AMOUNT").alias("TOTAL_AMOUNT")
#                          )

# [ADDED] Enhanced create_branch_summary_report with BRANCH_OPERATIONAL_DETAILS integration
# [MODIFIED] Now includes REGION and LAST_AUDIT_DATE columns populated conditionally

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level,
    integrating operational details such as REGION and LAST_AUDIT_DATE from BRANCH_OPERATIONAL_DETAILS.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame. [MODIFIED]")
    summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                              .join(branch_df, "BRANCH_ID") \
                              .groupBy("BRANCH_ID", "BRANCH_NAME") \
                              .agg(
                                  count("*").alias("TOTAL_TRANSACTIONS"),
                                  sum("AMOUNT").alias("TOTAL_AMOUNT")
                              )
    # [ADDED] Join with BRANCH_OPERATIONAL_DETAILS
    summary_df = summary_df.join(branch_operational_df, "BRANCH_ID", "left")
    # [ADDED] Conditional population of REGION and LAST_AUDIT_DATE
    summary_df = summary_df.withColumn(
        "REGION",
        when(col("IS_ACTIVE") == 'Y', col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE",
        when(col("IS_ACTIVE") == 'Y', col("LAST_AUDIT_DATE")).otherwise(None)
    )
    # [MODIFIED] Select only required columns for output
    summary_df = summary_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )
    return summary_df

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Delta table.

    :param df: The DataFrame to write.
    :param table_name: The name of the target Delta table.
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
        # [ADDED] Read BRANCH_OPERATIONAL_DETAILS
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # [MODIFIED] Create and write Enhanced BRANCH_SUMMARY_REPORT
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
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

# [NOTE] Ensure the BRANCH_SUMMARY_REPORT Delta table schema is updated to include REGION (STRING) and LAST_AUDIT_DATE (DATE) columns before running this ETL. See technical specifications for details.

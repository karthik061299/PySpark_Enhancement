# ====================================================================
# Author: Ascendion AVA+
# Date: 
# Description: Enhanced PySpark ETL for regulatory reporting. Adds REGION and LAST_AUDIT_DATE to BRANCH_SUMMARY_REPORT with conditional logic for active branches, preserves legacy logic, and annotates all changes for traceability.
# Updated by: ASCENDION AVA+
# Updated on: 
# Description: This output updates the ETL to join BRANCH_OPERATIONAL_DETAILS, conditionally populate new fields, and annotate all changes as per technical specs.
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

# [DEPRECATED] Old create_branch_summary_report function
# def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
#     """
#     Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
#     """
#     logger.info("Creating Branch Summary Report DataFrame.")
#     return transaction_df.join(account_df, "ACCOUNT_ID") \
#                          .join(branch_df, "BRANCH_ID") \
#                          .groupBy("BRANCH_ID", "BRANCH_NAME") \
#                          .agg(
#                              count("*").alias("TOTAL_TRANSACTIONS"),
#                              sum("AMOUNT").alias("TOTAL_AMOUNT")
#                          )

# [ADDED] Enhanced create_branch_summary_report function with REGION and LAST_AUDIT_DATE logic
# [MODIFIED] Applies conditional logic for REGION and LAST_AUDIT_DATE for active branches only

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_op_details_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level, joining with operational details, and conditionally populating REGION and LAST_AUDIT_DATE for active branches.
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_op_details_df: DataFrame with branch operational details.
    :return: A DataFrame containing the branch summary report with new fields.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with REGION and LAST_AUDIT_DATE.")
    joined_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                             .join(branch_df, "BRANCH_ID") \
                             .join(branch_op_details_df, "BRANCH_ID", "left")
    agg_df = joined_df.groupBy("BRANCH_ID", "BRANCH_NAME", "REGION", "LAST_AUDIT_DATE", "IS_ACTIVE") \
                     .agg(
                         count("*").alias("TOTAL_TRANSACTIONS"),
                         sum("AMOUNT").alias("TOTAL_AMOUNT")
                     )
    # [MODIFIED] Apply conditional logic for REGION and LAST_AUDIT_DATE
    result_df = agg_df.withColumn(
        "REGION",
        when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE",
        when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        col("REGION"),
        col("LAST_AUDIT_DATE")
    )
    return result_df

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
        branch_op_details_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)  # [ADDED]

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # [MODIFIED] Create and write enhanced BRANCH_SUMMARY_REPORT
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_details_df)
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

# [ADDED] All changes are annotated with [MODIFIED], [ADDED], or [DEPRECATED] for traceability.
# [ADDED] REGION and LAST_AUDIT_DATE are conditionally populated only for active branches (IS_ACTIVE = 'Y').
# [ADDED] Legacy logic is preserved and commented out where replaced.
# [ADDED] The output is ready-to-run and compatible with the updated schema.

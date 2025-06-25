# PySpark KPI Generation Logic will go here.
# This module will calculate KPIs from the transformed PySpark DataFrames.

import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, lit, count as _count, coalesce, avg as _avg

logger = logging.getLogger(__name__)

# Define allowed values for categorical columns
ALLOWED_CATEGORIES = ["men", "women", "kids"]
ALLOWED_PAYMENT_METHODS = ["upi", "credit_card", "wallet"]

def calculate_top_selling_products(df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    Calculates the top N selling products based on total quantity sold.

    Args:
        df: Input DataFrame (should be cleaned and transformed).
        top_n: The number of top products to return.

    Returns:
        A DataFrame with columns "product_id", "product_name", "total_quantity_sold",
        ordered by total_quantity_sold descending, limited to top_n.
    """
    logger.info(f"Calculating top {top_n} selling products...")
    if not isinstance(df, DataFrame):
        logger.error("Input to calculate_top_selling_products must be a valid PySpark DataFrame.")
        # Or raise ValueError, but for now, let Spark throw error if df is invalid for operations
        # Consider returning an empty DataFrame with schema if df is None or not a DataFrame.
        # For now, assuming df is valid or Spark will handle it.

    top_products_df = df.groupBy("product_id", "product_name") \
        .agg(_sum("quantity").alias("total_quantity_sold")) \
        .orderBy(desc("total_quantity_sold")) \
        .select("product_id", "product_name", "total_quantity_sold") \
        .limit(top_n)

    logger.info("Top selling products calculation complete.")
    return top_products_df

def calculate_total_revenue_by_category(df: DataFrame, spark_session: SparkSession) -> DataFrame:
    """
    Calculates the total revenue for each category.
    Ensures all allowed categories are present in the output, with 0.0 revenue if no sales.

    Args:
        df: Input DataFrame (should be cleaned and transformed).
        spark_session: The active SparkSession, needed to create the categories DataFrame.

    Returns:
        A DataFrame with columns "category" and "total_revenue".
    """
    logger.info("Calculating total revenue by category...")
    if not isinstance(df, DataFrame):
        logger.error("Input df to calculate_total_revenue_by_category must be a valid PySpark DataFrame.")
    if not isinstance(spark_session, SparkSession):
        logger.error("spark_session must be a valid SparkSession object for calculate_total_revenue_by_category.")

    df_with_item_revenue = df.withColumn("item_revenue", col("price") * col("quantity"))
    revenue_by_cat_df = df_with_item_revenue.groupBy("category") \
        .agg(_sum("item_revenue").alias("total_revenue_calculated"))

    categories_list = [(cat,) for cat in ALLOWED_CATEGORIES]
    all_categories_df = spark_session.createDataFrame(categories_list, ["category_ref"])

    joined_df = all_categories_df.join(
        revenue_by_cat_df,
        all_categories_df["category_ref"] == revenue_by_cat_df["category"],
        "left_outer"
    ).select(
        all_categories_df["category_ref"].alias("category"),
        coalesce(revenue_by_cat_df["total_revenue_calculated"], lit(0.0)).alias("total_revenue")
    ).orderBy("category")

    logger.info("Total revenue by category calculation complete.")
    return joined_df

def calculate_preferred_payment_method_by_category(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Counts the occurrences of each payment_method for each category.
    Ensures all categories and payment methods are present in a structured way.

    Args:
        df: Input DataFrame (should be cleaned and transformed).
        spark: The active SparkSession.

    Returns:
        A DataFrame with "category", "payment_method", and "count" columns.
    """
    logger.info("Calculating preferred payment method by category...")
    if not isinstance(df, DataFrame):
        logger.error("Input df to calculate_preferred_payment_method_by_category must be a valid PySpark DataFrame.")
    if not isinstance(spark, SparkSession):
        logger.error("spark must be a valid SparkSession object for calculate_preferred_payment_method_by_category.")

    payment_counts_df = df.groupBy("category", "payment_method") \
        .agg(_count("*").alias("actual_count"))

    all_combinations = []
    for cat in ALLOWED_CATEGORIES:
        for pm in ALLOWED_PAYMENT_METHODS:
            all_combinations.append((cat, pm))

    base_df = spark.createDataFrame(all_combinations, ["category_base", "payment_method_base"])

    result_df = base_df.join(
        payment_counts_df,
        (base_df["category_base"] == payment_counts_df["category"]) & \
        (base_df["payment_method_base"] == payment_counts_df["payment_method"]),
        "left_outer"
    ).select(
        base_df["category_base"].alias("category"),
        base_df["payment_method_base"].alias("payment_method"),
        coalesce(payment_counts_df["actual_count"], lit(0)).alias("count")
    ).orderBy("category", "payment_method")

    logger.info("Preferred payment method by category calculation complete.")
    return result_df

def calculate_purchase_frequency_per_user(df: DataFrame) -> DataFrame:
    """
    Counts how many items (rows) are associated with each user_id.

    Args:
        df: Input DataFrame (should be cleaned and transformed).

    Returns:
        A DataFrame with "user_id" and "item_count", ordered by item_count descending.
    """
    logger.info("Calculating purchase frequency per user...")
    if not isinstance(df, DataFrame):
        logger.error("Input to calculate_purchase_frequency_per_user must be a valid PySpark DataFrame.")

    frequency_df = df.groupBy("user_id") \
        .agg(_count("*").alias("item_count")) \
        .orderBy(desc("item_count"))

    logger.info("Purchase frequency per user calculation complete.")
    return frequency_df

def calculate_average_order_value(df: DataFrame) -> float:
    """
    Calculates the average value of orders.
    An order's value is the sum of (price * quantity) for all items with the same order_id.

    Args:
        df: Input DataFrame (should be cleaned and transformed).

    Returns:
        A single float value representing the average order value.
        Returns 0.0 if there are no orders or the input DataFrame is empty.
    """
    logger.info("Calculating average order value...")
    if not isinstance(df, DataFrame):
        logger.error("Input to calculate_average_order_value must be a valid PySpark DataFrame.")
        return 0.0 # Return default for invalid input type

    if df.rdd.isEmpty():
        logger.warning("Input DataFrame for AOV calculation is empty. Returning 0.0.")
        return 0.0

    df_with_item_revenue = df.withColumn("item_revenue", col("price") * col("quantity"))
    order_totals_df = df_with_item_revenue.groupBy("order_id") \
        .agg(_sum("item_revenue").alias("order_total_value"))

    if order_totals_df.rdd.isEmpty():
        logger.warning("No orders found after grouping for AOV calculation. Returning 0.0.")
        return 0.0

    aov_df = order_totals_df.agg(_avg("order_total_value").alias("average_order_value"))
    aov_result = aov_df.collect()

    if not aov_result or aov_result[0]["average_order_value"] is None:
        logger.warning("AOV calculation resulted in None (e.g. no valid order totals). Returning 0.0.")
        return 0.0

    final_aov = float(aov_result[0]["average_order_value"])
    logger.info(f"Average order value calculation complete. AOV: {final_aov:.2f}")
    return final_aov


if __name__ == '__main__':
    import sys
    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from pyspark_pipeline.logging_utils import setup_logging
    setup_logging() # Configure logging using the new utility

    spark = None
    try:
        from pyspark_pipeline.main import get_spark_session
        from pyspark_pipeline.ingestion import load_purchase_data
        from pyspark_pipeline.transformations import clean_and_transform_data

        spark = get_spark_session(app_name="KPIGenerationTestStandalone") # Changed app name for clarity
        logger.info("SparkSession obtained successfully for standalone KPI generation test.")

        csv_file_path = "../sample_purchases.csv"
        logger.info(f"Loading data from: {csv_file_path}")
        raw_df = load_purchase_data(spark, csv_file_path)

        if raw_df is None:
            logger.error("Raw data loading failed. Exiting KPI generation test.")
        else:
            logger.info("Cleaning and transforming data for KPI generation test...")
            cleaned_df = clean_and_transform_data(raw_df)

            if cleaned_df is None:
                logger.error("Data cleaning failed. Exiting KPI generation test.")
            else:
                cleaned_count = cleaned_df.count()
                logger.info(f"Cleaned data count for KPI generation: {cleaned_count}")
                if cleaned_count > 0 :
                    cleaned_df.show(5, truncate=False) # Show sample of data used for KPIs

                    logger.info("--- Testing KPI: Top Selling Products (Top 5) ---")
                    top_selling_df = calculate_top_selling_products(cleaned_df, top_n=5)
                    top_selling_df.show(truncate=False)

                    logger.info("--- Testing KPI: Total Revenue by Category ---")
                    total_revenue_df = calculate_total_revenue_by_category(cleaned_df, spark)
                    total_revenue_df.show(truncate=False)

                    logger.info("--- Testing KPI: Preferred Payment Method by Category ---")
                    preferred_payment_df = calculate_preferred_payment_method_by_category(cleaned_df, spark)
                    preferred_payment_df.show(truncate=False)

                    logger.info("--- Testing KPI: Purchase Frequency per User ---")
                    purchase_freq_df = calculate_purchase_frequency_per_user(cleaned_df)
                    purchase_freq_df.show(truncate=False)

                    logger.info("--- Testing KPI: Average Order Value ---")
                    avg_order_value = calculate_average_order_value(cleaned_df)
                    logger.info(f"Calculated Average Order Value: {avg_order_value:.2f}")
                else:
                    logger.warning("Cleaned data is empty. KPIs will reflect no data.")
                    # Test that KPI functions handle empty DFs
                    calculate_top_selling_products(cleaned_df, top_n=5).show()
                    calculate_total_revenue_by_category(cleaned_df, spark).show()
                    calculate_preferred_payment_method_by_category(cleaned_df, spark).show()
                    calculate_purchase_frequency_per_user(cleaned_df).show()
                    logger.info(f"AOV for empty data: {calculate_average_order_value(cleaned_df)}")


    except Exception as e:
        logger.error(f"An error occurred during KPI generation testing: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped after KPI generation test.")
pass

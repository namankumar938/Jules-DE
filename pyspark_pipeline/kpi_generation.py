# PySpark KPI Generation Logic will go here.
# This module will calculate KPIs from the transformed PySpark DataFrames.

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, lit, count as _count, coalesce, avg as _avg

# Define allowed values for categorical columns
# In a larger project, these might come from a shared constants module or configuration.
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
    if not isinstance(df, DataFrame):
        raise ValueError("Input must be a valid PySpark DataFrame.")

    top_products_df = df.groupBy("product_id", "product_name") \
        .agg(_sum("quantity").alias("total_quantity_sold")) \
        .orderBy(desc("total_quantity_sold")) \
        .select("product_id", "product_name", "total_quantity_sold") \
        .limit(top_n)

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
    if not isinstance(df, DataFrame):
        raise ValueError("Input df must be a valid PySpark DataFrame.")
    if not isinstance(spark_session, SparkSession):
        raise ValueError("spark_session must be a valid SparkSession object.")

    # Calculate revenue per item
    df_with_item_revenue = df.withColumn("item_revenue", col("price") * col("quantity"))

    # Aggregate revenue by category
    revenue_by_cat_df = df_with_item_revenue.groupBy("category") \
        .agg(_sum("item_revenue").alias("total_revenue_calculated")) # Renamed to avoid clash

    # Create a DataFrame of all allowed categories
    categories_list = [(cat,) for cat in ALLOWED_CATEGORIES] # List of tuples
    all_categories_df = spark_session.createDataFrame(categories_list, ["category_ref"])

    joined_df = all_categories_df.join(
        revenue_by_cat_df,
        all_categories_df["category_ref"] == revenue_by_cat_df["category"],
        "left_outer"
    ).select(
        all_categories_df["category_ref"].alias("category"),
        coalesce(revenue_by_cat_df["total_revenue_calculated"], lit(0.0)).alias("total_revenue")
    ).orderBy("category") # Added orderBy for consistent output

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
    if not isinstance(df, DataFrame):
        raise ValueError("Input df must be a valid PySpark DataFrame.")
    if not isinstance(spark, SparkSession):
        raise ValueError("spark must be a valid SparkSession object.")

    # Aggregate counts of payment methods per category from the actual data
    payment_counts_df = df.groupBy("category", "payment_method") \
        .agg(_count("*").alias("actual_count"))

    # Create a base DataFrame with all combinations of ALLOWED_CATEGORIES and ALLOWED_PAYMENT_METHODS
    all_combinations = []
    for cat in ALLOWED_CATEGORIES:
        for pm in ALLOWED_PAYMENT_METHODS:
            all_combinations.append((cat, pm))

    base_df = spark.createDataFrame(all_combinations, ["category_base", "payment_method_base"])

    # Left join the base DataFrame with the aggregated counts
    # Use aliases to avoid column name ambiguity after join
    result_df = base_df.join(
        payment_counts_df,
        (base_df["category_base"] == payment_counts_df["category"]) & \
        (base_df["payment_method_base"] == payment_counts_df["payment_method"]),
        "left_outer"
    ).select(
        base_df["category_base"].alias("category"),
        base_df["payment_method_base"].alias("payment_method"),
        coalesce(payment_counts_df["actual_count"], lit(0)).alias("count")
    ).orderBy("category", "payment_method") # Order for consistent output

    return result_df

def calculate_purchase_frequency_per_user(df: DataFrame) -> DataFrame:
    """
    Counts how many items (rows) are associated with each user_id.

    Args:
        df: Input DataFrame (should be cleaned and transformed).

    Returns:
        A DataFrame with "user_id" and "item_count", ordered by item_count descending.
    """
    if not isinstance(df, DataFrame):
        raise ValueError("Input must be a valid PySpark DataFrame.")

    frequency_df = df.groupBy("user_id") \
        .agg(_count("*").alias("item_count")) \
        .orderBy(desc("item_count"))

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
    if not isinstance(df, DataFrame):
        raise ValueError("Input must be a valid PySpark DataFrame.")

    if df.rdd.isEmpty(): # Check if DataFrame is empty
        return 0.0

    # Calculate revenue per item
    df_with_item_revenue = df.withColumn("item_revenue", col("price") * col("quantity"))

    # Sum revenue per order
    order_totals_df = df_with_item_revenue.groupBy("order_id") \
        .agg(_sum("item_revenue").alias("order_total_value"))

    if order_totals_df.rdd.isEmpty(): # Check if there are any orders after grouping
        return 0.0

    # Calculate average of these order totals
    aov_df = order_totals_df.agg(_avg("order_total_value").alias("average_order_value"))

    # Collect the result (DataFrame will have one row, one column)
    aov_result = aov_df.collect()

    if not aov_result or aov_result[0]["average_order_value"] is None:
        return 0.0 # Handles case where agg results in null (e.g. if order_totals_df was empty after all)

    return float(aov_result[0]["average_order_value"])


if __name__ == '__main__':
    import sys
    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    spark = None
    try:
        from pyspark_pipeline.main import get_spark_session
        from pyspark_pipeline.ingestion import load_purchase_data
        from pyspark_pipeline.transformations import clean_and_transform_data

        spark = get_spark_session(app_name="KPIGenerationTest")
        print("SparkSession obtained successfully.")

        csv_file_path = "../sample_purchases.csv"
        print(f"Loading data from: {csv_file_path}")
        raw_df = load_purchase_data(spark, csv_file_path)

        print("\nCleaning and transforming data...")
        cleaned_df = clean_and_transform_data(raw_df)

        print("\nCleaned Data Sample (first 5 rows):")
        cleaned_df.show(5, truncate=False)
        print(f"Cleaned data count: {cleaned_df.count()}")

        print("\nCalculating Top Selling Products (Top 5)...")
        top_selling_df = calculate_top_selling_products(cleaned_df, top_n=5)
        top_selling_df.show(truncate=False)

        print("\nCalculating Total Revenue by Category...")
        total_revenue_df = calculate_total_revenue_by_category(cleaned_df, spark)
        total_revenue_df.show(truncate=False)

        print("\nCalculating Preferred Payment Method by Category...")
        preferred_payment_df = calculate_preferred_payment_method_by_category(cleaned_df, spark)
        preferred_payment_df.show(truncate=False)

        print("\nCalculating Purchase Frequency per User...")
        purchase_freq_df = calculate_purchase_frequency_per_user(cleaned_df)
        purchase_freq_df.show(truncate=False)

        print("\nCalculating Average Order Value...")
        avg_order_value = calculate_average_order_value(cleaned_df)
        print(f"Average Order Value: {avg_order_value:.2f}")

    except Exception as e:
        print(f"An error occurred during testing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            spark.stop()
            print("\nSparkSession stopped.")
pass

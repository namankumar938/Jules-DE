import builtins # For explicit builtins.round
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count, desc, round as pyspark_round

def calculate_total_revenue(df: DataFrame, amount_col="total_purchase_amount") -> float:
    """Calculates the total revenue from the 'total_purchase_amount' column."""
    if amount_col not in df.columns:
        print(f"Warning: Column '{amount_col}' not found. Cannot calculate total revenue.")
        return 0.0

    print(f"Calculating total revenue from column '{amount_col}'...")
    total_revenue_df = df.agg(spark_sum(col(amount_col)).alias("total_revenue"))
    total_revenue = total_revenue_df.collect()[0]["total_revenue"]
    total_revenue = builtins.round(total_revenue, 2) if total_revenue is not None else 0.0 # Use builtins.round
    print(f"Total Revenue: ${total_revenue:,.2f}")
    return total_revenue

def calculate_revenue_by_category(df: DataFrame, category_col="category", amount_col="total_purchase_amount") -> DataFrame:
    """Calculates total revenue grouped by product category."""
    if category_col not in df.columns or amount_col not in df.columns:
        print(f"Warning: Columns '{category_col}' or '{amount_col}' not found. Cannot calculate revenue by category.")
        return df.sparkSession.createDataFrame([], df.schema) # Return empty DF with same schema or handle differently

    print(f"Calculating revenue by category (grouping by '{category_col}', summing '{amount_col}')...")
    revenue_by_cat_df = df.groupBy(col(category_col)) \
                           .agg(pyspark_round(spark_sum(col(amount_col)), 2).alias("total_revenue")) \
                           .orderBy(desc("total_revenue"))
    print("Revenue by Product Category:")
    revenue_by_cat_df.show(truncate=False)
    return revenue_by_cat_df

def calculate_top_n_selling_products(df: DataFrame, product_col="product_name", amount_col="total_purchase_amount", top_n=5) -> DataFrame:
    """Calculates the top N selling products by revenue."""
    if product_col not in df.columns or amount_col not in df.columns:
        print(f"Warning: Columns '{product_col}' or '{amount_col}' not found. Cannot calculate top selling products.")
        return df.sparkSession.createDataFrame([], df.schema)

    print(f"Calculating top {top_n} selling products (grouping by '{product_col}', summing '{amount_col}')...")
    top_products_df = df.groupBy(col(product_col)) \
                         .agg(pyspark_round(spark_sum(col(amount_col)), 2).alias("total_revenue")) \
                         .orderBy(desc("total_revenue")) \
                         .limit(top_n)
    print(f"Top {top_n} Selling Products by Revenue:")
    top_products_df.show(truncate=False)
    return top_products_df

def calculate_preferred_payment_methods(df: DataFrame, payment_col="payment_method") -> DataFrame:
    """Calculates the count of transactions by payment method."""
    if payment_col not in df.columns:
        print(f"Warning: Column '{payment_col}' not found. Cannot calculate preferred payment methods.")
        return df.sparkSession.createDataFrame([], df.schema)

    print(f"Calculating preferred payment methods (counting occurrences of '{payment_col}')...")
    payment_methods_df = df.groupBy(col(payment_col)) \
                            .agg(spark_count("*").alias("transaction_count")) \
                            .orderBy(desc("transaction_count"))
    print("Preferred Payment Methods by Transaction Count:")
    payment_methods_df.show(truncate=False)
    return payment_methods_df

def calculate_all_kpis(df: DataFrame, top_n_products=5) -> dict:
    """
    Calculates all defined KPIs and returns them in a dictionary.
    The results can be a mix of values and DataFrames.
    """
    if df is None:
        print("Error: Input DataFrame is None. Cannot calculate KPIs.")
        return {}

    print("\n--- Starting KPI Generation ---")

    kpis = {
        "total_revenue": calculate_total_revenue(df),
        "revenue_by_category": calculate_revenue_by_category(df),
        "top_selling_products": calculate_top_n_selling_products(df, top_n=top_n_products),
        "preferred_payment_methods": calculate_preferred_payment_methods(df)
    }

    print("\n--- KPI Generation Complete ---")
    return kpis

if __name__ == "__main__":
    from pyspark_scripts.spark_utils import get_spark_session, stop_spark_session
    from pyspark_scripts.ingestion import load_data
    from pyspark_scripts.transformation import transform_data

    spark = None
    try:
        spark = get_spark_session(app_name="KPIGenerationTest")
        if spark:
            # Load and transform data using existing modules
            raw_df = load_data(spark, file_path="data/sample_purchases.csv")
            if raw_df:
                transformed_df = transform_data(raw_df)
                if transformed_df:
                    # Calculate all KPIs
                    all_kpis = calculate_all_kpis(transformed_df, top_n_products=5)

                    print("\n--- All Calculated KPIs (from test block) ---")
                    print(f"Overall Total Revenue: ${all_kpis['total_revenue']:,.2f}")

                    print("\nRevenue by Category (DataFrame):")
                    all_kpis['revenue_by_category'].show()

                    print("\nTop Selling Products (DataFrame):")
                    all_kpis['top_selling_products'].show()

                    print("\nPreferred Payment Methods (DataFrame):")
                    all_kpis['preferred_payment_methods'].show()
                else:
                    print("KPI test skipped: Could not transform data.")
            else:
                print("KPI test skipped: Could not load raw data.")
    except Exception as e:
        print(f"An error occurred during KPI generation module test: {e}")
    finally:
        if spark:
            stop_spark_session(spark)
    print("KPI generation module test finished.")

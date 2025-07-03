from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def filter_by_min_age(df: DataFrame, age_column: str, min_age: int) -> DataFrame:
    """Filters DataFrame to keep rows where age_column is greater than min_age."""
    return df.filter(col(age_column) > min_age)

def drop_na_values(df: DataFrame, subset_columns: list[str]) -> DataFrame:
    """Drops rows with NA values in the specified subset of columns."""
    return df.dropna(subset=subset_columns)

def rename_column(df: DataFrame, old_column_name: str, new_column_name: str) -> DataFrame:
    """Renames a column in the DataFrame."""
    return df.withColumnRenamed(old_column_name, new_column_name)

def add_age_offset_column(df: DataFrame, new_column_name: str, age_column_name: str, offset: int) -> DataFrame:
    """Adds a new column by adding an offset to an existing age column."""
    return df.withColumn(new_column_name, col(age_column_name) + offset)

# Example of a more complex chained transformation helper (optional)
def apply_standard_customer_preprocessing(
    df: DataFrame,
    age_col: str = "age",
    min_age_threshold: int = 18,
    id_col: str = "user_id",
    new_id_col: str = "customer_id",
    na_check_cols: list[str] = ["name", "email"]
) -> DataFrame:
    """
    Applies a standard sequence of preprocessing steps for customer data.
    - Filters by minimum age.
    - Drops NA values in critical columns.
    - Renames user ID column.
    """
    df = filter_by_min_age(df, age_col, min_age_threshold)
    df = drop_na_values(df, na_check_cols)
    df = rename_column(df, id_col, new_id_col)
    return df

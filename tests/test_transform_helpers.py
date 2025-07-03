import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row
from utils.transform_helpers import (
    filter_by_min_age,
    drop_na_values,
    rename_column,
    add_age_offset_column,
    apply_standard_customer_preprocessing
)

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-spark-session") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_df(spark_session):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True)
    ])
    data = [
        ("1", "Alice", "alice@example.com", 25, "sales"),
        ("2", "Bob", None, 30, "hr"),
        ("3", "Charlie", "charlie@example.com", 17, "sales"),
        ("4", "David", "david@example.com", 40, "marketing"),
        ("5", "Eve", "eve@example.com", 22, "sales"),
        ("6", "Mallory", "mallory@example.com", None, "hr"),
        ("7", "Trent", "trent@example.com", 35, None)
    ]
    return spark_session.createDataFrame(data, schema)

def test_filter_by_min_age(sample_df):
    filtered_df = filter_by_min_age(sample_df, "age", 18)
    assert filtered_df.count() == 4  # Alice, Bob, David, Eve, Trent (age=None is not > 18)
    assert filtered_df.filter("age <= 18").count() == 0

def test_drop_na_values(sample_df):
    # Drop NAs from 'email'
    df_dropped_email = drop_na_values(sample_df, subset_columns=["email"])
    assert df_dropped_email.count() == 6 # Bob's email was None
    assert df_dropped_email.filter("email IS NULL").count() == 0

    # Drop NAs from 'age'
    df_dropped_age = drop_na_values(sample_df, subset_columns=["age"])
    assert df_dropped_age.count() == 6 # Mallory's age was None
    assert df_dropped_age.filter("age IS NULL").count() == 0

    # Drop NAs from 'email' OR 'age'
    df_dropped_multiple = drop_na_values(sample_df, subset_columns=["email", "age"])
    assert df_dropped_multiple.count() == 5 # Bob and Mallory had Nones
    assert df_dropped_multiple.filter("email IS NULL OR age IS NULL").count() == 0

def test_rename_column(sample_df):
    renamed_df = rename_column(sample_df, "user_id", "customer_id")
    assert "customer_id" in renamed_df.columns
    assert "user_id" not in renamed_df.columns

def test_add_age_offset_column(sample_df):
    offset_df = add_age_offset_column(sample_df, "age_plus_10", "age", 10)
    assert "age_plus_10" in offset_df.columns

    # Check a specific row (Alice, age 25 -> 35)
    alice_row = offset_df.filter("name == 'Alice'").first()
    if alice_row:
        assert alice_row["age_plus_10"] == 35
    else:
        pytest.fail("Alice's row not found after transformation")

    # Check row with null age (Mallory, age None -> None)
    mallory_row = offset_df.filter("name == 'Mallory'").first()
    if mallory_row:
        assert mallory_row["age_plus_10"] is None
    else:
        pytest.fail("Mallory's row not found")


def test_apply_standard_customer_preprocessing(sample_df):
    processed_df = apply_standard_customer_preprocessing(
        sample_df,
        age_col="age",
        min_age_threshold=20,
        id_col="user_id",
        new_id_col="customer_uid",
        na_check_cols=["name", "email"]
    )
    assert "customer_uid" in processed_df.columns
    assert "user_id" not in processed_df.columns

    # Expected count:
    # Original: 7
    # Filter age > 20: Alice (25), Bob (30), David (40), Eve (22), Trent (35) -> 5 rows. (Charlie 17, Mallory None removed)
    # Drop NA in name, email: Bob has no email, so Bob is removed. -> 4 rows (Alice, David, Eve, Trent)
    assert processed_df.count() == 4

    # Check that all remaining have age > 20
    assert processed_df.filter("age <= 20").count() == 0

    # Check that all remaining have non-null name and email
    assert processed_df.filter("name IS NULL OR email IS NULL").count() == 0

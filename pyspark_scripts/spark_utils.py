from pyspark.sql import SparkSession

def get_spark_session(app_name="RetailAnalyticsPySparkApp", master="local[*]"):
    """
    Gets an existing SparkSession or creates a new one.

    Args:
        app_name (str): The name of the Spark application.
        master (str): The Spark master URL. Defaults to "local[*]" for local mode.
                      For a cluster, this would be like "yarn" or "spark://host:port".

    Returns:
        pyspark.sql.SparkSession: The SparkSession object.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .getOrCreate()
        # You can add further configurations here if needed, e.g.:
        # .config("spark.driver.memory", "2g") \
        # .config("spark.executor.memory", "2g")

        print(f"SparkSession '{app_name}' initialized/retrieved successfully.")
        print(f"Spark version: {spark.version}")
        return spark
    except Exception as e:
        print(f"Error initializing SparkSession: {e}")
        # Depending on requirements, you might want to raise the exception
        # or handle it by returning None or exiting.
        raise

def stop_spark_session(spark: SparkSession):
    """
    Stops the given SparkSession.

    Args:
        spark (pyspark.sql.SparkSession): The SparkSession to stop.
    """
    if spark:
        try:
            spark.stop()
            print("SparkSession stopped successfully.")
        except Exception as e:
            print(f"Error stopping SparkSession: {e}")

if __name__ == "__main__":
    # Example usage (for testing the module directly)
    spark_instance = None
    try:
        print("Attempting to get Spark session...")
        spark_instance = get_spark_session(app_name="SparkUtilsTest")

        if spark_instance:
            print("Spark session obtained. Performing a simple operation...")
            data = [("Alice", 1), ("Bob", 2)]
            columns = ["name", "id"]
            df = spark_instance.createDataFrame(data, columns)
            df.show()
            print(f"Number of rows in test DataFrame: {df.count()}")
        else:
            print("Failed to obtain Spark session.")

    except Exception as e:
        print(f"An error occurred during SparkUtilsTest: {e}")
    finally:
        if spark_instance:
            print("Attempting to stop Spark session...")
            stop_spark_session(spark_instance)
    print("SparkUtilsTest finished.")

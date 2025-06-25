# Retail Data Analytics Pipeline (PySpark Version)

## Overview
This project is a PySpark-based data analytics pipeline designed to process customer purchase data from a CSV file. It leverages Apache Spark for distributed data processing to clean the data, calculate various Key Performance Indicators (KPIs), and then prints a summary of these KPIs to the console.

## Features (KPIs Generated)
The pipeline calculates and reports the following KPIs:
*   Top Selling Products
*   Total Revenue by Category
*   Preferred Payment Method by Category
*   Purchase Frequency per User
*   Average Order Value

## Robustness
*   **Structured Logging**: The pipeline uses Python's built-in `logging` module for structured and consistent log messages across all components, aiding in traceability and debugging.
*   **Error Handling**: Enhanced error handling is implemented, particularly in data ingestion and transformation steps, to manage potential issues gracefully (e.g., file not found, data processing errors).

## Directory Structure
*   `pyspark_pipeline/`: Directory containing the core PySpark pipeline modules.
    *   `__init__.py`: Makes `pyspark_pipeline` a Python package.
    *   `main.py`: Main script to orchestrate the entire PySpark pipeline.
    *   `ingestion.py`: Handles loading raw purchase data into Spark DataFrames.
    *   `transformations.py`: Contains logic for data cleaning and transformation using Spark DataFrame operations.
    *   `kpi_generation.py`: Responsible for calculating KPIs from the transformed Spark DataFrame.
    *   `logging_utils.py`: Utility for configuring the structured logging setup.
*   `data_models.py`: Defines the conceptual schemas for input data (still relevant for understanding input CSV structure).
*   `sample_purchases.csv`: Sample input CSV data used by the pipeline.
*   `tests/`: Contains unit tests (currently for the original Python version; PySpark tests would be a future addition).
*   Original Python files (e.g., `data_ingestion.py`, `data_processing.py`, `reporting.py`): These are now superseded by the modules in `pyspark_pipeline/` for the PySpark version of the pipeline.

## Data Format

### Input Data (`sample_purchases.csv`)
The input data is expected in a CSV format. The expected columns are:
`event_timestamp`, `user_id`, `order_id`, `product_id`, `product_name`, `category`, `price`, `quantity`, `payment_method`.
An explicit schema (defining data types like StringType, FloatType, IntegerType) is applied during data ingestion in the PySpark pipeline.

### Output KPIs
The calculated KPIs are printed to the console. Table-based KPIs (like Top Selling Products) are displayed using `DataFrame.show()`, and scalar KPIs (like Average Order Value) are printed directly.

## Setup and Dependencies
*   This project is written in Python 3 and uses Apache Spark.
*   **Dependencies:**
    *   `pyspark`: The PySpark library. Install using `pip install pyspark`.
*   **Environment:**
    *   A working Spark environment is required. For local execution, installing `pyspark` via pip is usually sufficient as it includes a bundled Spark distribution that can run in local mode.

## How to Run

### Prerequisites:
Ensure PySpark is installed in your Python environment:
```bash
pip install pyspark
```

### Running the Pipeline:
1.  Navigate to the root directory of the project.
2.  Execute the main pipeline script using:
    ```bash
    PYTHONPATH=. python -m pyspark_pipeline.main
    ```
    *   `PYTHONPATH=.` ensures that the `pyspark_pipeline` package (located in the current directory) is discoverable by Python if it's not installed as a site package.
    *   This command runs the `main.py` script from the `pyspark_pipeline` package, which orchestrates the entire process.
    *   The script uses `sample_purchases.csv` located in the project root by default.

    (Alternatively, for more complex deployments or cluster execution, `spark-submit` would typically be used, but for this project structure, the above command is suitable for local runs.)

### Logging Configuration
The pipeline logs informational messages by default. To enable more detailed DEBUG level logging (which includes showing samples of raw and cleaned DataFrames during the main pipeline execution if enabled in code):
1.  Open `pyspark_pipeline/main.py`.
2.  In the `main()` function, change the line `setup_logging()` (or `setup_logging(level=logging.INFO)`) to `setup_logging(level=logging.DEBUG)`.

## Modules Overview (`pyspark_pipeline/`)

*   **`main.py`**: Initializes the `SparkSession` and orchestrates the pipeline's execution flow. It calls functions from ingestion, transformations, and kpi_generation modules in sequence and prints the final KPI results.
*   **`ingestion.py`**: Defines how raw data from the input CSV file is loaded into a Spark DataFrame. This includes applying a predefined schema to ensure data types are correctly inferred and handled from the start.
*   **`transformations.py`**: Contains the `clean_and_transform_data` function, which applies various data cleaning rules to the DataFrame. This includes handling null or invalid values, normalizing string data (e.g., lowercase for categories), and filtering records based on predefined business rules (e.g., allowed categories, positive price/quantity).
*   **`kpi_generation.py`**: Houses functions to compute the defined Key Performance Indicators. Each function takes the cleaned Spark DataFrame as input and performs necessary aggregations, groupings, and calculations using Spark DataFrame operations to generate the KPI results.
*   **`logging_utils.py`**: Provides a `setup_logging` function to configure a standardized console logger. This ensures consistent log formatting (timestamp, logger name, level, message) throughout the application.

## How It Works (High-Level PySpark Flow)
1.  **SparkSession Initialization**: A `SparkSession` is created, serving as the entry point for Spark functionality (`main.py`).
2.  **Data Ingestion**: Purchase data is loaded from `sample_purchases.csv` into a Spark DataFrame. An explicit schema is applied during this stage to define data types for each column (`ingestion.py`).
3.  **Data Transformation**: The raw DataFrame is then processed by the `clean_and_transform_data` function in `transformations.py`. This step involves filtering out invalid or incomplete records, normalizing data (e.g., converting categories to lowercase), and ensuring data quality using Spark DataFrame transformations.
4.  **KPI Calculation**: The cleaned and transformed DataFrame is passed to various functions in `kpi_generation.py`. These functions use Spark's distributed computation capabilities (e.g., `groupBy`, `agg`, `sum`, `avg`, `count`) to calculate each of the defined KPIs.
5.  **Results Display**: The resulting KPI DataFrames (and the scalar Average Order Value) are then printed to the console for review (`main.py`). For DataFrame results, `DataFrame.show()` is used.
6.  **SparkSession Stop**: Finally, the `SparkSession` is stopped to release resources.

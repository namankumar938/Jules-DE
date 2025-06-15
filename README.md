# Retail Data Analytics Pipeline

## Overview
This project is a Python-based data analytics pipeline designed to process customer purchase data from a CSV file for a retail platform. It performs data cleaning and validation, calculates various Key Performance Indicators (KPIs), and then prints a formatted report of these KPIs to the console.

## Features (KPIs Generated)
The pipeline calculates and reports the following KPIs:
*   Top Selling Products
*   Total Revenue by Category
*   Preferred Payment Method by Category
*   Purchase Frequency per User
*   Average Order Value

## Directory Structure
*   `data_models.py`: Defines the schemas (as comments) for input purchase data and the conceptual structures for output KPIs.
*   `data_ingestion.py`: Contains the function `load_purchase_data_from_csv` for loading raw purchase data from CSV files.
*   `data_processing.py`: Includes functions for data cleaning and transformation (`clean_and_transform_data`), and all core KPI calculation logic (e.g., `calculate_top_selling_products`).
*   `reporting.py`: Responsible for formatting the calculated KPIs and printing a comprehensive report to the console via `generate_kpi_report`.
*   `sample_purchases.csv`: A sample CSV file illustrating the expected input data format.
*   `tests/`: Contains all unit tests for the project, ensuring reliability of data ingestion, processing, and reporting logic.

## Data Format

### Input Data (`sample_purchases.csv`)
The input data is expected in a CSV format. The column names and their conceptual data types are outlined in `data_models.py`. Key columns include:
*   `event_timestamp`: Timestamp of the purchase.
*   `user_id`: Unique identifier for the user.
*   `order_id`: Unique identifier for the order.
*   `product_id`: Unique identifier for the product.
*   `product_name`: Name of the product.
*   `category`: Product category (e.g., "men", "women", "kids").
*   `price`: Price of a single unit of the product (float).
*   `quantity`: Number of units purchased (integer).
*   `payment_method`: Method used for payment (e.g., "upi", "credit_card", "wallet").

### Output KPIs
The calculated KPIs are printed to the console in a human-readable format. The conceptual structure and data types for these KPIs are described in `data_models.py`.

## Setup and Dependencies
*   This project is written in Python 3.
*   It uses standard Python libraries (e.g., `csv`, `collections`, `unittest`, `io`). No external packages need to be installed beyond a standard Python 3 environment.

## How to Run

### To Generate a Sample Report:
To generate and print the KPI report using the sample data:
```bash
python reporting.py
```
This command executes the main block in `reporting.py`, which loads data from `sample_purchases.csv` (or an embedded comprehensive sample), processes it, and prints the full KPI report.

### To Run Unit Tests:
To execute the suite of unit tests:
```bash
python -m unittest discover tests
```
This command will discover and run all tests located in the `tests/` directory.

## Modules Overview

*   **`data_models.py`**: This file serves as a reference for data structures. It uses comments to outline the expected schema for input CSV data and the intended structure of the various KPI results before they are formatted for display.

*   **`data_ingestion.py`**: Provides the `load_purchase_data_from_csv` function, which is responsible for reading data from a specified CSV file path. It handles basic parsing and type conversion for `price` and `quantity` fields, skipping rows with conversion errors.

*   **`data_processing.py`**: This is the core analytical engine. It contains `clean_and_transform_data` to filter and validate raw data according to defined business rules (e.g., valid categories, positive price/quantity). It also houses all the KPI calculation functions (e.g., `calculate_top_selling_products`, `calculate_average_order_value`).

*   **`reporting.py`**: Focuses on the presentation of results. It imports calculated KPIs from `data_processing.py` and uses a set of `format_and_print_...` functions to display each KPI clearly in the console. The `generate_kpi_report` function orchestrates this process.

## How It Works (High-Level Flow)
1.  **Data Loading**: Purchase data is loaded from an input CSV file (e.g., `sample_purchases.csv`) by the `load_purchase_data_from_csv` function in `data_ingestion.py`.
2.  **Data Cleaning**: The raw list of dictionaries is then passed to the `clean_and_transform_data` function in `data_processing.py`. This function filters out invalid records, normalizes data (e.g., case for categories), and ensures data consistency.
3.  **KPI Calculation**: The cleaned data is used by the various `calculate_...` functions within `data_processing.py` (e.g., `calculate_total_revenue_by_category`, `calculate_purchase_frequency_per_user`) to compute the defined Key Performance Indicators.
4.  **Report Generation**: Finally, the `generate_kpi_report` function in `reporting.py` takes the cleaned data, calls the necessary calculation functions from `data_processing.py`, and then uses its own `format_and_print_...` helper functions to display the final KPI report on the console.

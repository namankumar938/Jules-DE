# Retail Data Analytics Pipeline

## Description
This project demonstrates a simple Python-based pipeline for processing raw customer purchase data. It involves generating sample data, cleaning it, transforming it by calculating additional metrics, and finally generating several key performance indicators (KPIs) commonly used in retail analytics.

## Features/KPIs Generated
The pipeline calculates and displays the following Key Performance Indicators:
*   **Top N Selling Products**: Identifies the most popular products based on:
    *   Total Quantity Sold
    *   Total Revenue Generated
*   **Total Revenue by Product Category**: Aggregates total revenue for each product category (e.g., electronics, women, men).
*   **Total Revenue by Payment Method**: Shows the breakdown of total revenue by payment methods used (e.g., credit card, UPI, wallet).
*   **Average Purchase Value**: Calculates the average monetary value of all transactions.
*   **Average Purchase Frequency per User**: Determines the average number of purchases made per unique user.

## File Structure
The project is organized into the following key files:
*   `main.py`: The main script that orchestrates the entire data processing flow from data generation to KPI display.
*   `data_structures.py`: Defines the `create_purchase_record` function, which acts as a schema for purchase records.
*   `sample_data_generator.py`: Contains logic to generate randomized sample purchase data for demonstration purposes.
*   `data_processor.py`: Houses all functions related to:
    *   `clean_data`: Cleaning raw data (handling missing values, type conversions).
    *   `transform_data`: Enriching data (e.g., calculating `total_purchase_value`).
    *   KPI calculation functions (e.g., `calculate_top_selling_products`, `calculate_total_revenue_by_category`, etc.).
*   `tests/test_data_processor.py`: Contains unit tests for the functions in `data_processor.py` to ensure their correctness.

## How to Run
This project is written in Python 3. No external libraries beyond the standard library are strictly required for the core logic.

1.  **Clone the repository (if applicable).**
2.  **Navigate to the project directory.**
3.  **To run the main data processing pipeline and view KPI output:**
    ```bash
    python main.py
    ```
4.  **To run the unit tests:**
    ```bash
    python -m unittest discover tests
    ```

## Output
When `main.py` is executed, it will:
1.  Print a sample of the first few transformed data records. Each record will include the calculated `total_purchase_value`.
2.  Print a section detailing the calculated Key Performance Indicators based on the generated sample dataset.
3.  During the data cleaning phase, warnings may be printed to the console if records are found with missing or invalid essential data (like price or quantity). This is an expected behavior of the `clean_data` function.

## Future Enhancements
This project serves as a basic demonstration. Potential future enhancements could include:
*   Connecting to real data sources (CSV, databases).
*   Storing processed data and KPIs in a database.
*   Developing an interactive dashboard for KPI visualization.
*   Implementing more advanced data cleaning, validation, and analytics.
*   Adding configuration management and robust logging.
*   Packaging the project for easier distribution.

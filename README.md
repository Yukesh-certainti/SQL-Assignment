# ShopEase Database Management Script

## Description

This Python script (`SQL.py`) provides a comprehensive solution for managing a retail shop database using PostgreSQL. It covers various database operations, from setting up the database schema and optimizing performance to data manipulation, advanced querying, and data export. The script is menu-driven, allowing users to interactively perform different tasks.

## Features

This script implements the following functionalities:

*   **Database Setup:**
    *   Creates tables for stores, employees, customers, suppliers, products, orders, order items, and payments (Part 1 - Task 1).
    *   Creates indexes to optimize query performance (Part 2 - Task 2).
    *   Creates views for top-selling products and store revenue (Part 3 - Task 3).
    *   Creates triggers to enforce data integrity (e.g., prevent out-of-stock orders, audit employee deletions) (Part 3 - Task 4).

*   **Data Management:**
    *   Inserts sample data into all tables for testing and demonstration (Part 4 - Task 5).
    *   Loads data from XLSX files into database tables (Part 4 - Task 6).
    *   Performs CRUD (Create, Read, Update, Delete) operations using stored procedures (Part 7 - Task 13).

*   **Advanced Querying:**
    *   Demonstrates Common Table Expressions (CTE) for recursive queries, like displaying employee hierarchies (Part 5 - Task 7).
    *   Illustrates various JOIN operations (INNER, LEFT, RIGHT, FULL, SELF JOIN) for data retrieval across tables (Part 5 - Task 9).
    *   Utilizes UNION and UNION ALL for combining result sets (Part 5 - Task 10).
    *   Performs data pivoting (transposing data) to display monthly sales using `crosstab` (Part 6 - Task 11).

*   **Data Modification and Deletion:**
    *   Updates data, such as increasing product prices, updating employee salaries, and adjusting product stock based on shipments (Part 6 - Task 10).
    *   Deletes data, including inactive customers, orders (with cascading deletion of order items), and truncates audit tables (Part 6 - Task 12).

*   **Data Export:**
    *   Exports monthly revenue per store to CSV or XLSX files (Part 8 - Task 14).
    *   Exports a list of customers and their total spending to CSV or XLSX files (Part 8 - Task 14).

## Prerequisites

Before running the script, ensure you have the following installed:

*   **Python 3.x:**
*   **psycopg2:** PostgreSQL adapter for Python. Install using pip:
    ```bash
    pip install psycopg2-binary
    ```
*   **pandas:** Data analysis library for Python, used for XLSX and CSV export and XLSX import. Install using pip:
    ```bash
    pip install pandas openpyxl
    ```
*   **PostgreSQL:** Database server.
    *   Ensure PostgreSQL is installed and running.
    *   Create a database named `ShopDetails` (or modify `DB_NAME` in the script).
    *   Create a PostgreSQL user named `postgres` with password `yukesh8585` (or modify `DB_USER` and `DB_PASSWORD` in the script).  **It is highly recommended to use stronger and different credentials for production environments.**

## Setup

1.  **Download the script:** Save the `db.py` Python script to your local machine.
2.  **Database Configuration:**
    *   Open the `db.py` file in a text editor.
    *   Modify the database connection details in the script if necessary to match your PostgreSQL setup:
        ```python
        DB_NAME = "ShopDetails"
        DB_USER = "postgres"
        DB_PASSWORD = "yukesh8585"
        DB_HOST = "localhost"
        DB_PORT = "5432"
        ```
3.  **Install Extensions (tablefunc):** The pivot table functionality uses the `tablefunc` extension in PostgreSQL. The script attempts to create it if it doesn't exist. You might need superuser privileges if it's not enabled in your PostgreSQL setup by default.

## How to Use

1.  **Run the script:** Open a terminal or command prompt, navigate to the directory where you saved `db.py`, and run the script using:
    ```bash
    python db.py
    ```

2.  **Menu Options:** The script will display a numbered menu:
    ```
    1. Create tables
    2. Create indexes
    3. Create Views
    4. Create Triggers
    5. Insert Values into Table
    6. Load Xlsx to DB
    7. Common Table Expressions - (CTE)- Employee hierarchy
    8. Query Data using Joins(INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, SELF JOIN)
    9. UNION and UNION ALL
    10. Update Data
    11. Pivot (Transpose Data)
    12. Delete Data
    13. CRUD Operations
    14. Data Export

    ->
    ```

3.  **Select a Task:** Enter the number corresponding to the task you want to perform and press **Enter**.

    *   **Example:** To create tables, enter `1` and press **Enter**.
    *   For data export, enter `14` and then choose export type (1 or 2) and file format (CSV or XLSX) when prompted.
    *   For loading data from XLSX (option 6), you will be prompted to enter the XLSX file path and the table name.

4.  **Follow Prompts:** The script will guide you through each selected task with further prompts if necessary.

## Tasks Covered and Menu Options Mapping

| Part | Task Description                                                 | Menu Option |
|------|-----------------------------------------------------------------|-------------|
| Part 1 | Task 1: Create Database Tables                                  | 1           |
| Part 2 | Task 2: Create Indexes                                       | 2           |
| Part 3 | Task 3: Create Views                                         | 3           |
| Part 3 | Task 4: Create Triggers                                      | 4           |
| Part 4 | Task 5: Insert Values into Tables                              | 5           |
| Part 4 | Task 6: Import Data from CSV/XLSX (XLSX implemented)          | 6           |
| Part 5 | Task 7: Common Table Expressions (CTE) - Employee hierarchy    | 7           |
| Part 5 | Task 9: Query Data using Joins                                  | 8           |
| Part 5 | Task 10: UNION and UNION ALL                                   | 9           |
| Part 6 | Task 11: Update Data                                         | 10          |
| Part 6 | Task 8: Pivot (Transpose Data)                               | 11          |
| Part 6 | Task 12: Delete Data                                         | 12          |
| Part 7 | Task 13: CRUD Operations (Stored Procedures)                | 13          |
| Part 8 | Task 14: Extract Results to CSV/XLSX (Data Export)           | 14          |


## File Exports

When using option `14` (Data Export), the script will generate the following files in the same directory as `db.py`:

*   **Monthly Revenue per Store:**
    *   `monthly_store_revenue.csv` (if CSV format is chosen)
    *   `monthly_store_revenue.xlsx` (if XLSX format is chosen)
    *   Contains monthly revenue data for each store, including 'Store Name', 'Month', 'Year', and 'Monthly Revenue'.

*   **Customer Total Spending:**
    *   `customer_total_spending.csv` (if CSV format is chosen)
    *   `customer_total_spending.xlsx` (if XLSX format is chosen)
    *   Contains a list of customers and their total spending, including 'Customer Name' and 'Total Spending'.

## Notes

*   **Error Handling:** The script includes basic error handling for database connections and operations. Check the console output for error messages if something goes wrong.
*   **Sample Data:** Option 5 inserts a significant amount of sample data. You can modify or extend the `insert_sample_data` function in the script to adjust the data being inserted.
*   **Security:** The provided database credentials in the script are for local development and testing purposes. **Do not use these credentials in production environments.** Implement proper security measures and credential management for production deployments.
*   **Dependencies:** Ensure all required Python libraries (`psycopg2`, `pandas`, `openpyxl`) are installed before running the script.
*   **PostgreSQL Setup:** Make sure your PostgreSQL server is running and configured correctly before running the script.

This README provides a comprehensive guide to using the `db.py` script for managing your ShopEase database. Use the menu options to explore and perform various database tasks as needed.

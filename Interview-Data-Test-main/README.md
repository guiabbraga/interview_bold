# Data Engineering Interview Test

## Overview

This test evaluates your ability to design and implement a complete ETL (Extract, Transform, Load) pipeline and perform data analysis using SQL. You'll be working with e-commerce data from multiple sources and formats, transforming it into a structured data warehouse, and extracting business insights.

## Task Description

You are tasked with building an ETL pipeline for an e-commerce company that needs to analyze its sales data. The company has data stored in a SQL Server database with intentional data quality issues to test your ETL processes.

Your goal is to create a data pipeline that:
1. Extracts data from the SQL Server database
2. Transforms the data (cleans, normalizes, and integrates it)
3. Loads the transformed data into a data warehouse
4. Enables business analysis through SQL queries

## Requirements

### 1. Data Extraction

- Extract data from the SQL Server database tables
- Handle the data quality issues appropriately
- Document any assumptions or issues encountered

### 2. Data Transformation

- Clean the data (handle missing values, duplicates, etc.)
- Normalize and standardize the data
- Create appropriate dimension and fact tables for a star schema
- Implement data quality checks 
- Document your transformation logic and decisions

### 3. Data Loading

- Create a database schema (use the provided `db_setup.sql` as a starting point)
- Load the transformed data into the database
- Ensure referential integrity and proper indexing

### 4. Data Warehouse Design

- Design a star schema for the e-commerce data warehouse
- Create dimension tables for:
  - Customer dimension (customer demographics, segments, etc.)
  - Product dimension (product details, categories, suppliers)
  - Time dimension (date hierarchies for trend analysis)
  - Store/Location dimension (if applicable)
- Create fact tables for:
  - Sales fact table (purchases, quantities, amounts)
  - Returns fact table (product returns, reasons, refunds)
- Implement slowly changing dimensions (SCDs) where appropriate
- Document your dimensional modeling decisions and trade-offs

### 5. Data Analysis

Write SQL queries to answer the following business questions:

1. Who are the top 10 customers by total purchase amount?

   SELECT TOP 10 
      c.customer_id,
      c.first_name,
      c.last_name,
      c.email,
      SUM(s.total_amount) AS total_spent
   FROM interview_dw.dbo.fact_sales s
   JOIN interview_dw.dbo.dim_customers c ON s.customer_id = c.customer_id
   GROUP BY 
      c.customer_id, c.first_name, c.last_name, c.email
   ORDER BY total_spent DESC;

2. What are the monthly sales trends over the past year?
   SELECT 
      FORMAT(s.purchase_date, 'yyyy-MM') AS year_month,
      SUM(s.total_amount) AS monthly_sales
   FROM interview_dw.dbo.fact_sales s
   WHERE s.purchase_date >= DATEADD(YEAR, -1, GETDATE())
   GROUP BY FORMAT(s.purchase_date, 'yyyy-MM')
   ORDER BY year_month;
3. Which product categories have the highest profit margins?

   SELECT 
      p.category,
      ROUND(AVG(p.selling_price - s.unit_price), 2) AS avg_profit_margin
   FROM interview_dw.dbo.fact_sales s
   JOIN interview_dw.dbo.dim_products p ON s.product_id = p.product_id
   GROUP BY p.category
   ORDER BY avg_profit_margin DESC;

4. How can customers be segmented based on their purchase behavior?

   SELECT 
      c.customer_id,
      c.first_name,
      c.last_name,
      COUNT(s.purchase_id) AS total_purchases,
      SUM(s.total_amount) AS total_spent,
      CASE
         WHEN COUNT(s.purchase_id) >= 50 THEN 'High Value'
         WHEN COUNT(s.purchase_id) BETWEEN 20 AND 49 THEN 'Medium Value'
         ELSE 'Low Value'
      END AS customer_segment
   FROM interview_dw.dbo.fact_sales s
   JOIN interview_dw.dbo.dim_customers c ON s.customer_id = c.customer_id
   GROUP BY c.customer_id, c.first_name, c.last_name
   ORDER BY total_spent DESC;

5. Are there any potential fraud patterns in the transactions?

   SELECT 
      c.customer_id,
      c.first_name,
      c.last_name,
      COUNT(DISTINCT r.return_id) AS total_returns,
      SUM(r.refund_amount) AS total_refunded,
      COUNT(CASE WHEN s.payment_status IN ('refunded', 'failed') THEN 1 END) AS risky_transactions
   FROM interview_dw.dbo.fact_returns r
   JOIN interview_dw.dbo.fact_sales s ON r.purchase_id = s.purchase_id
   JOIN interview_dw.dbo.dim_customers c ON r.customer_id = c.customer_id
   GROUP BY c.customer_id, c.first_name, c.last_name
   HAVING 
      SUM(r.refund_amount) > 1000 OR
      COUNT(CASE WHEN s.payment_status IN ('refunded', 'failed') THEN 1 END) >= 3
   ORDER BY total_refunded DESC;

## Getting Started

1. Clone this repository
2. Ensure Docker and Docker Compose are installed on your system
3. Run the automated setup script:
   - For Windows PowerShell: `.\setup.ps1`
   - For Windows Command Prompt: `setup.bat`
   - For Linux/Mac: `./setup.sh` (make sure to first run `chmod +x setup.sh` to make it executable)

This script will:
- Start the SQL Server container
- Create the database and tables
- Generate and insert data with intentional data quality issues
- Set up the Python environment and install dependencies

### Script Options

The setup scripts now support the following options:

- `--append`: Add new data without recreating tables (preserves existing data)
- `--clients N`: Generate N client records (default: 50)
- `--customers N`: Generate N customer records (default: 200)
- `--products N`: Generate N product records (default: 100)
- `--purchases N`: Generate N purchase records (default: 500)
- `--returns N`: Generate N return records (default: 100)

Examples:
- PowerShell: `.\setup.ps1 -AppendOnly` (add data without recreating tables)
- PowerShell: `.\setup.ps1 -Clients 20 -Customers 100` (create 20 clients and 100 customers)
- Command Prompt: `setup.bat --append` (add data without recreating tables)
- Command Prompt: `setup.bat --products 50 --purchases 200` (create 50 products and 200 purchases)
- Linux/Mac: `./setup.sh --append` (add data without recreating tables)
- Linux/Mac: `./setup.sh --clients 20 --customers 100` (create 20 clients and 100 customers)

Alternatively, you can manually run the following steps:

1. Start the Docker containers: `docker-compose up -d`
2. Wait for the database to be ready (typically takes about 30 seconds)
3. Install the required packages: `pip install -r requirements.txt` or `uv pip install -r requirements.txt`
4. Run the data insertion script: `python scripts/Insert_data.py`
   - Add `--append` flag to add data without recreating tables
   - Use `--help` to see all available options

## Project Structure

```
Data Engineering/
├── docker-compose.yaml    # Docker configuration for SQL Server
├── setup.ps1              # PowerShell setup script
├── setup.bat              # Batch setup script
├── setup.sh               # Shell script for Linux/Mac
├── requirements.txt       # Python dependencies
├── scripts/
│   ├── Insert_data.py     # Script to generate and insert data with quality issues
│   └── sql_server_setup.sql   # SQL script to create database schema
└── README.md              # Project documentation
```

## Data Quality Issues

This project intentionally includes various data quality issues to test ETL processes:

1. **Inconsistent Date Formats**: Dates are stored in various formats like 'YYYY-MM-DD', 'DD/MM/YYYY', 'MM-DD-YYYY', etc.
2. **Invalid Dates**: Some dates are completely invalid (e.g., '31/02/2022', 'INVALID DATE', 'N/A')
3. **Mixed Data Types**: Product IDs sometimes contain names instead of numeric IDs
4. **Negative Values**: Quantities and prices can be negative
5. **Calculation Errors**: Total amounts may not match quantity × unit price

These issues are designed to test your data cleaning and validation processes.

## Data Quality Challenge

As part of this challenge, you are expected to:

1. **Identify Data Quality Issues**: Develop methods to detect and quantify the data quality problems in the database.
2. **Implement Data Cleaning**: Create transformation processes that address the identified issues.
3. **Document Your Approach**: Explain your methodology for identifying and resolving data quality problems.

**Note for Evaluators**: A reference data quality verification script is available. This script can be used to assess candidate solutions but should not be shared with candidates during the interview process.

## Deliverables

1. Complete ETL pipeline code
2. Database schema and loaded data
3. SQL queries for the business questions
4. Documentation of your approach, including:
   - Data quality issues identified and how you handled them
   - Transformation logic and decisions
   - Database schema design
   - Any assumptions made
   - Challenges encountered and how you solved them

## Evaluation Criteria

Your solution will be evaluated based on:
`` 
1. **Code Quality**:
   - Readability and organization
   - Error handling
   - Documentation
   - Appropriate use of libraries

2. **ETL Design**:
   - Efficiency of data processing
   - Handling of different data formats
   - Data validation and cleaning approach

3. **Database Design**:
   - Schema design
   - Appropriate indexing
   - Normalization level

4. **SQL Proficiency**:
   - Query correctness
   - Query efficiency
   - Use of appropriate SQL features

5. **Problem-Solving**:
   - Approach to data quality issues
   - Handling of edge cases
   - Scalability considerations

## Time Allocation

You have 2 days to complete this test. Focus on demonstrating your approach and thought process rather than completing every aspect perfectly.

## Bonus Points

- Implement incremental loading strategy
- Add data visualization component
- Design for scalability (e.g., using Spark)
- Implement data quality monitoring 
- Create a dashboard for the business insights

Good luck!
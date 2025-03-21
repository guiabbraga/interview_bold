# interview_bold

During the initial setup of the project, I encountered technical issues related to the ODBC Driver 17, which was not functioning correctly in my environment. To resolve this, I updated the configuration to use ODBC Driver 18, and adjusted the Python dependencies to be compatible with Python 3.12. These changes caused an unexpected delay in getting started, but once resolved, I focused on delivering the best solution I could within the time frame I had promised (Tuesday to Friday).

While I know the result may not be perfect, I made every effort to address the requirements thoughtfully and thoroughly, and I genuinely hope this submission aligns with the expectations and goals of the assignment.

I deeply appreciate the time and consideration given to this process, and I’m very grateful for the opportunity to participate.

Thank you for your understanding

## 📁 Schema Overview

We structured the warehouse with the following tables:

### 🧩 Dimension Tables

| Table          | Description                          |
|----------------|--------------------------------------|
| `dim_clients`  | Contains client metadata             |
| `dim_customers`| Contains customer demographics       |
| `dim_products` | Product and category data            |

### 📊 Fact Tables

| Table          | Description                          |
|----------------|--------------------------------------|
| `fact_sales`   | All purchase transactions            |
| `fact_returns` | Product return transactions          |


### ✅ ETL Pipeline Development

- **File Used:** `etl_template.py`
- We created a class-based ETL pipeline to:
  - Extract data from raw tables (`Insert_data.py`)
  - Clean and transform datasets with built-in data quality handling
  - Load transformed data into `interview_dw.dbo` tables using `SQLAlchemy` and `pyodbc`


### ⚠️ Data Quality Issues Handled

| Issue Detected                                  | Resolution Strategy                                                                 |
|--------------------------------------------------|--------------------------------------------------------------------------------------|
| Inconsistent date formats                        | Used `pd.to_datetime(..., errors='coerce')`                          |
| Invalid or unknown `product_id` values (names)   | Mapped product names to IDs using a lookup from `dim_products`, dropped nulls       |
| Negative prices/quantities                       | Applied `.abs()` to ensure all numeric values are positive                          |
| `SettingWithCopyWarning` from pandas             | Solved using `.loc[:, col] = ...` after DataFrame filtering           |
| Duplicated primary keys on insert                | Applied `drop_duplicates()` before load and ensured full table refresh when needed  |
| Foreign key violations                           | Implemented checks to exclude facts with non-existent dimension keys                |

---

## 🔧 Difficulties & Fixes

| Problem                                                       | Fix Applied                                                                 |
|----------------------------------------------------------------|------------------------------------------------------------------------------|
| `SettingWithCopyWarning`                                      | Always used `.loc[:, col]` for safe assignment                 |
| `AttributeError: Can only use .str accessor...`               | Checked column type before using `.str` methods                             |
| `TypeError: cannot convert the series to <class 'int'>`       | Used `pd.to_numeric(...).astype('Int64')` for nullable int conversion       |
| `IntegrityError: Violation of PRIMARY KEY constraint`         | Replaced or deduplicated tables before insert                               |
| `FOREIGN KEY constraint violation on product_id or client_id` | Ensured facts only reference existing dimension keys, or dropped rows       |
                               


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

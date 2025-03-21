IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'interview_dw')
BEGIN
    CREATE DATABASE interview_dw;
END
GO

-- Use the database
USE interview_dw;
GO

-- Only drop tables if the parameter @drop_tables = 1
-- This parameter will be passed from the setup scripts
IF OBJECT_ID('tempdb..#setup_params') IS NOT NULL DROP TABLE #setup_params;
CREATE TABLE #setup_params (drop_tables BIT);
INSERT INTO #setup_params (drop_tables) VALUES (ISNULL($(drop_tables), 1));

DECLARE @should_drop_tables BIT;
SELECT @should_drop_tables = drop_tables FROM #setup_params;

-- Drop tables only if @should_drop_tables = 1
IF @should_drop_tables = 1
BEGIN
    PRINT 'Dropping existing tables...';
    IF OBJECT_ID('dbo.dim_products', 'U') IS NOT NULL DROP TABLE dbo.dim_products;
    IF OBJECT_ID('dbo.dim_customers', 'U') IS NOT NULL DROP TABLE dbo.dim_customers;
    IF OBJECT_ID('dbo.dim_clients', 'U') IS NOT NULL DROP TABLE dbo.dim_clients;
    IF OBJECT_ID('dbo.fact_returns', 'U') IS NOT NULL DROP TABLE dbo.fact_returns;
    IF OBJECT_ID('dbo.fact_sales', 'U') IS NOT NULL DROP TABLE dbo.fact_sales;
END
ELSE
BEGIN
    PRINT 'Keeping existing tables and data...';
END

-- Create tables if they don't exist
IF OBJECT_ID('dbo.dim_clients', 'U') IS NULL
BEGIN
    -- Create dim_clients table
    CREATE TABLE dbo.dim_clients (
        client_id INT PRIMARY KEY,
        company_name NVARCHAR(255),
        contact_name NVARCHAR(255),
        email NVARCHAR(255),
        phone NVARCHAR(50),
        city NVARCHAR(100),
        state NVARCHAR(100),
        country NVARCHAR(100),
        status NVARCHAR(50),
    );
END

IF OBJECT_ID('dbo.dim_customers', 'U') IS NULL
BEGIN
    -- Create dim_customers table
    CREATE TABLE dbo.dim_customers (
        customer_id INT PRIMARY KEY,
        first_name NVARCHAR(255),
        last_name NVARCHAR(255),
        email NVARCHAR(255),
        phone NVARCHAR(50),
        city NVARCHAR(100),
        state NVARCHAR(100),
        country NVARCHAR(100),
        birth_date DATE,
    );
END

IF OBJECT_ID('dbo.dim_products', 'U') IS NULL
BEGIN
    -- Create dim_products table
    CREATE TABLE dbo.dim_products (
        product_id INT PRIMARY KEY,
        product_name NVARCHAR(255),
        category NVARCHAR(100),
        sub_category NVARCHAR(100),
        supplier NVARCHAR(255),
        selling_price DECIMAL(18,2),
        is_active BIT
    );
END

IF OBJECT_ID('dbo.fact_sales', 'U') IS NULL
BEGIN
    -- Create fact_sales table
    CREATE TABLE dbo.fact_sales (
        purchase_id INT PRIMARY KEY,
        client_id INT,
        customer_id INT,
        product_id INT,
        purchase_date DATE,
        quantity INT,
        unit_price DECIMAL(18,2),
        total_amount DECIMAL(18,2),
        payment_method NVARCHAR(50),
        payment_status NVARCHAR(50),
        FOREIGN KEY (client_id) REFERENCES dim_clients(client_id),
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
    );
END

IF OBJECT_ID('dbo.fact_returns', 'U') IS NULL
BEGIN
    -- Create fact_returns table
    CREATE TABLE dbo.fact_returns (
        return_id INT PRIMARY KEY,
        purchase_id INT,
        client_id INT,
        customer_id INT,
        product_id INT,
        return_date DATE,
        quantity INT,
        refund_amount DECIMAL(18,2),
        status NVARCHAR(50),
        FOREIGN KEY (purchase_id) REFERENCES fact_sales(purchase_id),
        FOREIGN KEY (client_id) REFERENCES dim_clients(client_id),
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
    );
END

-- Clean up temporary table
DROP TABLE #setup_params;
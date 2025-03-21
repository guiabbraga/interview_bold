-- SQL Server database setup script with intentional data quality issues

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'interview_db')
BEGIN
    CREATE DATABASE interview_db;
END
GO

-- Use the database
USE interview_db;
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
    IF OBJECT_ID('dbo.returns', 'U') IS NOT NULL DROP TABLE dbo.returns;
    IF OBJECT_ID('dbo.purchases', 'U') IS NOT NULL DROP TABLE dbo.purchases;
    IF OBJECT_ID('dbo.products', 'U') IS NOT NULL DROP TABLE dbo.products;
    IF OBJECT_ID('dbo.customer', 'U') IS NOT NULL DROP TABLE dbo.customer;
    IF OBJECT_ID('dbo.client', 'U') IS NOT NULL DROP TABLE dbo.client;
END
ELSE
BEGIN
    PRINT 'Keeping existing tables and data...';
END

-- Create tables if they don't exist
IF OBJECT_ID('dbo.client', 'U') IS NULL
BEGIN
    -- Create client table
    CREATE TABLE dbo.client (
        client_id INT PRIMARY KEY,
        company_name NVARCHAR(100),
        contact_name NVARCHAR(100),
        email NVARCHAR(100),
        phone NVARCHAR(20),
        address NVARCHAR(200),
        city NVARCHAR(50),
        state NVARCHAR(50),
        zip_code NVARCHAR(20),
        country NVARCHAR(50),
        registration_date NVARCHAR(20),  -- Intentional issue: storing dates as strings
        status NVARCHAR(20),
        credit_limit DECIMAL(10, 2),
        last_update DATE
    );
END

IF OBJECT_ID('dbo.customer', 'U') IS NULL
BEGIN
    -- Create customer table
    CREATE TABLE dbo.customer (
        customer_id INT PRIMARY KEY,
        client_id INT,
        first_name NVARCHAR(50),
        last_name NVARCHAR(50),
        email NVARCHAR(100),
        phone NVARCHAR(20),
        birth_date NVARCHAR(20),  -- Intentional issue: storing dates as strings
        address NVARCHAR(200),
        city NVARCHAR(50),
        state NVARCHAR(50),
        zip_code NVARCHAR(20),
        country NVARCHAR(50),
        registration_date NVARCHAR(20),  -- Intentional issue: storing dates as strings
        last_login NVARCHAR(30),
        updated_at DATETIME, -- Intentional issue: inconsistent date format
        last_update DATE
    );
END

IF OBJECT_ID('dbo.products', 'U') IS NULL
BEGIN
    -- Create products table
    CREATE TABLE dbo.products (
        product_id INT PRIMARY KEY,
        product_name NVARCHAR(100),
        description NVARCHAR(500),
        category NVARCHAR(50),
        sub_category NVARCHAR(50),
        supplier NVARCHAR(100),
        cost_price DECIMAL(10, 2),
        selling_price DECIMAL(10, 2),
        stock_quantity INT,
        min_stock_level INT,
        is_active BIT,
        is_apparel BIT,  -- Nova coluna para identificar produtos de vestuário
        created_at DATETIME,
        updated_at DATETIME,
        last_update DATE
    );
END

IF OBJECT_ID('dbo.purchases', 'U') IS NULL
BEGIN
    -- Create purchases table with intentional issues
    CREATE TABLE dbo.purchases (
        purchase_id INT PRIMARY KEY,
        client_id INT,
        customer_id INT,
        product_id NVARCHAR(100),  -- Intentional issue: storing product names instead of IDs
        purchase_date NVARCHAR(30),  -- Intentional issue: inconsistent date format
        quantity INT,  -- Will contain negative values
        unit_price DECIMAL(10, 2),  -- Will contain negative values
        total_amount DECIMAL(10, 2),
        payment_method NVARCHAR(50),
        payment_status NVARCHAR(20),
        shipping_address NVARCHAR(200),
        shipping_city NVARCHAR(50),
        shipping_state NVARCHAR(50),
        shipping_zip NVARCHAR(20),
        shipping_country NVARCHAR(50),
        shipping_date NVARCHAR(30),  -- Intentional issue: inconsistent date format
        delivery_date NVARCHAR(30),  -- Intentional issue: inconsistent date format
        notes NVARCHAR(500),
        last_update DATE
    );
END

IF OBJECT_ID('dbo.returns', 'U') IS NULL
BEGIN
    -- Create returns table with intentional issues
    CREATE TABLE dbo.returns (
        return_id INT PRIMARY KEY,
        purchase_id INT,
        client_id INT,
        customer_id INT,
        product_id NVARCHAR(100),  -- Intentional issue: storing product names instead of IDs
        return_date NVARCHAR(30),  -- Intentional issue: inconsistent date format
        quantity INT,  -- Will contain negative values
        reason NVARCHAR(200),
        refund_amount DECIMAL(10, 2),  -- Will contain negative values
        status NVARCHAR(20),
        processed_by NVARCHAR(100),
        notes NVARCHAR(500),
        last_update DATE
    );
END

-- Clean up temporary table
DROP TABLE #setup_params;
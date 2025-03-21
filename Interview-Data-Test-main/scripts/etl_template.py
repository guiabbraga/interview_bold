import pandas as pd
import sqlalchemy
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='etl_process.log'
)
logger = logging.getLogger('etl_process')

class ETLPipeline:
    def __init__(self, config):
        """
        Initialize ETL pipeline with configuration parameters.
        
        Args:
            config (dict): Configuration parameters for the ETL process
        """
        self.config = config
        self.source_conn = None
        self.target_conn = None
        
    def extract_data(self, table_name):
        """
        Extract data from source SQL Server database.
        
        Args:
            table_name (str): Name of the table to extract data from
            
        Returns:
            pd.DataFrame: DataFrame containing extracted data
        """
        try:
            query = f"SELECT * FROM {table_name} WHERE last_update > (SELECT MAX(last_update) FROM {table_name})"
            if pd.read_sql(query, self.source_conn).empty:
                query = f"SELECT * FROM {table_name}"

            return pd.read_sql(query, self.source_conn)
        except Exception as e:
            logger.error(f"Error extracting data from {table_name}: {str(e)}")
            raise

    def get_product_mapping(self):
        """Fetch product_id mappings from the products table."""
        
        query = "SELECT product_name, product_id FROM products"
        df_products = pd.read_sql(query, self.source_conn)
        
        return dict(zip(df_products['product_name'].str.lower().str.strip(), df_products['product_id']))

    def clean_dataframe(self, df):
        # Load product mapping dynamically
        for column in df.columns:

            if df[column].dtype in ['float64', 'int64']:
                df.loc[:, column] = df[column].fillna(0)

            if column in ['registration_date', 'birth_date', 'purchase_date', 'shipping_date', 'delivery_date', 'return_date', 'last_login', 'last_update']:
                df.loc[:, column] = pd.to_datetime(df[column], errors='coerce')

            if column in ['quantity', 'unit_price', 'refund_amount']:
                df.loc[:, column] = pd.to_numeric(df[column], errors='coerce')
                df.loc[:, column] = df[column].abs()

            if column == 'phone':
                df.loc[:, column] = df[column].str.replace(r'\D', '', regex=True) 

            if column == 'product_id':
                df.loc[:, 'product_id'] = pd.to_numeric(df['product_id'], errors='coerce').astype('Int64')

            if df[column].dtype == "object":
                df.loc[:, column] = df[column].astype(str).str.lower().str.strip()
                df.loc[:, column] = df[column].fillna("Unknown")

        return df

    def transform_client_data(self, df):
        """
        Transform client data.
        
        Args:
            df (pd.DataFrame): Client data
            
        Returns:
            pd.DataFrame: Transformed client data
        """

        logger.info("🔄 Transforming client data")
        
        df = self.clean_dataframe(df)

        df = df.drop_duplicates(subset=['client_id'])

        df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        df['status'] = df['status'].str.lower().str.replace(' ', '_')
        
        return df
    
    
    def transform_customer_data(self, df):
        """
        Transform customer data.
        
        Args:
            df (pd.DataFrame): Customer data
            
        Returns:
            pd.DataFrame: Transformed customer data
        """

        logger.info("🔄 Transforming customer data")

        df = df.drop_duplicates(subset=['customer_id'])

        df = self.clean_dataframe(df)

        df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        df['birth_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        df['last_login'] = pd.to_datetime(df['registration_date'], errors='coerce')        

        return df
    
    def transform_product_data(self, df):
        """
        Transform product data.
        
        Args:
            df (pd.DataFrame): Product data
            
        Returns:
            pd.DataFrame: Transformed product data
        """
        logger.info("🔄 Transforming product data")

        df = self.clean_dataframe(df)

        df = df.drop_duplicates(subset=['product_id'])

        return df
    
    def product_id_processing(self,df):
        product_mapping = self.get_product_mapping()

        df['product_id'] = df['product_id'].apply(lambda x: product_mapping.get(x.lower().strip(), None) if isinstance(x, str) else x)

        df = df.dropna(subset=['product_id'])

        return df

    def transform_purchase_data(self, df):
        """
        Transform purchase data.
        
        Args:
            df (pd.DataFrame): Purchase data
            
        Returns:
            pd.DataFrame: Transformed purchase data
        """

        logger.info("🔄 Transforming purchase data")


        df = self.product_id_processing(df)

        df = self.clean_dataframe(df)

        df.loc[:, 'payment_status'] = df['payment_status'].str.lower().str.replace(' ', '_')

        df = df.drop_duplicates(subset=['purchase_id'])

        return df
    
    def transform_return_data(self, df):
        """
        Transform return data.
        
        Args:
            df (pd.DataFrame): Return data
            
        Returns:
            pd.DataFrame: Transformed return data
        """

        logger.info("🔄 Transforming return data")

        df = self.product_id_processing(df)

        df = self.clean_dataframe(df)

        df.loc[:, 'status'] = df['status'].str.lower().str.replace(' ', '_')
        df = df.drop_duplicates(subset=['return_id'])
        return df
    
    def get_product_mapping(self):
        """Fetch product_id mappings from the products table."""
        
        query = "SELECT product_name, product_id FROM interview_db.dbo.products"
        df_products = pd.read_sql(query, self.source_conn)
        
        return dict(zip(df_products['product_name'].str.lower().str.strip(), df_products['product_id']))


    def validate_data(self, transformed_data):
        """
        Validate transformed data for quality issues.
        
        Args:
            transformed_data (dict): Dictionary containing transformed DataFrames
            
        Returns:
            dict: Dictionary containing validation results
        """
        validation_results = {}
        
        for table_name, df in transformed_data.items():
            table_results = {
                'missing_values': self.check_missing_values(df),
                'duplicates': self.check_duplicates(df),
                'negative_values': self.check_negative_values(df),
                'invalid_dates': self.check_invalid_dates(df)
            }
            validation_results[table_name] = table_results
            
        return validation_results
    
    def check_missing_values(self, df):
        """Check for missing values in DataFrame."""
        missing_values = df.isnull().sum()
        missing_dict = missing_values[missing_values > 0].to_dict()

        logger.info(f"Missing values detected: {missing_dict}")
        return missing_dict
    
    def check_duplicates(self, df):
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            df = df.drop_duplicates()
    
    def check_negative_values(self, df):
        """Check for negative values in numeric columns."""
        numeric_cols = df.select_dtypes(include=['number'])
        negatives = (numeric_cols < 0).sum()
        negative_dict = negatives[negatives > 0].to_dict()

        logger.info(f"Negative values detected: {negative_dict}")
        return negative_dict
    
    def check_invalid_dates(self, df):
        date_cols = df.select_dtypes(include=['datetime64'])
        invalid_dates = {col: df[col].isna().sum() for col in date_cols}

        logger.info(f"Invalid dates detected: {invalid_dates}")
        return invalid_dates
    
    def create_dimension_tables(self, transformed_data):
        """
        Create dimension tables for the data warehouse.
        
        Args:
            transformed_data (dict): Dictionary containing transformed DataFrames
            
        Returns:
            dict: Dictionary containing dimension tables
        """
        dimensions = {}

        dimensions['dim_clients'] = transformed_data['clients'][['client_id', 'company_name', 'contact_name', 'email', 'phone', 'city', 'state', 'country', 'status']]
        dimensions['dim_customers'] = transformed_data['customers'][['customer_id', 'first_name','last_name', 'email', 'phone', 'city', 'state', 'country', 'birth_date']]
        dimensions['dim_products'] = transformed_data['products'][['product_id', 'product_name', 'category', 'sub_category', 'supplier', 'selling_price', 'is_active']]

        logger.info("✅ Dimension tables created successfully")
        return dimensions
    
    def create_fact_tables(self, transformed_data):
        """
        Create fact tables for the data warehouse.
        
        Args:
            transformed_data (dict): Dictionary containing transformed DataFrames
            dimensions (dict): Dictionary containing dimension tables
            
        Returns:
            dict: Dictionary containing fact tables
        """
        facts = {}

        facts['fact_sales'] = transformed_data['purchases'][['purchase_id', 'client_id', 'customer_id', 'product_id', 'purchase_date', 'quantity', 'unit_price', 'total_amount', 'payment_method', 'payment_status']]
        facts['fact_returns'] = transformed_data['returns'][['return_id', 'purchase_id', 'client_id', 'customer_id', 'product_id', 'return_date', 'quantity', 'refund_amount', 'status']]

        logger.info("✅ Fact tables created successfully")
        return facts
    
    def load_data(self, tables):
        """
        Load transformed data into the target database.
        
        Args:
            tables (dict): Dictionary containing tables to load
        """
        try:
            for table_name, df in tables.items():
                df.to_sql(table_name, self.target_conn, if_exists='append', index=False)
                logger.info(f"Loaded {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def connect_to_source_database(self):
        """
        Establish connection to the source database.
        """
        try:
            server = self.config.get('source_server', os.getenv('DB_SERVER', 'localhost,1433'))
            database = self.config.get('source_database', os.getenv('DB_NAME', 'interview_db'))
            username = self.config.get('source_username', os.getenv('DB_USER', 'sa'))
            password = self.config.get('source_password', os.getenv('DB_PASSWORD', 'YourStrongPassword123!'))
            
            connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
            self.source_conn = sqlalchemy.create_engine(connection_string)
            logger.info("Connected to source database")
        except Exception as e:
            logger.error(f"Error connecting to source database: {str(e)}")
            raise
    
    def connect_to_target_database(self):
        """
        Establish connection to the target database.
        """
        try:
            server = self.config.get('target_server', os.getenv('DW_SERVER', 'localhost,1433'))
            database = self.config.get('target_database', os.getenv('DW_NAME', 'interview_dw'))
            username = self.config.get('target_username', os.getenv('DW_USER', 'sa'))
            password = self.config.get('target_password', os.getenv('DW_PASSWORD', 'YourStrongPassword123!'))
            
            connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
            self.target_conn = sqlalchemy.create_engine(connection_string)
            logger.info("Connected to target database")
        except Exception as e:
            logger.error(f"Error connecting to target database: {str(e)}")
            raise
    
    def close_connections(self):
        """Close database connections."""
        if self.source_conn:
            self.source_conn.dispose()
        if self.target_conn:
            self.target_conn.dispose()
        logger.info("Database connections closed")
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline."""
        try:
            logger.info("Starting ETL process")
            
            # Connect to databases
            self.connect_to_source_database()
            self.connect_to_target_database()

            # Extract
            logger.info("Extracting data from source database")
            extracted_data = {
                'clients': self.extract_data('client'),
                'customers': self.extract_data('customer'),
                'products': self.extract_data('products'),
                'purchases': self.extract_data('purchases'),
                'returns': self.extract_data('returns')
            }
            
            # Transform
            logger.info("Transforming data")
            transformed_data = {
                'clients': self.transform_client_data(extracted_data['clients']),
                'customers': self.transform_customer_data(extracted_data['customers']),
                'products': self.transform_product_data(extracted_data['products']),
                'purchases': self.transform_purchase_data(extracted_data['purchases']),
                'returns': self.transform_return_data(extracted_data['returns'])
            }
            
            # Validate
            logger.info("Validating data")
            validation_results = self.validate_data(transformed_data)
            
            # Create dimension and fact tables
            logger.info("Creating dimension tables")
            dimensions = self.create_dimension_tables(transformed_data)
            
            logger.info("Creating fact tables")
            facts = self.create_fact_tables(transformed_data)
            
            # Load
            logger.info("Loading dimension tables into data warehouse")
            self.load_data(dimensions)
            
            logger.info("Loading fact tables into data warehouse")
            self.load_data(facts)
            
            logger.info("ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise
        finally:
            self.close_connections()



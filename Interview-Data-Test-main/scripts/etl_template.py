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
            query = f"SELECT * FROM {table_name}"
            return pd.read_sql(query, self.source_conn)
        except Exception as e:
            logger.error(f"Error extracting data from {table_name}: {str(e)}")
            raise
    
    def transform_client_data(self, df):
        """
        Transform client data.
        
        Args:
            df (pd.DataFrame): Client data
            
        Returns:
            pd.DataFrame: Transformed client data
        """
        # TODO: Implement transformation logic
        # Example: Clean and standardize date formats
        # df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        return df
    
    def transform_customer_data(self, df):
        """
        Transform customer data.
        
        Args:
            df (pd.DataFrame): Customer data
            
        Returns:
            pd.DataFrame: Transformed customer data
        """
        # TODO: Implement transformation logic
        return df
    
    def transform_product_data(self, df):
        """
        Transform product data.
        
        Args:
            df (pd.DataFrame): Product data
            
        Returns:
            pd.DataFrame: Transformed product data
        """
        # TODO: Implement transformation logic
        return df
    
    def transform_purchase_data(self, df):
        """
        Transform purchase data.
        
        Args:
            df (pd.DataFrame): Purchase data
            
        Returns:
            pd.DataFrame: Transformed purchase data
        """
        # TODO: Implement transformation logic
        return df
    
    def transform_return_data(self, df):
        """
        Transform return data.
        
        Args:
            df (pd.DataFrame): Return data
            
        Returns:
            pd.DataFrame: Transformed return data
        """
        # TODO: Implement transformation logic
        return df
    
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
        # TODO: Implement missing values check
        return {}
    
    def check_duplicates(self, df):
        """Check for duplicate records in DataFrame."""
        # TODO: Implement duplicates check
        return {}
    
    def check_negative_values(self, df):
        """Check for negative values in numeric columns."""
        # TODO: Implement negative values check
        return {}
    
    def check_invalid_dates(self, df):
        """Check for invalid date formats."""
        # TODO: Implement date validation
        return {}
    
    def create_dimension_tables(self, transformed_data):
        """
        Create dimension tables for the data warehouse.
        
        Args:
            transformed_data (dict): Dictionary containing transformed DataFrames
            
        Returns:
            dict: Dictionary containing dimension tables
        """
        dimensions = {}
        
        # TODO: Implement dimension table creation
        # Example: Customer dimension
        # dimensions['dim_customer'] = self.create_customer_dimension(transformed_data['customers'])
        
        return dimensions
    
    def create_fact_tables(self, transformed_data, dimensions):
        """
        Create fact tables for the data warehouse.
        
        Args:
            transformed_data (dict): Dictionary containing transformed DataFrames
            dimensions (dict): Dictionary containing dimension tables
            
        Returns:
            dict: Dictionary containing fact tables
        """
        facts = {}
        
        # TODO: Implement fact table creation
        # Example: Sales fact table
        # facts['fact_sales'] = self.create_sales_fact(transformed_data['purchases'], dimensions)
        
        return facts
    
    def load_data(self, tables):
        """
        Load transformed data into the target database.
        
        Args:
            tables (dict): Dictionary containing tables to load
        """
        try:
            for table_name, df in tables.items():
                df.to_sql(table_name, self.target_conn, if_exists='replace', index=False)
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
            
            connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
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

            print(extracted_data)
            
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
            facts = self.create_fact_tables(transformed_data, dimensions)
            
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

if __name__ == "__main__":
    # Configuration
    config = {
        'source_server': os.getenv('DB_SERVER', 'localhost,1433'),
        'source_database': os.getenv('DB_NAME', 'interview_db'),
        'source_username': os.getenv('DB_USER', 'sa'),
        'source_password': os.getenv('DB_PASSWORD', 'YourStrongPassword123!'),
        'target_server': os.getenv('DW_SERVER', 'localhost,1433'),
        'target_database': os.getenv('DW_NAME', 'interview_dw'),
        'target_username': os.getenv('DW_USER', 'sa'),
        'target_password': os.getenv('DW_PASSWORD', 'YourStrongPassword123!')
    }
    
    # Run ETL pipeline
    etl = ETLPipeline(config)
    etl.run_pipeline()
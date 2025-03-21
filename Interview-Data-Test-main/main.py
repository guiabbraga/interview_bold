from scripts.etl_template import ETLPipeline
import os
import logging

logging.basicConfig(level=logging.INFO)

def main():
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

    # Create an instance of ETLPipeline
    etl = ETLPipeline(config)

    # Run the pipeline
    etl.run_pipeline()

if __name__ == "__main__":
    main()

#!/bin/bash
# Setup script for the Interview Data Engineering project
# This script configures the environment, starts containers and runs the data insertion script

# Define default values for parameters
APPEND_ONLY=false
CLIENTS=50
CUSTOMERS=200
PRODUCTS=100
PURCHASES=500
RETURNS=100

# Process command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --append)
      APPEND_ONLY=true
      shift
      ;;
    --clients)
      CLIENTS="$2"
      shift 2
      ;;
    --customers)
      CUSTOMERS="$2"
      shift 2
      ;;
    --products)
      PRODUCTS="$2"
      shift 2
      ;;
    --purchases)
      PURCHASES="$2"
      shift 2
      ;;
    --returns)
      RETURNS="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Function to display colored messages
print_message() {
  local color=$1
  local message=$2
  
  case $color in
    "green")
      echo -e "\033[0;32m$message\033[0m"
      ;;
    "yellow")
      echo -e "\033[0;33m$message\033[0m"
      ;;
    "red")
      echo -e "\033[0;31m$message\033[0m"
      ;;
    "cyan")
      echo -e "\033[0;36m$message\033[0m"
      ;;
    *)
      echo "$message"
      ;;
  esac
}

print_message "green" "Starting the Interview Data Engineering environment..."

if [ "$APPEND_ONLY" = true ]; then
  print_message "cyan" "Mode: Add new data without recreating tables"
else
  print_message "cyan" "Mode: Recreate tables and insert data"
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  print_message "red" "Error: Docker is not running. Please start Docker and run this script again."
  exit 1
fi

# Stop and remove existing containers (if any)
print_message "yellow" "Stopping and removing existing containers..."
docker-compose down

# Start the SQL Server container
print_message "yellow" "Starting the SQL Server container..."
docker-compose up -d

# Wait for SQL Server to start completely
print_message "yellow" "Waiting for SQL Server to start (30 seconds)..."
sleep 30

# Run the configuration script
print_message "yellow" "Creating the database and tables..."

# Define the value of the drop_tables parameter based on the execution mode
if [ "$APPEND_ONLY" = true ]; then
  DROP_TABLES=0
else
  DROP_TABLES=1
fi

# Try to run the configuration script
if ! docker exec interview-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrongPassword123!" -C -i /scripts/sql_server_setup.sql -v drop_tables=$DROP_TABLES; then
  print_message "red" "Error configuring the database"
  print_message "yellow" "Trying alternative method..."
  
  # Alternative method using local sqlcmd
  if command -v sqlcmd > /dev/null; then
    if sqlcmd -S localhost,1433 -U sa -P "YourStrongPassword123!" -C -i scripts/sql_server_setup.sql -v drop_tables=$DROP_TABLES; then
      print_message "green" "Database and tables created successfully (alternative method)!"
    else
      print_message "red" "Error configuring the database with alternative method"
      print_message "yellow" "Please run the following commands manually:"
      print_message "cyan" "sqlcmd -S localhost,1433 -U sa -P 'YourStrongPassword123!' -C -i scripts/sql_server_setup.sql -v drop_tables=$DROP_TABLES"
      
      read -p "Press Enter to continue after running the commands manually, or type 'exit' to quit: " CONTINUAR
      if [ "$CONTINUAR" = "exit" ]; then
        exit 1
      fi
    fi
  else
    print_message "red" "sqlcmd is not installed. Please install sqlcmd or run the commands manually."
    print_message "cyan" "docker exec interview-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrongPassword123!' -C -i /scripts/sql_server_setup.sql -v drop_tables=$DROP_TABLES"
    
    read -p "Press Enter to continue after running the commands manually, or type 'exit' to quit: " CONTINUAR
    if [ "$CONTINUAR" = "exit" ]; then
      exit 1
    fi
  fi
else
  print_message "green" "Database and tables created successfully!"
fi

# Configure Python environment
print_message "yellow" "Configuring Python environment..."

# Check if the virtual environment exists
if [ -d ".venv" ]; then
  print_message "yellow" "Activating Python virtual environment..."
  
  # Determine the activation script based on the current shell
  if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
  else
    print_message "yellow" "Warning: Could not activate virtual environment. Continuing without activation..."
  fi
else
  print_message "yellow" "Creating Python virtual environment..."
  
  # Try to create the virtual environment
  if python3 -m venv .venv; then
    if [ -f ".venv/bin/activate" ]; then
      source .venv/bin/activate
      
      print_message "yellow" "Installing dependencies..."
      pip install -r requirements.txt
    fi
  else
    print_message "yellow" "Warning: Could not create virtual environment. Continuing without virtual environment..."
  fi
fi

# Run the data insertion script
print_message "yellow" "Running the data insertion script..."

# Build the arguments for the Python script
PYTHON_ARGS=""
if [ "$APPEND_ONLY" = true ]; then
  PYTHON_ARGS="$PYTHON_ARGS --append"
fi
if [ "$CLIENTS" -ne 50 ]; then
  PYTHON_ARGS="$PYTHON_ARGS --clients $CLIENTS"
fi
if [ "$CUSTOMERS" -ne 200 ]; then
  PYTHON_ARGS="$PYTHON_ARGS --customers $CUSTOMERS"
fi
if [ "$PRODUCTS" -ne 100 ]; then
  PYTHON_ARGS="$PYTHON_ARGS --products $PRODUCTS"
fi
if [ "$PURCHASES" -ne 500 ]; then
  PYTHON_ARGS="$PYTHON_ARGS --purchases $PURCHASES"
fi
if [ "$RETURNS" -ne 100 ]; then
  PYTHON_ARGS="$PYTHON_ARGS --returns $RETURNS"
fi

print_message "yellow"  $PYTHON_ARGS

# Run the Python script with the arguments
python3 scripts/Insert_data.py $PYTHON_ARGS

print_message "green" "Process completed successfully!"
print_message "green" "The database has been configured and data with intentional quality issues has been inserted."
print_message "yellow" "You can verify the inserted data by running SQL queries, for example:"
print_message "cyan" "docker exec interview-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrongPassword123!' -C -d interview_db -Q 'SELECT TOP 10 * FROM client'"

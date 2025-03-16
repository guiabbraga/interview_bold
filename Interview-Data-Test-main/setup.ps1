# Setup script for the Interview Data Engineering project
# This script configures the environment, starts containers and runs the data insertion script

param(
    [switch]$AppendOnly = $false,
    [int]$Clients = 50,
    [int]$Customers = 200,
    [int]$Products = 100,
    [int]$Purchases = 500,
    [int]$Returns = 100
)

Write-Host "Starting the Interview Data Engineering environment..." -ForegroundColor Green

if ($AppendOnly) {
    Write-Host "Mode: Add new data without recreating tables" -ForegroundColor Cyan
} else {
    Write-Host "Mode: Recreate tables and insert data" -ForegroundColor Cyan
}

# Check if Docker is running
try {
    docker info | Out-Null
}
catch {
    Write-Host "Error: Docker is not running. Please start Docker Desktop and run this script again." -ForegroundColor Red
    exit 1
}

# Stop and remove existing containers (if any)
Write-Host "Stopping and removing existing containers..." -ForegroundColor Yellow
docker-compose down

# Start the SQL Server container
Write-Host "Starting the SQL Server container..." -ForegroundColor Yellow
docker-compose up -d

# Wait for SQL Server to fully start
Write-Host "Waiting for SQL Server to start (30 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Run the setup script
Write-Host "Creating the database and tables..." -ForegroundColor Yellow
try {
    # Define the value of the drop_tables parameter based on the execution mode
    $dropTables = if ($AppendOnly) { 0 } else { 1 }
    
    # Run the setup script with the drop_tables parameter
    docker exec interview-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrongPassword123!" -C -i /scripts/sql_server_setup.sql -v drop_tables=$dropTables
    
    Write-Host "Database and tables created successfully!" -ForegroundColor Green
}
catch {
    Write-Host "Error configuring the database: $_" -ForegroundColor Red
    Write-Host "Trying alternative method..." -ForegroundColor Yellow
    
    # Alternative method using local sqlcmd
    try {
        sqlcmd -S localhost,1433 -U sa -P "YourStrongPassword123!" -C -i scripts\sql_server_setup.sql -v drop_tables=$dropTables
        
        Write-Host "Database and tables created successfully (alternative method)!" -ForegroundColor Green
    }
    catch {
        Write-Host "Error configuring the database with alternative method: $_" -ForegroundColor Red
        Write-Host "Please run the following commands manually:" -ForegroundColor Yellow
        Write-Host "sqlcmd -S localhost,1433 -U sa -P 'YourStrongPassword123!' -C -i scripts\sql_server_setup.sql -v drop_tables=$dropTables" -ForegroundColor Cyan
        
        $continuar = Read-Host "Press Enter to continue after running the commands manually, or type 'exit' to quit"
        if ($continuar -eq "exit") {
            exit 1
        }
    }
}

# Configure Python environment
Write-Host "Configuring Python environment..." -ForegroundColor Yellow

# Check if the virtual environment exists
if (Test-Path ".venv") {
    Write-Host "Activating Python virtual environment..." -ForegroundColor Yellow
    try {
        & .venv\Scripts\Activate.ps1
    }
    catch {
        Write-Host "Warning: Could not activate virtual environment. Continuing without activation..." -ForegroundColor Yellow
    }
}
else {
    Write-Host "Creating Python virtual environment..." -ForegroundColor Yellow
    try {
        python -m venv .venv
        & .venv\Scripts\Activate.ps1
        
        Write-Host "Installing dependencies..." -ForegroundColor Yellow
        pip install -r requirements.txt
    }
    catch {
        Write-Host "Warning: Could not create virtual environment. Continuing without virtual environment..." -ForegroundColor Yellow
    }
}

# Run the data insertion script
Write-Host "Running the data insertion script..." -ForegroundColor Yellow

# Build arguments for the Python script
$pythonArgs = @()
if ($AppendOnly) {
    $pythonArgs += "--append"
}
if ($Clients -ne 50) {
    $pythonArgs += "--clients", $Clients
}
if ($Customers -ne 200) {
    $pythonArgs += "--customers", $Customers
}
if ($Products -ne 100) {
    $pythonArgs += "--products", $Products
}
if ($Purchases -ne 500) {
    $pythonArgs += "--purchases", $Purchases
}
if ($Returns -ne 100) {
    $pythonArgs += "--returns", $Returns
}

# Run the Python script with arguments
python scripts/Insert_data.py $pythonArgs

Write-Host "Process completed successfully!" -ForegroundColor Green
Write-Host "The database has been configured and data with intentional quality issues has been inserted." -ForegroundColor Green
Write-Host "You can verify the inserted data by running SQL queries, for example:" -ForegroundColor Yellow
Write-Host "docker exec interview-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrongPassword123!' -C -d interview_db -Q 'SELECT TOP 10 * FROM client'" -ForegroundColor Cyan

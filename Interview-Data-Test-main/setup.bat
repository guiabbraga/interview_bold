@echo off
setlocal enabledelayedexpansion

REM Process parameters
set APPEND_ONLY=0
set CLIENTS=50
set CUSTOMERS=200
set PRODUCTS=100
set PURCHASES=500
set RETURNS=100

REM Process command line arguments
:parse_args
if "%~1"=="" goto end_parse_args
if /i "%~1"=="--append" (
    set APPEND_ONLY=1
    shift
    goto parse_args
)
if /i "%~1"=="--clients" (
    set CLIENTS=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--customers" (
    set CUSTOMERS=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--products" (
    set PRODUCTS=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--purchases" (
    set PURCHASES=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--returns" (
    set RETURNS=%~2
    shift
    shift
    goto parse_args
)
shift
goto parse_args
:end_parse_args

echo Starting the Interview Data Engineering environment...

if %APPEND_ONLY%==1 (
    echo Mode: Add new data without recreating tables
) else (
    echo Mode: Recreate tables and insert data
)

REM Check if Docker is running
docker info > nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Error: Docker is not running. Please start Docker Desktop and run this script again.
    exit /b 1
)

REM Stop and remove existing containers (if any)
echo Stopping and removing existing containers...
docker-compose down

REM Start the SQL Server container
echo Starting the SQL Server container...
docker-compose up -d

REM Wait for SQL Server to start completely
echo Waiting for SQL Server to start (30 seconds)...
timeout /t 30 > nul

REM Create the database and run the setup script
echo Creating the database and tables...

REM Define the value of the drop_tables parameter based on the execution mode
set DROP_TABLES=1
if %APPEND_ONLY%==1 set DROP_TABLES=0

REM Try the main method using docker exec
docker exec interview-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrongPassword123!" -C -i /scripts/sql_server_setup.sql -v drop_tables=%DROP_TABLES% > nul 2>&1
if %ERRORLEVEL% neq 0 goto ALTERNATIVE_METHOD

echo Database and tables created successfully!
goto SETUP_PYTHON

:ALTERNATIVE_METHOD
echo Trying alternative method...

REM Alternative method using local sqlcmd
sqlcmd -S localhost,1433 -U sa -P "YourStrongPassword123!" -C -i scripts\sql_server_setup.sql -v drop_tables=%DROP_TABLES% > nul 2>&1
if %ERRORLEVEL% neq 0 goto MANUAL_METHOD

echo Database and tables created successfully (alternative method)!
goto SETUP_PYTHON

:MANUAL_METHOD
echo Error configuring the database automatically.
echo Please run the following commands manually:
echo sqlcmd -S localhost,1433 -U sa -P "YourStrongPassword123!" -C -i scripts\sql_server_setup.sql -v drop_tables=%DROP_TABLES%
set /p continuar=Press Enter to continue after running the commands manually, or type 'exit' to quit: 
if "%continuar%"=="exit" exit /b 1

:SETUP_PYTHON
REM Check if the Python virtual environment exists and activate it
if exist .venv (
    echo Activating Python virtual environment...
    call .venv\Scripts\activate.bat 2> nul
    if %ERRORLEVEL% neq 0 (
        echo Warning: Could not activate virtual environment. Continuing without activation...
    )
) else (
    echo Creating Python virtual environment...
    python -m venv .venv 2> nul
    if %ERRORLEVEL% neq 0 (
        echo Warning: Could not create virtual environment. Continuing without virtual environment...
    ) else (
        call .venv\Scripts\activate.bat 2> nul
        if %ERRORLEVEL% neq 0 (
            echo Warning: Could not activate virtual environment. Continuing without activation...
        )
    )
    
    echo Installing dependencies...
    pip install -r requirements.txt
)

REM Run the data insertion script
echo Running the data insertion script...

REM Build the arguments for the Python script
set PYTHON_ARGS=
if %APPEND_ONLY%==1 set PYTHON_ARGS=!PYTHON_ARGS! --append
if %CLIENTS% NEQ 50 set PYTHON_ARGS=!PYTHON_ARGS! --clients %CLIENTS%
if %CUSTOMERS% NEQ 200 set PYTHON_ARGS=!PYTHON_ARGS! --customers %CUSTOMERS%
if %PRODUCTS% NEQ 100 set PYTHON_ARGS=!PYTHON_ARGS! --products %PRODUCTS%
if %PURCHASES% NEQ 500 set PYTHON_ARGS=!PYTHON_ARGS! --purchases %PURCHASES%
if %RETURNS% NEQ 100 set PYTHON_ARGS=!PYTHON_ARGS! --returns %RETURNS%

REM Run the Python script with the arguments
python scripts/Insert_data.py %PYTHON_ARGS%

echo Process completed successfully!
echo The database has been configured and data with intentional quality issues has been inserted.
echo You can verify the inserted data by running SQL queries, for example:
echo docker exec interview-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YourStrongPassword123!" -C -d interview_db -Q "SELECT TOP 10 * FROM client"

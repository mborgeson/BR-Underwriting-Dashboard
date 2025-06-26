@echo off
REM Batch file to run tests with correct Python path
REM Place this in your project root: /home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard/

echo ========================================
echo B&R Capital Dashboard - Test Runner
echo ========================================
echo.

REM Activate conda environment
echo Activating conda environment...
call conda activate underwriting_dashboard

REM Change to project directory
cd "/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard"

echo.
echo Current directory: %CD%
echo.

REM Menu
echo Select a test to run:
echo 1. Check Python paths (diagnostic)
echo 2. Test SharePoint connection
echo 3. Test Excel extraction
echo 4. Run complete workflow
echo 5. Exit
echo.

set /p choice="Enter your choice (1-5): "

if "%choice%"=="1" (
    echo.
    echo Running path diagnostic...
    python tests\check_paths.py
) else if "%choice%"=="2" (
    echo.
    echo Testing SharePoint connection...
    python tests\test_sharepoint_connection.py
) else if "%choice%"=="3" (
    echo.
    echo Testing Excel extraction...
    python tests\excel_extraction_test.py
) else if "%choice%"=="4" (
    echo.
    echo Running complete workflow...
    python complete_extraction_workflow.py
) else if "%choice%"=="5" (
    echo.
    echo Exiting...
    exit /b 0
) else (
    echo.
    echo Invalid choice!
)

echo.
pause
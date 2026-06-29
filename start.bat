@echo off
REM ============================================================
REM  City Pulse — Auto Setup & Run Script (Windows)
REM  Usage: Double-click start.bat  OR  run in Command Prompt
REM ============================================================

title City Pulse — NYC ETL Pipeline

echo.
echo ============================================================
echo    City Pulse -- NYC ETL Pipeline Setup
echo ============================================================
echo.

REM ── Step 1: Check Python ─────────────────────────────────
echo [1/5] Checking Python...
python --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo [ERROR] Python not found.
    echo         Install it from https://python.org
    echo         Make sure to check "Add Python to PATH" during install.
    pause
    exit /b 1
)
echo [OK] Python found:
python --version
echo.

REM ── Step 2: Create dags/__init__.py if missing ───────────
echo [2/5] Checking dags\ folder...
IF NOT EXIST "dags" (
    echo [!] dags\ folder not found -- creating it...
    mkdir dags
)
IF NOT EXIST "dags\__init__.py" (
    echo [!] dags\__init__.py missing -- creating it...
    echo. > dags\__init__.py
)
echo [OK] dags\ folder ready
echo.

REM ── Step 3: Check required project files ─────────────────
echo [3/5] Checking required files...
SET MISSING=0
FOR %%f IN (run.py config.py database.py transforms.py data_generator.py dashboard.py scheduler.py) DO (
    IF NOT EXIST "%%f" (
        echo [ERROR] Missing file: %%f
        SET MISSING=1
    )
)
IF NOT EXIST "dags\city_pulse_dag.py" (
    echo [ERROR] Missing file: dags\city_pulse_dag.py
    SET MISSING=1
)
IF "%MISSING%"=="1" (
    echo.
    echo [ERROR] Some files are missing. Make sure all project files
    echo         are in the same folder as this start.bat file.
    pause
    exit /b 1
)
echo [OK] All required files found
echo.

REM ── Step 4: Virtual environment ──────────────────────────
echo [4/5] Setting up virtual environment...
IF NOT EXIST "venv" (
    echo Creating virtual environment...
    python -m venv venv
) ELSE (
    echo Virtual environment already exists -- skipping
)
echo Activating virtual environment...
call venv\Scripts\activate.bat
echo [OK] Virtual environment ready
echo.

REM ── Step 5: Install dependencies ─────────────────────────
echo [5/5] Installing dependencies...
python -m pip install --quiet --upgrade pip
python -m pip install --quiet flask pandas requests numpy
echo [OK] Dependencies installed
echo.

REM ── Launch ───────────────────────────────────────────────
echo ============================================================
echo   Setup complete! Launching City Pulse...
echo.
echo   Dashboard will be at: http://localhost:5050
echo   Running backfill (90 days of data) -- takes ~1-2 minutes
echo   Press Ctrl+C to stop
echo ============================================================
echo.

python run.py --backfill --rows 100000

pause

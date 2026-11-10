@echo off
REM ====================================================================
REM Quick Task Runner for Tiki Real-time Market Analytics Platform
REM Author: TrungPham233
REM ====================================================================

IF "%1"=="" GOTO help
IF "%1"=="test" GOTO test
IF "%1"=="crawl" GOTO crawl
IF "%1"=="medallion" GOTO medallion
IF "%1"=="dwh" GOTO dwh
IF "%1"=="all" GOTO all
GOTO help

:test
echo Running unit tests...
python -m pytest tests/ -v
GOTO end

:crawl
echo Running data collection...
python data_collection/crawl_product_ids.py --max-products 50
python data_collection/crawl_product_detail.py
python data_collection/crawl_reviews.py --max-reviews-per-product 20
python data_collection/simulate_price_events.py --count 100
GOTO end

:medallion
echo Running Medallion Pipeline (Bronze - Silver - Gold)...
python spark/run_medallion_pipeline.py
GOTO end

:dwh
echo Syncing Gold layer to Data Warehouse...
python data_warehouse/load_gold_to_postgres.py
GOTO end

:all
echo Executing End-to-End Pipeline...
call %0 medallion
call %0 dwh
call %0 test
echo All tasks executed successfully!
GOTO end

:help
echo Usage: run.bat [test ^| crawl ^| medallion ^| dwh ^| all]
echo.
echo Commands:
echo   test       Run pytest suite across all modules (25 unit tests)
echo   crawl      Trigger Tiki crawlers and price simulator
echo   medallion  Execute Bronze - Silver - Gold Lakehouse ETL
echo   dwh        Sync Gold Parquet tables into PostgreSQL/SQLite Data Warehouse
echo   all        Run entire end-to-end pipeline and test suite
GOTO end

:end


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
IF "%1"=="dashboard" GOTO dashboard
IF "%1"=="all" GOTO all
GOTO help

:test
echo Running unit tests...
python -m pytest tests/ -v
GOTO end

:crawl
echo Running data collection...
python data_collection/crawl_product_ids.py --max-products 2000
python data_collection/crawl_product_detail.py
python data_collection/crawl_reviews.py --max-reviews-per-product 20
python data_collection/simulate_price_events.py --count 500 --rate 0
GOTO end

:medallion
echo Running Medallion Pipeline (PySpark + Delta: Bronze - Silver - Quality Gate - Gold - Stream - Serving)...
python spark/run_medallion_pipeline.py
GOTO end

:dwh
echo Syncing Gold layer to Data Warehouse...
python data_warehouse/load_gold_to_postgres.py
GOTO end

:dashboard
echo Refreshing Power BI source data and regenerating PBIP visuals...
python dashboard/export_dashboard_data.py
python dashboard/generate_pbip_dashboards.py
GOTO end

:all
echo Executing End-to-End Pipeline...
call %0 medallion
call %0 dwh
call %0 dashboard
call %0 test
echo All tasks executed successfully!
GOTO end

:help
echo Usage: run.bat [test ^| crawl ^| medallion ^| dwh ^| dashboard ^| all]
echo.
echo Commands:
echo   test       Run pytest suite (unit + PySpark/Delta integration tests)
echo   crawl      Crawl up to 2000 Tiki products + reviews, simulate price events
echo   medallion  Execute the PySpark + Delta Lake Medallion pipeline
echo   dwh        Sync serving-layer Parquet into PostgreSQL/SQLite Data Warehouse
echo   dashboard  Refresh Power BI source data and regenerate the PBIP report
echo   all        Run entire end-to-end pipeline and test suite
GOTO end

:end


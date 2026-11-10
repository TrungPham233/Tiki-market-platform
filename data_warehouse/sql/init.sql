-- Initialization script for PostgreSQL Data Warehouse
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Comment on schemas
COMMENT ON SCHEMA staging IS 'Temporary staging tables loaded from Data Lake';
COMMENT ON SCHEMA dwh IS 'Production Star Schema tables (Fact & Dimension)';
COMMENT ON SCHEMA analytics IS 'Serving views and aggregate tables for Power BI';

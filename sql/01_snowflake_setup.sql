-- =====================================================================
-- STEP 1: CREATE WAREHOUSE & DATABASE INFRASTRUCTURE
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- Create lightweight virtual warehouse for dbt transformations
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE 
    INITIALLY_SUSPENDED = TRUE;

-- Create primary project database
CREATE DATABASE IF NOT EXISTS HEALTHCARE_ANALYTICS;

-- Create Schemas for Medallion Architecture (Bronze -> Silver -> Gold)
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_ANALYTICS.RAW;       -- Ingestion layer
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_ANALYTICS.STG;       -- Cleaned staging layer (dbt)
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_ANALYTICS.ANALYTICS; -- Dimensional models (dbt)

-- =====================================================================
-- STEP 2: RBAC SETUP - DEDICATED TRANSFORM ROLE & HIERARCHY
-- =====================================================================
-- Create custom functional role for dbt & Airflow service execution
CREATE ROLE IF NOT EXISTS TRANSFORM_ROLE;

-- Attach custom role to SYSADMIN to maintain proper hierarchy
GRANT ROLE TRANSFORM_ROLE TO ROLE SYSADMIN;

-- Grant warehouse execution privileges
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORM_ROLE;

-- Grant database & schema privileges
GRANT USAGE ON DATABASE HEALTHCARE_ANALYTICS TO ROLE TRANSFORM_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA HEALTHCARE_ANALYTICS.RAW TO ROLE TRANSFORM_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA HEALTHCARE_ANALYTICS.STG TO ROLE TRANSFORM_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA HEALTHCARE_ANALYTICS.ANALYTICS TO ROLE TRANSFORM_ROLE;

-- Grant privileges to create future tables and views in target schemas
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HEALTHCARE_ANALYTICS.STG TO ROLE TRANSFORM_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HEALTHCARE_ANALYTICS.ANALYTICS TO ROLE TRANSFORM_ROLE;

-- =====================================================================
-- STEP 3: MOCK RAW DATA POPULATION
-- =====================================================================
USE ROLE TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE HEALTHCARE_ANALYTICS;
USE SCHEMA RAW;

-- =====================================================================
-- STEP 4:STAGE Creation in RAW Schema
-- =====================================================================
USE DATABASE HEALTHCARE_ANALYTICS;
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS MY_RAW_STAGE;

SHOW STAGES IN SCHEMA HEALTHCARE_ANALYTICS.RAW;

--Enable Directory for this stage and refresh
ALTER STAGE MY_RAW_STAGE SET DIRECTORY = (enable = TRUE);
ALTER STAGE MY_RAW_STAGE REFRESH;

--Create STREAM on the stage directory
CREATE OR REPLACE STREAM RAW_STAGE_FILE_STREAM ON STAGE MY_RAW_STAGE;
SHOW STREAMS IN SCHEMA HEALTHCARE_ANALYTICS.RAW;
SELECT * FROM RAW_STAGE_FILE_STREAM;
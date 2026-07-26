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

-- Create raw patients source table
CREATE OR REPLACE TABLE RAW_PATIENTS (
    patient_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    dob STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create raw visits source table
CREATE OR REPLACE TABLE RAW_VISITS (
    visit_id STRING,
    patient_id STRING,
    visit_date STRING,
    diagnosis_code STRING,
    total_amount NUMBER(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample raw records
INSERT INTO RAW_PATIENTS (patient_id, first_name, last_name, gender, dob) VALUES
('P101', 'john', 'doe', 'M', '1985-06-12'),
('P102', 'jane', 'smith', 'F', '1992-01-25'),
('P103', 'robert', 'johnson', 'M', '1978-11-03');

INSERT INTO RAW_VISITS (visit_id, patient_id, visit_date, diagnosis_code, total_amount) VALUES
('V1001', 'P101', '2026-07-01', 'A01.0', 250.00),
('V1002', 'P102', '2026-07-02', 'B20.0', 1100.50),
('V1003', 'P101', '2026-07-05', 'C34.9', 450.75),
('V1004', 'P103', '2026-07-10', 'E11.9', 300.00);

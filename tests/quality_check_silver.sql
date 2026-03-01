/* ====================================================================================
   Script Name:      Data_Quality_Validation_Checks.sql
   Author:           [Enes]
   Create Date:      [1-3-2025]
   Description:      This script contains a suite of Data Quality (DQ) and Exploratory 
                     Data Analysis (EDA) queries for the Bronze and Silver layers.
                     
   Purpose:          - Identify nulls, duplicates, and invalid primary keys.
                     - Detect formatting issues (e.g., unwanted spaces, case inconsistencies).
                     - Validate business logic (e.g., negative costs, invalid dates).
                     - Check data consistency (e.g., Sales = Quantity * Price).
                     - Verify cross-table referential integrity.
                     
   Modification Log:
   Date            Author            Description
   ----------  ---------------   -------------------------------------------------------
   [1-3-2025]      [Enes]           Initial Creation
==================================================================================== */

-- ====================================================================================
-- 1. TABLE: crm_cust_info (Bronze & Silver)
-- ====================================================================================

-- Checking for Nulls or duplicates in Primary key (Bronze)
-- Expectation: No Result
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Checking unwanted Spaces (Bronze)
-- Expectation: No result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Checking for Nulls or duplicates in Primary key (Silver)
-- Expectation: No Result
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Checking unwanted Spaces (Silver)
-- Expectation: No result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency (Silver)
SELECT DISTINCT cst_gender
FROM silver.crm_cust_info;

SELECT *
FROM silver.crm_cust_info;


-- ====================================================================================
-- 2. TABLE: crm_prd_info (Bronze & Silver)
-- ====================================================================================

-- Checking for Nulls or duplicates in Primary key (Bronze)
-- Expectation: No Result
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Checking unwanted Spaces (Bronze)
-- Expectation: No result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Checking negative number or nulls (Bronze)
-- Expectation: No result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Review Distinct Product Lines
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Test LEAD function for End Date derivation
SELECT *,
       LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) as new_prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');

-- Checking for invalid date logic (Silver)
-- Expectation: No Result
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


-- ====================================================================================
-- 3. TABLE: crm_sales_details (Bronze & Silver)
-- ====================================================================================

-- Referential Integrity Check: Sales to Customers
SELECT sls_ord_name, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price    
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check invalid Order Dates
SELECT NULLIF(sls_order_dt,0) as sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=

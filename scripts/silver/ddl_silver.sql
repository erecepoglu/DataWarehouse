

/* ====================================================================================
   Script Name:      Create_Silver_Layer_Tables.sql
   Author:           [Enes]
   Create Date:      [1-3-2025]
   Description:      DDL script to initialize the Silver layer for the Data Warehouse.
                     This script ensures the 'silver' schema exists, drops existing 
                     tables to prevent duplication errors, and creates standardized 
                     tables for CRM and ERP data.
                     
                     All tables include a standard 'dwh_create_date' audit column 
                     to track when the records were loaded into the Data Warehouse.
   
   Modification Log:
   Date              Author            Description
   ----------  ---------------   -------------------------------------------------------
   [1-3-2025]      [Enes]            Initial Creation
==================================================================================== */

-- -----------------------------------------------------------------------------
-- Create Schema: silver
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- -----------------------------------------------------------------------------
-- Table: silver.crm_cust_info
-- -----------------------------------------------------------------------------
IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gender          VARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- -----------------------------------------------------------------------------
-- Table: silver.crm_prd_info
-- -----------------------------------------------------------------------------
IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        INT,
    prd_line        VARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- -----------------------------------------------------------------------------
-- Table: silver.crm_sales_details
-- -----------------------------------------------------------------------------
IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- -----------------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-- -----------------------------------------------------------------------------
IF OBJECT_ID ('silver.erp_cust_az12','U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- -----------------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-- -----------------------------------------------------------------------------
IF OBJECT_ID ('silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- -----------------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-- -----------------------------------------------------------------------------
IF OBJECT_ID ('silver.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

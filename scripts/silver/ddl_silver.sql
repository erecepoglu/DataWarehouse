/* ====================================================================================
   Stored Procedure: silver.load_silver
   Author:           [Enes]
   Create Date:      [1-3-2025]
   Description:      This procedure loads data from the Bronze layer into the Silver layer.
                     It performs data cleansing, deduplication, and standardizes formats 
                     for customer, product, sales, and ERP data.
                     
   Execution:        EXEC silver.load_silver;
   
   Modification Log:
   Date              Author            Description
   ----------  ---------------   -------------------------------------------------------
   [1-3-2025]        [Enes]           Initial Creation
==================================================================================== */
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME, @start_time DATETIME, @end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==================================';
        PRINT 'Loading Silver Layer';
        PRINT '==================================';

        -- -----------------------------------------------------
        -- 1. Load: silver.crm_cust_info
        -- -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        
        PRINT '>> Inserting data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gender,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                 ELSE 'n/a'
            END AS cst_material_status,
            CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
                 ELSE 'n/a'
            END AS cst_gender,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t 
        WHERE flag_last = 1;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------';

        -- -----------------------------------------------------
        -- 2. Load: silver.crm_prd_info
        -- -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        
        PRINT '>> Inserting data into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        ) 
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'R' THEN 'Road'
                 WHEN 'S' THEN 'Other Sales'
                 WHEN 'T' THEN 'Touring'
                 ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            -- Using DATEADD for date math and applying SUBSTRING directly inside partition 
            -- so it groups by transformed key, not raw table key.
            CAST(
                DATEADD(day, -1, LEAD(prd_start_dt) OVER(
                    PARTITION BY SUBSTRING(prd_key, 7, LEN(prd_key)) 
                    ORDER BY prd_start_dt
                )) 
            AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------';

        -- -----------------------------------------------------
        -- 3. Load: silver.crm_sales_details
        -- -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        
        PRINT '>> Inserting data into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price 
        )
        SELECT
            sls_ord_num, -- Fixed alias (was sls_ord_name)
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
            END AS sls_order_dt, -- Fixed alias (was order_dt)
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales != (sls_quantity * ABS(sls_price))
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price
            END AS sls_price    
        FROM bronze.crm_sales_details;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------';

        -- -----------------------------------------------------
        -- 4. Load: silver.erp_cust_az12
        -- -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        
        PRINT '>> Inserting data into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
                 ELSE cid
            END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL
                 ELSE bdate
            END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------';

        -- -----------------------------------------------------
        -- 5. Load: silver.erp_loc_a101
        -- -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        
        PRINT '>> Inserting data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                 WHEN TRIM(cntry) = 'UK' THEN 'United Kingdom'
                 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'n/a'
                 ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------';

        -- -----------------------------------------------------
        -- 6. Load: silver.erp_px_cat_g1v2
        -- -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        -- Fixed copy-paste bug in the PRINT statement here
        PRINT '>> Inserting data into: silver.erp_px_cat_g1v2'; 
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance 
        )
        SELECT
            *
        FROM bronze.erp_px_cat_g1v2;
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '---------------------------';
        
        -- -----------------------------------------------------
        -- Execution Summary
        -- -----------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '===========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '     - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '===========================================';

    END TRY
    BEGIN CATCH 
        PRINT '===========================================';
        -- Fixed typo (ERRO -> ERROR) and wrong layer referenced (BRONZE -> SILVER)
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'; 
        PRINT 'Error Message: ' + ERROR_MESSAGE(); 
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===========================================';
    END CATCH
END

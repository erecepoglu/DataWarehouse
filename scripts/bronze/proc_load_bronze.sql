/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source to Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'Bronze' schema using UNC paths.
    It performs a 'Truncate and Load' strategy for CRM and ERP source systems.

Usage:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    -- Change this variable to your Shared Network Path (UNC)
    DECLARE @base_path NVARCHAR(500) = '\\YourComputerName\DataWarehouse\dataset\'; 

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        --------------------------------------------------------
        -- CRM Tables
        --------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- 1. crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Bulk Inserting: bronze.crm_cust_info';
        EXEC('BULK INSERT bronze.crm_cust_info FROM ''' + @base_path + 'source_crm\cust_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'', TABLOCK);');
        
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 2. crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Bulk Inserting: bronze.crm_prd_info';
        EXEC('BULK INSERT bronze.crm_prd_info FROM ''' + @base_path + 'source_crm\prd_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'', TABLOCK);');

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 3. crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Bulk Inserting: bronze.crm_sales_details';
        EXEC('BULK INSERT bronze.crm_sales_details FROM ''' + @base_path + 'source_crm\sales_details.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'', TABLOCK);');

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        --------------------------------------------------------
        -- ERP Tables
        --------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- 4. erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Bulk Inserting: bronze.erp_cust_az12';
        EXEC('BULK INSERT bronze.erp_cust_az12 FROM ''' + @base_path + 'source_erp\cust_az12.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'', TABLOCK);');

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 5. erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Bulk Inserting: bronze.erp_loc_a101';
        EXEC('BULK INSERT bronze.erp_loc_a101 FROM ''' + @base_path + 'source_erp\loc_a101.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'', TABLOCK);');

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- 6. erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Bulk Inserting: bronze.erp_px_cat_g1v2';
        EXEC('BULK INSERT bronze.erp_px_cat_g1v2 FROM ''' + @base_path + 'source_erp\px_cat_g1v2.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'', TABLOCK);');

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer Completed';
        PRINT ' - Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH 
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE(); 
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
GO

-- Execute the procedure
EXEC bronze.load_bronze;

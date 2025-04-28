USE DataWarehouse
GO 
CREATE OR ALTER PROCEDURE silver.sp_load_crm_prd_info_to_silver AS
BEGIN

     DECLARE @start_time DATETIME
     DECLARE @end_time DATETIME
     DECLARE @batch_start_time DATETIME
     DECLARE @batch_end_time DATETIME
     DECLARE @rows_moved INT
     DECLARE @ErrorMessage NVARCHAR(4000);
     DECLARE @ErrorLine INT
     DECLARE @exec_time INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Prd_info Table';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		
        
        SET @rows_moved = @@ROWCOUNT;
		
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        
        SET @exec_time = DATEDIFF(second, @start_time, @end_time);
       
        INSERT INTO ADMIN.ADMIN_LOGGING_DATA (source, table_name, stage, status, error_message, rows_moved, start_time, end_time, execution_time, insert_ts)
        VALUES ('Bronze', 'cust_info', 'Bronze to Silver', 'SUCCEDEED', NULL, @rows_moved, @start_time, @end_time, @exec_time, GETDATE());

	END TRY
        BEGIN CATCH
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorLine = ERROR_LINE();

        PRINT '>> ERROR: Failed to insert data into silver.crm_cust_info';
        PRINT '>> Error Message: ' + @ErrorMessage;
        
        -- Log error to the logging table with detailed information
        INSERT INTO ADMIN.ADMIN_LOGGING_DATA 
            (source, table_name, stage, status, error_message, rows_moved, start_time, end_time, execution_time, insert_ts)
        VALUES ('Bronze', 'cust_info', 'Bronze to Silver', 'FAILED', 'Error at line ' + CAST(@ErrorLine AS NVARCHAR) + ': ' + @ErrorMessage, 
            0, @start_time, GETDATE(), DATEDIFF(second, @start_time, GETDATE()), GETDATE());
        END CATCH
END

GO
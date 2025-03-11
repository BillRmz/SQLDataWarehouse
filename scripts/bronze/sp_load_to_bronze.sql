/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the BULK INSERT command to load data from CSV files to bronze tables.
    - Inserts metadata related to bulk operation in logging table.

Parameters:
    @source_name - A string parameter to filter which source system's data to load.
    Currently hardcoded to 'source_crm' but should be parameterized.

Usage Example:
    EXEC bronze.load_bronze @source_name = 'source_crm';
===============================================================================
*/

CREATE OR ALTER PROCEDURE ADMIN.SP_LOAD_SOURCE_TO_BRONZE
    @source_name VARCHAR(250)
AS
BEGIN
DECLARE @i INT = 1;
DECLARE @max INT;
DECLARE @rows_moved INT;
DECLARE @count_statement VARCHAR(MAX)
DECLARE @table_name VARCHAR(250);
DECLARE @truncate_statement VARCHAR(250);
DECLARE @start_time DATETIME;
DECLARE @end_time DATETIME;
DECLARE @bulk_insert_statement VARCHAR(MAX);
DECLARE @parsed_source_name VARCHAR(250);
DECLARE @exec_time INT;
DECLARE @ErrorMessage NVARCHAR(4000);
DECLARE @ErrorLine INT;

    	PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading ' + UPPER(@source_name) +' Tables';
		PRINT '------------------------------------------------';

-- Create a temporary table to store tables that need processing
-- The CTE numbers rows to allow sequential processing
WITH NumberedResults AS (
    SELECT table_name, source,
        ROW_NUMBER() OVER (ORDER BY source) AS RowNum
    FROM ADMIN.ADMIN_CONTROL_LOAD
    WHERE source = @source_name
)
SELECT * 
INTO #Temp_NumberedResults
FROM NumberedResults;

SELECT @max = COUNT(*) FROM #Temp_NumberedResults;

BEGIN TRY
WHILE @i <= @max
BEGIN
    -- Get current row values

    SELECT 
    	@table_name  = REPLACE(table_name, '.csv', ''),
      @parsed_source_name = SUBSTRING(source, 
              CHARINDEX('_', source) + 1, 
              LEN(source))
    FROM 
		#Temp_NumberedResults
    WHERE 
		RowNum = @i;

    SET @truncate_statement = 'TRUNCATE TABLE bronze.' +@parsed_source_name + '_'+@table_name;
    EXEC (@truncate_statement);
    SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.'+ @table_name;
		PRINT '>> Inserting Data Into: bronze.' +@parsed_source_name + '_' + @table_name 
 	
	-- Build and execute BULK INSERT statement
    -- Note: File path is hardcoded to a specific user directory
	SET @bulk_insert_statement = '
    BULK INSERT bronze.'+@parsed_source_name + '_' + @table_name + '
    FROM ''C:\Users\bilya\OneDrive\Documents\Repos\SQLDataWarehouse\dataset\'+@source_name+'\' + @table_name + '.csv''
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = '','',
        TABLOCK
    );';
    
    EXEC (@bulk_insert_statement);
		
		SET @rows_moved = @@ROWCOUNT;
		PRINT 'Rows Moved: ' + CAST(@rows_moved AS NVARCHAR);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		SET @exec_time = CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);
		PRINT '>> -------------';
		
	
       -- Debug output
    PRINT 'Processing: ' + @table_name + ', Value: ';
    
      -- Log operation details to audit table
   INSERT INTO ADMIN.ADMIN_LOGGING_DATA (source, table_name, stage, status, error_message, rows_moved, start_time, end_time, execution_time, insert_ts)
    values (@source_name, @table_name, 'Source to Bronze', 'SUCCEDEED', NULL, @rows_moved, @start_time, @end_time, @exec_time, GETDATE())
    -- Increment counter
    SET @i = @i + 1;
 TRUNCATE TABLE #Temp_NumberedResults;
END;
   END TRY
    BEGIN CATCH
        SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorLine = ERROR_LINE();

	PRINT '>> ERROR: Failed to insert data into ' + @table_name;
    PRINT '>> Error Message: ' + @ErrorMessage;
    
	-- Log error to the logging table with detailed information
    INSERT INTO ADMIN.ADMIN_LOGGING_DATA 
        (source, table_name, stage, status, error_message, rows_moved, start_time, end_time, execution_time, insert_ts)
    VALUES (@source_name, @table_name, 'Source to Bronze', 'FAILED', 'Error at line ' + CAST(@ErrorLine AS NVARCHAR) + ': ' + @ErrorMessage, 
         0, @start_time, GETDATE(), DATEDIFF(second, @start_time, GETDATE()), GETDATE());
	 END CATCH;

	PRINT '================================================';
    PRINT 'Bronze Layer Loading Complete';
    PRINT '================================================';
END;
go 



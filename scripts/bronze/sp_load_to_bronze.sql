/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    Source Name. 
	  This stored procedure use a string parameter to filter by source.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


--CREATE OR ALTER PROCEDURE ADMIN.SP_LOAD_SOURCE_TO_BRONZE AS

use DataWarehouse;
GO

DECLARE @i INT = 1;
DECLARE @max INT;
DECLARE @source_name VARCHAR(250); -- I WANT TO HAVE THIS AS SP PARAMETER
DECLARE @table_name VARCHAR(250);
DECLARE @truncate_statement VARCHAR(250);
DECLARE @start_time DATETIME;
DECLARE @end_time DATETIME;
DECLARE @bulk_insert_statement VARCHAR(MAX);
DECLARE @parsed_source_name VARCHAR(250);

DROP TABLE #Temp_NumberedResults;
SET @source_name = 'source_crm'; --THIS SHOULD BE A SP PARAMETER
    PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading ' + UPPER(@source_name) +' Tables';
		PRINT '------------------------------------------------';
-- Get the count using the CTE in a single statement
WITH NumberedResults AS (
    SELECT table_name, source,
        ROW_NUMBER() OVER (ORDER BY source) AS RowNum
    FROM ADMIN.ADMIN_CONTROL_LOAD
    WHERE source = @source_name
)
SELECT * 
INTO #Temp_NumberedResults
FROM NumberedResults;

SELECT @max = COUNT(*) FROM #Temp_NumberedResults

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
	 SET @bulk_insert_statement = '
    BULK INSERT bronze.'+@parsed_source_name + '_' + @table_name + '
    FROM ''C:\Users\bilya\OneDrive\Documents\Repos\SQLDataWarehouse\dataset\'+@source_name+'\' + @table_name + '.csv''
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = '','',
        TABLOCK
    );';
    
    EXEC (@bulk_insert_statement);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
    
    -- Do something with the values
    PRINT 'Processing: ' + @table_name + ', Value: ';
    
    -- Your processing logic here
   
    -- Increment counter
    SET @i = @i + 1;
    
END;
go 

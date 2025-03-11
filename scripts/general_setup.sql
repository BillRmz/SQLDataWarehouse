CREATE DATABASE DataWarehouse;

--give credits to this channel 
--https://github.com/DataWithBaraa/sql-data-warehouse-project/blob/main/scripts/gold/ddl_gold.sql
USE DataWarehouse;

Go CREATE SCHEMA ADMIN;

GO
CREATE TABLE
    ADMIN.ADMIN_LOGGING_DATA (
        insert_id INT PRIMARY KEY IDENTITY (1, 1) NOT NULL,
        source NVARCHAR (50),
        table_name NVARCHAR (50),
        stage NVARCHAR (50),
        status NVARCHAR (50),
        error_message NVARCHAR (50),
        rows_moved NVARCHAR (50),
        start_time DATETIME,
        end_time DATETIME,
        execution_time DATETIME,
        insert_ts DATETIME
    );

SELECT
    *
FROM
    ADMIN.ADMIN_LOGGING_DATA GO
CREATE TABLE
    ADMIN.ADMIN_CONTROL_LOAD (
        row_id INT PRIMARY KEY IDENTITY (1, 1) NOT NULL,
        table_name VARCHAR(50),
        source VARCHAR(50),
        insert_ts DATETIME
    );

INSERT INTO
    ADMIN.ADMIN_CONTROL_LOAD (table_name, source, insert_ts)
VALUES
    ('cust_info.csv', 'source_crm', GETDATE ())
INSERT INTO
    ADMIN.ADMIN_CONTROL_LOAD (table_name, source, insert_ts)
VALUES
    ('prd_info.csv', 'source_crm', GETDATE ()),
    ('sales_details.csv', 'source_crm', GETDATE ())
INSERT INTO
    ADMIN.ADMIN_CONTROL_LOAD (table_name, source, insert_ts)
VALUES
    ('CUST_AZ12.csv', 'source_erp', GETDATE ()),
    ('LOC_A101.csv', 'source_erp', GETDATE ()),
    ('PX_CAT_G1V2.csv', 'source_erp', GETDATE ())
SELECT
    *
FROM
    ADMIN.ADMIN_CONTROL_LOAD;
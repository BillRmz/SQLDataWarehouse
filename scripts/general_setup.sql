CREATE DATABASE DataWarehouse;

USE DataWarehouse;

Go CREATE SCHEMA ADMIN;

GO
CREATE TABLE
    ADMIN.ADMIN_LOGGING_DATA (
        insert_id INT PRIMARY KEY IDENTITY (1, 1) NOT NULL,
        table_name NVARCHAR (50),
        from_stage NVARCHAR (50),
        to_stage NVARCHAR (50),
        rows_moved NVARCHAR (50),
        source NVARCHAR (50),
        insert_ts DATETIME
    );

SELECT
    *
FROM
    ADMIN.ADMIN_LOGGING_DATA
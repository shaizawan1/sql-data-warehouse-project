Go
use master;

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Datawarehouse;
END;
GO

--CREATING DATABASE
create database Datawarehouse;

--USING DATABASE
Go
use Datawarehouse;


--CREATING SCHEMAS
Go
create schema bronze;

Go
create schema silver;

Go
create schema gold;


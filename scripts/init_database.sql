/* 
CREATING DATABASE AND SCHEMA
 
Purpose: 
	This is to create a new database with the name "DataWarehouse" (deleting if already exists and creating new one"
	Post this there are three schema's created.


Warning:
	Running this will permanently delete the existing data so proceed with caution. 
*/ 
USE master;
GO

--Checking if database exists and dropping if exists
if exists(select 1 from sys.databases where name = 'DataWarehouse')
Begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
GO

--Creating database
CREATE DATABASE DataWarehouse;

--Creating schema
use DataWarehouse;
GO
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO

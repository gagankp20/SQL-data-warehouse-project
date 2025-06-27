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
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
BEGIN
	Declare @start_time datetime , @end_time datetime, @layer_begin datetime , @layer_end datetime;
	
	BEGIN TRY
		set @layer_begin = GETDATE();
		Print'========================================';
		Print 'Loading bronze layer';
		Print'========================================';
	
		Print'----------------------------------------';
		Print 'Loading CRM Tables';
		Print'----------------------------------------';
	
		set @start_time = GETDATE();
		print '>> Truncating table :  bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>> Inserting table :  bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\projects\analytics\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration: '+cast( datediff(second, @start_time, @end_time) as varchar) + 'seconds';
		--select * from bronze.crm_cust_info;


		set @start_time = GETDATE();
		print '>> Truncating table :  bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>> Inserting table :  bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\projects\analytics\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration: '+cast( datediff(second, @start_time, @end_time) as varchar) + 'seconds';
		--select * from bronze.crm_prd_info;


		set @start_time = GETDATE();
		print '>> Truncating table :  bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>> Inserting table :  bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\projects\analytics\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration: '+cast( datediff(second, @start_time, @end_time) as varchar) + 'seconds';
		--select * from bronze.crm_cust_info;

		Print'----------------------------------------';
		Print 'Loading ERP Tables';
		Print'----------------------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table :  bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print '>> Inserting table :  bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\projects\analytics\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration: '+cast( datediff(second, @start_time, @end_time) as varchar) + 'seconds';
		--select * from bronze.erp_cust_az12;
	

		set @start_time = GETDATE();
		print '>> Truncating table :  bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print '>> Inserting table : bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\projects\analytics\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration: '+cast( datediff(second, @start_time, @end_time) as varchar) + 'seconds';
		--select * from bronze.erp_loc_a101;


		set @start_time = GETDATE();
		print '>> Truncating table :  bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
	
		print '>> Inserting table :  bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\projects\analytics\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		);
		set @end_time = GETDATE();
		print '>> Load duration: '+cast( datediff(second, @start_time, @end_time) as varchar) + 'seconds';
		--select * from bronze.erp_px_cat_g1v2;
		set @layer_end = GETDATE();
		print 'Total time to load bronze layer: '+ cast(datediff(second, @layer_begin, @layer_end) as varchar) + 'seconds';
		END TRY
		BEGIN CATCH
			PRINT '======================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error message'+ error_message();
			PRINT 'Error Number'+ cast(error_number() as varchar);
			PRINT '======================================';
		END CATCH
	

	print 'Total time to load bronze layer: '+ cast(datediff(second, @layer_begin, @layer_end) as varchar) + 'seconds';
END

/*
====================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================
Script Purpose:

This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;

====================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bornze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_full DATETIME, @end_full DATETIME;
	
	BEGIN TRY

		PRINT '===================================';
		PRINT '      LOADING BRONZE LAYER';
		PRINT '===================================';

		PRINT '-----------------------------------';
		PRINT '      Loading CRM tables';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE()
		SET @start_full = GETDATE()
		IF OBJECT_ID ( 'bronze.crm_cust_info' , 'U' ) IS NOT NULL
		DROP TABLE bronze.crm_cust_info;

		CREATE TABLE bronze.crm_cust_info (
			cst_id INT,
			cst_key NVARCHAR(50),
			cst_firstname NVARCHAR(50),
			cst_lastname NVARCHAR(50),
			cst_material_status NVARCHAR(50),
			cst_gndr NVARCHAR(50),
			cst_create_date DATE
		);

		SET @end_time = GETDATE()
		PRINT('>>>LOAD DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		SET @start_time = GETDATE()
		IF OBJECT_ID ( 'bronze.crm_prd_info' , 'U' ) IS NOT NULL
		DROP TABLE bronze.crm_prd_info;

		CREATE TABLE bronze.crm_prd_info (
			prd_id INT,
			prd_key NVARCHAR(50),
			prd_nm NVARCHAR(50),
			prd_cost INT,
			prd_line NVARCHAR(50),
			prd_start_dt DATETIME,
			prd_end_dt DATETIME
		);

		SET @end_time = GETDATE()
		PRINT('>>>LOAD DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		SET @start_time = GETDATE()
		IF OBJECT_ID ( 'bronze.crm_sales_details' , 'U' ) IS NOT NULL
		DROP TABLE bronze.crm_sales_details;

		CREATE TABLE bronze.crm_sales_details (
			sls_ord_num NVARCHAR(50),
			sls_prd_key NVARCHAR(50),
			sls_cust_id INT,
			sls_order_dt INT,
			sls_ship_dt INT,
			sls_due_dt INT,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT
		);

		SET @end_time = GETDATE()
		PRINT('>>>LOAD DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		PRINT '-----------------------------------';
		PRINT '      Loading ERP tables';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE()
		IF OBJECT_ID ( 'bronze.erp_loc_a101' , 'U' ) IS NOT NULL
		DROP TABLE bronze.erp_loc_a101;

		CREATE TABLE bronze.erp_loc_a101 (
			cid NVARCHAR(50),
			cntry NVARCHAR(50)
		);

		SET @end_time = GETDATE()
		PRINT('>>>LOAD DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');


		SET @start_time = GETDATE()
		IF OBJECT_ID ( 'bronze.erp_cust_az12' , 'U' ) IS NOT NULL
		DROP TABLE bronze.erp_cust_az12;

		CREATE TABLE bronze.erp_cust_az12 (
			cid NVARCHAR(50),
			bdate DATE,
			gen NVARCHAR(50)
		);

		SET @end_time = GETDATE()
		PRINT('>>>LOAD DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		SET @start_time = GETDATE()
		IF OBJECT_ID ( 'bronze.erp_px_cat_g1v2' , 'U' ) IS NOT NULL
		DROP TABLE bronze.erp_px_cat_g1v2;

		CREATE TABLE bronze.erp_px_cat_g1v2 (
			id NVARCHAR(50),
			cat NVARCHAR(50),
			subcat NVARCHAR(50),
			maintenance NVARCHAR(50)
		);
		SET @end_time = GETDATE()
		PRINT('>>>LOAD DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');


		--- LOAD DATA FROM FILES ----


		PRINT '  >>>>>Truncate bronze.crm_cust_info<<<<<';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '----------------------------------------';
		PRINT ' INSERT INTO:    bronze.crm_cust_info'
		PRINT '----------------------------------------';

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Education\PYTHON\POWER BI\dashbaord\2.SQL\data warehouse Barrar\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT('>>>INSRTING DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		SET @start_time = GETDATE()
		PRINT '>>>>>>>Truncate: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '---------------------------------------------';
		PRINT '  Inserting Data into bronze.crm_prd_info';
		PRINT '---------------------------------------------';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Education\PYTHON\POWER BI\dashbaord\2.SQL\data warehouse Barrar\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT('>>>INSRTING DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		SET @start_time = GETDATE()
		PRINT '>>>>> TRUNCATE: crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '---------------------------------------------';
		PRINT '  Inserting Data into bronze.crm_sales_details';
		PRINT '---------------------------------------------';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Education\PYTHON\POWER BI\dashbaord\2.SQL\data warehouse Barrar\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT('>>>INSRTING DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');


		--- ERP Tables ---

		SET @start_time = GETDATE()
		PRINT 'TRUNCATE: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		PRINT '---------------------------------------------';
		PRINT '  Inserting Data into bronze.erp_CUST_AZ12';
		PRINT '---------------------------------------------';

		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\Education\PYTHON\POWER BI\dashbaord\2.SQL\data warehouse Barrar\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT('>>>INSRTING DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		SET @start_time = GETDATE()
		PRINT 'TRUNCATE: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101
		PRINT '---------------------------------------------';
		PRINT '  Inserting Data into bronze.erp_LOC_A101';
		PRINT '---------------------------------------------';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\Education\PYTHON\POWER BI\dashbaord\2.SQL\data warehouse Barrar\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT('>>>INSRTING DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');

		SET @start_time = GETDATE()
		PRINT 'TRUNCATE: bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_LOC_A101
		PRINT '---------------------------------------------';
		PRINT '  Inserting Data into bronze.erp_PX_CAT_G1V2';
		PRINT '---------------------------------------------';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\Education\PYTHON\POWER BI\dashbaord\2.SQL\data warehouse Barrar\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		

		PRINT('>>>INSRTING DURATION: ' +CAST( DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.');
		SET @end_full = GETDATE()
		PRINT('>>>>>Total Time: ' + CAST(DATEDIFF(second, @start_full, @end_full) AS NVARCHAR) + ' seconds'+'<<<<<');
		PRINT 'ALL WORK HAS BEEN DONE SUCCESSFULY!'
	END TRY
	BEGIN CATCH 

	PRINT '=================================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
	PRINT 'ERROR NUMBER ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR STATE ' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '=================================================';

	END CATCH
	

END



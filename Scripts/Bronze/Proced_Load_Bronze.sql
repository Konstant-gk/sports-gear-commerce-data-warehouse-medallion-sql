/*
============================================================================================================
Stored Procedure: Load Bronze Layer (Source ->  Bronze)
============================================================================================================

Script Purpose:
This script will load data into the bronze schema from external CSV files. It performs the following actions:
- Truncates the bronze tables before loading data
- Used the BULK INSERT command to load data from csv files to bronze tables

Parameteres:
-None
-This stored procedure does not accept any parameters or return any values

============================================================================================================
*/


USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze_tables AS
BEGIN 
	TRUNCATE TABLE [bronze].[crm_cust_info]
	BULK INSERT [bronze].[crm_cust_info]
	FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_Data Warehouse_Project\Dataset\source_crm\cust_info.csv'
	WITH 
		(FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);


	TRUNCATE TABLE [bronze].[crm_prd_info]
	BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_Data Warehouse_Project\Dataset\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


	TRUNCATE TABLE [bronze].[crm_sales_details]
	BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_Data Warehouse_Project\Dataset\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);



	BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_Data Warehouse_Project\Dataset\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


	TRUNCATE TABLE [bronze].[erp_loc_a101]
	BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_Data Warehouse_Project\Dataset\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


	TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]
	BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_Data Warehouse_Project\Dataset\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
END

SELECT COUNT(*) FROM [bronze].[crm_cust_info]


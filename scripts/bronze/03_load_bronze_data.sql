/*
==================================================================================================
Stored Procedure: Load Bronze Layer (Source ->  Bronze)
==================================================================================================

Script Purpose:
This script will load data into the bronze schema from external CSV files. It performs the following actions:
- Truncates the bronze tables before loading data
- Used the BULK INSERT command to load data from csv files to bronze tables

Parameteres:
-None
-This stored procedure does not accept any parameters or return any values

==================================================================================================
*/


USE DataWarehouse;
GO

EXEC bronze.load_bronze_tables 
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze_tables AS
BEGIN
	DECLARE @Start_Time DATETIME, @End_Time DATETIME, @Batch_Start_Time DATETIME, @Batch_End_Time DATETIME 
	BEGIN TRY
		SET @Batch_Start_Time = GETDATE()

		PRINT ('=================================================================================')
		PRINT ('Loading Bronze Layer Tables')
		PRINT ('=================================================================================')

		PRINT ('---------------------------------------------------------------------------------')
		PRINT ('Load CRM Tables')
		PRINT ('---------------------------------------------------------------------------------')

		
		SET @Start_Time = GETDATE() 
		PRINT ('>> Truncating Data from table bronze.crm_cust_info')
		TRUNCATE TABLE [bronze].[crm_cust_info]

		PRINT ('>> Inserting Data into table bronze.crm_cust_info')
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_DataWarehouse_Project\Dataset\source_crm\cust_info.csv'
		WITH 
			(FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')




		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table bronze.crm_prd_info')
		TRUNCATE TABLE [bronze].[crm_prd_info]

		PRINT ('>> Inserting Data into table bronze.crm_prd_info')
		BULK INSERT [bronze].[crm_prd_info]
			FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_DataWarehouse_Project\Dataset\source_crm\prd_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')


		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table bronze.crm_sales_details')
		TRUNCATE TABLE [bronze].[crm_sales_details]

		PRINT ('>> Inserting Data into table bronze.crm_sales_details')
		BULK INSERT [bronze].[crm_sales_details]
			FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_DataWarehouse_Project\Dataset\source_crm\sales_details.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')





		PRINT ('---------------------------------------------------------------------------------')
		PRINT ('Load ERP Tables')
		PRINT ('---------------------------------------------------------------------------------')


		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table bronze.erp_cust_az12')
		TRUNCATE TABLE [bronze].[erp_cust_az12]

		PRINT ('>> Inserting Data into table bronze.erp_cust_az12')
		BULK INSERT [bronze].[erp_cust_az12]
			FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_DataWarehouse_Project\Dataset\source_erp\cust_az12.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')



		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table bronze.erp_loc_a101')
		TRUNCATE TABLE [bronze].[erp_loc_a101]

		PRINT ('>> Inserting Data into table bronze.erp_loc_a101')
		BULK INSERT [bronze].[erp_loc_a101]
			FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_DataWarehouse_Project\Dataset\source_erp\loc_a101.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was:' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')



		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table bronze.erp_px_cat_g1v2')
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]

		PRINT ('>> Inserting Data into table bronze.erp_px_cat_g1v2')
		BULK INSERT [bronze].[erp_px_cat_g1v2]
			FROM 'C:\Users\Konstant\Documents\Offline_Backup\Documents\Backup\Non_Drive_Files_081225\Projects\Trainings\Portfolio_Projects\SQL_DataWarehouse_Project\Dataset\source_erp\px_cat_g1v2.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')



		SET @Batch_End_Time = GETDATE()
		PRINT ('The overall loading time of the batch was: ' + CAST(DATEDIFF(SECOND, @Batch_Start_Time, @Batch_End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')
	END TRY
	BEGIN CATCH
		PRINT ('---------------------------------------------------------------------------------')
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE()
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT ('---------------------------------------------------------------------------------')
	END CATCH
END


/*
SELECT * FROM [bronze].[crm_cust_info]
SELECT * FROM [bronze].[crm_prd_info]
SELECT * FROM [bronze].[crm_sales_details]
SELECT * FROM [bronze].[erp_loc_a101]
SELECT * FROM [bronze].[erp_loc_a101]
SELECT * FROM [bronze].[erp_px_cat_g1v2]
*/
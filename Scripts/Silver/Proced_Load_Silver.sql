/*
==================================================================================================
Stored Procedure: Load Silver Layer (Bronze ->  Silver)
==================================================================================================

Script Purpose:
This script will load data into the Silver schema from the bronze schema tables. It performs the following actions:
- Truncates the silver tables before loading data
- Used the INSERT INTO command to load data the transformed data from the bronze tables

Parameteres:
-None
-This stored procedure does not accept any parameters or return any values

==================================================================================================
*/

USE DataWarehouse;
GO


EXEC silver.load_silver_tables
GO


CREATE OR ALTER PROCEDURE silver.load_silver_tables AS
BEGIN
	BEGIN TRY
		DECLARE @Start_Time DATETIME2, @End_Time DATETIME2, @Batch_Start_Time DATETIME2, @Batch_End_Time DATETIME2

		SET @Batch_Start_Time = GETDATE() 


		PRINT ('=================================================================================')
		PRINT ('Loading Silver Layer Tables')
		PRINT ('=================================================================================')

		PRINT ('---------------------------------------------------------------------------------')
		PRINT ('Load CRM Tables')
		PRINT ('---------------------------------------------------------------------------------')



		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table silver.crm_cust_info')
		TRUNCATE TABLE [silver].[crm_cust_info]
		PRINT ('>> Inserting Data into table bronze.crm_cust_info')
		INSERT INTO [silver].[crm_cust_info] (
				[cst_id],
				[cst_key],
				[cst_firstname],
				[cst_lastname],
				[cst_marital_status],
				[cst_gndr],
				[cst_create_date]
			)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_id
			FROM [bronze].[crm_cust_info]) t
			WHERE Flag_id = 1 AND cst_id IS NOT NULL
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')






		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table silver.crm_prd_info')
		TRUNCATE TABLE [silver].[crm_prd_info]
		PRINT ('>> Inserting Data into table bronze.crm_prd_info')
		INSERT INTO [silver].[crm_prd_info]
			(	[prd_id],
				[cat_id],
				[prd_key],
				[prd_nm],
				[prd_cost],
				[prd_line],
				[prd_start_dt],
				[prd_end_dt]
			)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(UPPER(TRIM(prd_key)),1,5),'-','_') AS cat_id,
			SUBSTRING(UPPER(TRIM(prd_key)),7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			prd_start_dt,
			DATEADD(DAY,-1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM [bronze].[crm_prd_info]
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')




		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table silver.crm_sales_details')
		TRUNCATE TABLE [silver].[crm_sales_details]
		PRINT ('>> Inserting Data into table bronze.crm_sales_details')
		INSERT INTO [silver].[crm_sales_details] 
			(
			[sls_ord_num],
			[sls_prd_key],
			[sls_cust_id],
			[sls_order_dt],
			[sls_ship_dt],
			[sls_due_dt],
			[sls_sales],
			[sls_quantity],
			[sls_price]
			)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL 
				WHEN sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt THEN NULL
				WHEN sls_order_dt > 20500101 OR sls_order_dt < 19000101 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt) != 8 THEN NULL 
				WHEN sls_ship_dt < sls_order_dt THEN NULL
				WHEN sls_ship_dt > 20500101 OR sls_ship_dt < 19000101 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
				WHEN sls_due_dt < sls_order_dt THEN NULL
				WHEN sls_due_dt > 20500101 OR sls_due_dt < 19000101 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales <=0 OR sls_sales IS NULL THEN ABS(sls_price) * sls_quantity
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price <=0 OR sls_price IS NULL THEN sls_sales / sls_quantity
				ELSE ABS(sls_price)
			END sls_price
		FROM [bronze].[crm_sales_details]
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')


	
		PRINT ('---------------------------------------------------------------------------------')
		PRINT ('Load ERP Tables')
		PRINT ('---------------------------------------------------------------------------------')



		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table silver.erp_cust_az12')
		TRUNCATE TABLE [silver].[erp_cust_az12]
		PRINT ('>> Inserting Data into table bronze.erp_cust_az12')
		INSERT INTO [silver].[erp_cust_az12]
			(
			[cid],
			[bdate],
			[gen]
			)
		SELECT 
			CASE
				WHEN UPPER(TRIM(cid)) LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate > GETDATE() OR bdate < '1920-01-01' THEN NULL
				ELSE bdate
			END AS bdate,
			CASE
				WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(gen)) = 'Female' THEN gen
				WHEN UPPER(TRIM(gen)) = 'Male' THEN gen
				ELSE 'n/a'
			END AS gen
		FROM [bronze].[erp_cust_az12]
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')







		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table silver.erp_loc_a101')
		TRUNCATE TABLE [silver].[erp_loc_a101]
		PRINT ('>> Inserting Data into table bronze.erp_loc_a101')
		INSERT INTO [silver].[erp_loc_a101]
			(
			[cid],
			[cntry]
			)
		SELECT
			REPLACE(UPPER(TRIM(cid)),'-','') AS cid,
			CASE 
				WHEN cntry = 'DE' THEN 'Germany'
				WHEN cntry = 'US' OR cntry = 'USA' THEN 'United States'
				WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
				ELSE cntry
			END AS cntry
		FROM [bronze].[erp_loc_a101]
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')







		SET @Start_Time = GETDATE()
		PRINT ('>> Truncating Data from table silver.erp_px_cat_g1v2')
		TRUNCATE TABLE [silver].[erp_px_cat_g1v2]
		PRINT ('>> Inserting Data into table bronze.erp_px_cat_g1v2')
		INSERT INTO [silver].[erp_px_cat_g1v2]
			(
			[id],
			[cat],
			[subcat],
			[maintenance]
			)
		SELECT
			[id],
			[cat],
			[subcat],
			[maintenance]
		FROM [bronze].[erp_px_cat_g1v2]
		SET @End_Time = GETDATE()
		PRINT ('The loading time of the table was: ' + CAST(DATEDIFF(SECOND, @Start_Time, @End_Time) AS NVARCHAR) + ' seconds')
		PRINT ('---------------------------------------------------------------------------------')






		SET @Batch_End_Time = GETDATE()
		PRINT 'Procesure loading time was ' + CAST(DATEDIFF(second, @Batch_End_Time, @Batch_Start_Time) AS NVARCHAR) + ' seconds'
		PRINT ('---------------------------------------------------------------------------------')

	END TRY
	BEGIN CATCH
		PRINT ('---------------------------------------------------------------------------------')
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'The query has the following error : ' + ERROR_MESSAGE()
		PRINT 'The query has the following error nunmber: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'The query has the following error state: ' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT ('---------------------------------------------------------------------------------')
	END CATCH
END
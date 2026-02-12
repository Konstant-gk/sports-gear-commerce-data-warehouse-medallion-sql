/*


*/


USE DataWarehouse;
GO


TRUNCATE TABLE [silver].[crm_cust_info]
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




TRUNCATE TABLE [silver].[crm_prd_info]
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




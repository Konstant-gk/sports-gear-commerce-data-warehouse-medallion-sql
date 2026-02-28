/*

============================
Script: Data Quality checks
============================

This script shows data quality checks during the data transformation in the tables for the silver layer

*/



USE DataWarehouse;
GO

--crm_cust_info
----------------------------------------


--Check for duplicates in primary key 
SELECT cst_id, COUNT(*) AS duplicate_ids
FROM [bronze].[crm_cust_info]
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Check for duplicates in specific case
SELECT *
FROM [bronze].[crm_cust_info]
WHERE cst_id = 29466 

--Verification Check for duplicates in primary key
SELECT cst_id, COUNT(*) AS duplicate_ids
FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_id
	FROM [bronze].[crm_cust_info]) t
WHERE Flag_id = 1
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


--Check for white spaces and blanks in Strings
SELECT
cst_gndr
FROM SILVER.[crm_cust_info]
WHERE	cst_gndr != TRIM(cst_gndr) 

--Check distinct values in low cardinality columns
SELECT DISTINCT
cst_gndr
FROM [bronze].[crm_cust_info]


--Verification distinct values in low cardinality columns after replacement
SELECT DISTINCT
cst_gndr,
CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	ELSE 'n/a'
END 
FROM [bronze].[crm_cust_info]

--Check for Invalid dates or out of range dates
SELECT
cst_create_date
FROM [bronze].[crm_cust_info]
WHERE cst_create_date > '2050-01-01' OR cst_create_date < '1900-01-01'



--crm_pd_info
----------------------------------------


--Check for duplicates in primary key 
SELECT prd_id, COUNT(*) AS duplicate_ids
FROM [bronze].[crm_prd_info]
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


--Check for white spaces and blanks in Strings
SELECT
prd_line
FROM [bronze].[crm_prd_info]
WHERE prd_line != TRIM(prd_line) 


--Check if there are unmatching ids in the other table with matching ids
WITH cleaned_id AS (
SELECT
	REPLACE(SUBSTRING(UPPER(TRIM(prd_key)),1,5),'-','_') AS cat_id
FROM [bronze].[crm_prd_info])
SELECT cat_id
FROM cleaned_id c
WHERE NOT EXISTS (SELECT 1 FROM [bronze].[erp_px_cat_g1v2] e WHERE c.cat_id = e.id )


--Verify if the unmatched ids do not simply exist as options in the other table
SELECT 
* 
FROM [bronze].[erp_px_cat_g1v2] 
WHERE id = 'CO_PE'


--Check for negative, 0 or null values in price
SELECT
*
FROM [bronze].[crm_prd_info]
WHERE prd_cost IS NULL OR prd_cost <= 0 


--Check distinct values in low cardinality columns
SELECT DISTINCT
prd_line
FROM [bronze].[crm_prd_info]


--Check for invalid dates 
SELECT
*
FROM [bronze].[crm_prd_info]
WHERE prd_start_dt > prd_end_dt OR prd_start_dt IS NULL OR prd_end_dt IS NULL
ORDER BY prd_id


--Verify Invalid Dates
SELECT
	*,
	DATEADD(DAY,-1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM [bronze].[crm_prd_info]
WHERE prd_key = 'CL-CA-CA-1098' OR prd_key = 'AC-HE-HL-U509'



--crm_sales_details
----------------------------------------

--Check for white spaces and blanks in Strings
SELECT
sls_ord_num
FROM [bronze].[crm_sales_details]
WHERE sls_ord_num != TRIM(sls_ord_num) OR sls_ord_num IS NULL


--Check if there are unmatching ids in the other table with matching ids
SELECT
	sls_cust_id
FROM [bronze].[crm_sales_details]
WHERE sls_cust_id NOT IN (SELECT cst_id FROM [silver].[crm_cust_info])


--Check for nulls 
SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM [bronze].[crm_sales_details]
WHERE sls_order_dt IS NULL OR sls_ship_dt IS NULL OR sls_due_dt IS NULL


--Check for invalid dates or out of range date 
SELECT
NULLIF(sls_order_dt,0), sls_order_dt
FROM [bronze].[crm_sales_details]
WHERE
	LEN(sls_order_dt) != 8 OR sls_order_dt <=0 OR
	sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt OR
	sls_order_dt > 20500101 OR sls_order_dt < 19000101 OR 
	sls_order_dt IS NULL


--Check for negative, 0 or null values in price
SELECT
	sls_price,
	sls_sales,
	sls_quantity,
	CASE
		WHEN sls_sales <=0 OR sls_sales IS NULL THEN ABS(sls_price) * sls_quantity
		ELSE sls_sales
	END AS new_sales,
	CASE
		WHEN sls_price <=0 OR sls_price IS NULL THEN ABS(sls_sales) / sls_quantity
		ELSE ABS(sls_price)
	END AS new_price
FROM [bronze].[crm_sales_details]
WHERE	sls_sales IS NULL OR sls_sales <= 0 OR
		sls_price IS NULL OR sls_price <= 0 OR
		sls_quantity IS NULL OR sls_quantity <= 0 OR
		sls_sales != sls_price * sls_quantity



--erp_cust_az12
----------------------------------------


--Check if there are unmatching ids with CTE in the other table with matching ids
WITH cleaned_ids AS (
SELECT
	CASE
		WHEN UPPER(TRIM(cid)) LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid 
	END AS new_cid
FROM [bronze].[erp_cust_az12])
SELECT 
*
FROM cleaned_ids cs
WHERE NOT EXISTS (SELECT 1 FROM [silver].[crm_cust_info] ci WHERE ci.cst_key = cs.new_cid );


--Check if there are unmatching ids with subquery  in the other table with matching ids

SELECT
*
FROM (
	SELECT
	CASE
		WHEN UPPER(TRIM(cid)) LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid 
	END AS new_cid
FROM [bronze].[erp_cust_az12]) t
WHERE new_cid NOT IN ( SELECT cst_key FROM [silver].[crm_cust_info]);


--Check if there are unmatching ids in the other table with matching ids

SELECT
cid
FROM [bronze].[erp_cust_az12]
WHERE cid NOT IN (SELECT cst_key FROM [silver].[crm_cust_info])


--Check narrowed down values with specific characteristic
SELECT 
	*
FROM [bronze].[erp_cust_az12]
WHERE cid LIKE 'NAS%'


--Check distinct values in low cardinality columns
SELECT DISTINCT
gen, COUNT(*)
FROM [bronze].[erp_cust_az12]
GROUP BY gen
HAVING COUNT(*) >1 OR gen IS NULL


--Check for white spaces and blanks in Strings
SELECT
cid
FROM [bronze].[erp_cust_az12]
WHERE cid != TRIM(cid)


--Check for invalid dates or out of range date 
SELECT
	bdate,
	CASE
		WHEN bdate > GETDATE() OR bdate < '1920-01-01' THEN NULL
		ELSE bdate
	END AS bdate
FROM [bronze].[erp_cust_az12]
WHERE bdate > GETDATE() OR bdate < '1920-01-01'






--erp_loc_a101
----------------------------------------

--Check unmatching ids values in different tables
SELECT
	REPLACE(UPPER(TRIM(cid)),'-','') AS cid
FROM [bronze].[erp_loc_a101] 
WHERE REPLACE(UPPER(TRIM(cid)),'-','') NOT IN (SELECT cst_key FROM [silver].[crm_cust_info]);

-- Check for white spaces and blanks in Strings
SELECT
cid
FROM [bronze].[erp_loc_a101]
WHERE cid != TRIM(cid)


-- Check the length of ID
SELECT
cid
FROM [bronze].[erp_loc_a101]
WHERE LEN(cid) != 11


-- Check distinct choices in low cardinality columns
SELECT DISTINCT
cntry
FROM [bronze].[erp_loc_a101] 


--erp_px_cat_g1v2
------------------

-- Check distinct choices in low cardinality columns
SELECT DISTINCT
cat
FROM [bronze].[erp_px_cat_g1v2] 


-- Check for white spaces and blanks in Strings
SELECT
maintenance
FROM [bronze].[erp_px_cat_g1v2]
WHERE maintenance != TRIM(maintenance)



--Check unmatching ids values in different tables
SELECT
id
FROM [bronze].[erp_px_cat_g1v2]
WHERE id NOT IN (SELECT prd_key FROM silver.crm_prd_info);

--Verify unmatching ids values in different tables
SELECT prd_key
FROM silver.crm_prd_info
WHERE prd_key LIKE 'CL_VE%' OR prd_key LIKE 'AC_BS%' OR prd_key LIKE 'BI_TB%'
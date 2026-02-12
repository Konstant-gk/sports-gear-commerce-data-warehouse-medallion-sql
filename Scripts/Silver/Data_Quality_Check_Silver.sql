
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
cst_create_date,
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




SELECT * FROM [bronze].[crm_sales_details]

/*

============================
Script: Data Integration checks
============================

This script shows data quality checks during the data transformation in the views for the gold layer

*/




--Check ids have duplicates
---------------------

SELECT 
cst_id, COUNT(*)
FROM (
SELECT
	cc.[cst_id],
	cc.[cst_key],
	cc.[cst_marital_status],
	cc.[cst_gndr],
	ec.[gen]
FROM [silver].[crm_cust_info] cc
LEFT JOIN [silver].[erp_loc_a101] el
ON cc.cst_key = el.cid
LEFT JOIN [silver].[erp_cust_az12] ec
ON cc.cst_key = ec.cid) t
GROUP BY cst_id
HAVING COUNT(*) > 1



--Check if columns have unmatching data
---------------------

SELECT DISTINCT
	cc.[cst_gndr],
	ec.[gen]
FROM [silver].[crm_cust_info] cc
LEFT JOIN [silver].[erp_loc_a101] el
ON cc.cst_key = el.cid
LEFT JOIN [silver].[erp_cust_az12] ec
ON cc.cst_key = ec.cid


--Check end dates with NULLS for the sae product key
SELECT 
*
FROM [silver].[crm_prd_info]
ORDER BY prd_id


--Check connection between dimension tables and fact table

SELECT
*
FROM [gold].[fact_sales] a
LEFT JOIN [gold].[dim_customer] b
ON a.customer_key = b.customer_key
LEFT JOIN [gold].[dim_product] c
ON a.product_key = c.product_key

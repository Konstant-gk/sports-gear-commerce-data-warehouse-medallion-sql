/*
==================================================================================================
DDL Script: Create Gold Views
==================================================================================================

Script Purpose:
This script creates views for the Gold layer in the data warehosue.
The Gold represents the final dimension and fact tables (star schema)

Each view performs transformations and combines data from the silver layer to produce a clean, enriched and business-iready dataset

==================================================================================================


*/

USE DataWarehouse;
GO


--Dim Customer View

CREATE VIEW gold.dim_customer AS 
SELECT
	cc.[cst_id] AS customer_id,
	ROW_NUMBER() OVER (ORDER BY cc.cst_id) AS customer_key,
	cc.[cst_firstname] AS customer_first_name,
	cc.[cst_lastname] AS customer_lastname,
	CASE
		WHEN cc.[cst_gndr] != 'n/a' THEN cc.[cst_gndr] --Gender data integration, CRM source
		ELSE COALESCE(ec.gen, 'n/a')
	END AS customer_gender,
	cc.[cst_marital_status] AS customer_marital_status,
	el.[cntry] AS customer_country,
	ec.[bdate] AS customer_birthdate,
	cc.[cst_create_date] AS customer_create_date
FROM [silver].[crm_cust_info] cc
LEFT JOIN [silver].[erp_loc_a101] el
ON cc.cst_key = el.cid
LEFT JOIN [silver].[erp_cust_az12] ec
ON cc.cst_key = ec.cid


-- Dim Product View
CREATE VIEW gold.dim_product AS
SELECT
	cp.[prd_id] AS product_id,
	ROW_NUMBER() OVER (ORDER BY cp.prd_id, cp.prd_start_dt) AS product_key,
	cp.[prd_nm] AS product_name,
	cp.[cat_id] AS product_number,
	cp.[prd_cost] AS product_cost,
	cp.[prd_line] AS product_line,
	cp.[prd_key] AS category_id,
	ep.[cat] AS product_category,
	ep.[subcat] AS product_subcategory,
	ep.[maintenance] AS product_maintenance,
	cp.[prd_start_dt] AS product_start_date
	FROM [silver].[crm_prd_info] cp
LEFT JOIN [silver].[erp_px_cat_g1v2] ep
ON cp.cat_id = ep.id
WHERE cp.[prd_end_dt] IS NULL  --filtered out historization for the date

SELECT * FROM [gold].[dim_product]


--Fact Sales View
CREATE VIEW gold.fact_sales AS
SELECT 
	cs.[sls_ord_num] AS order_number,
	dp.product_key AS product_key,
	dc.customer_key AS customer_key,
	cs.[sls_sales] AS sales_amount,
	cs.[sls_price] AS price,
	cs.[sls_quantity] AS quantity,
	cs.[sls_order_dt] AS order_date,
	cs.[sls_ship_dt] AS shipping_date,
	cs.[sls_due_dt]AS due_date
FROM [silver].[crm_sales_details] cs
LEFT JOIN [gold].[dim_product] dp
ON cs.sls_prd_key = dp.category_id
LEFT JOIN [gold].[dim_customer] dc
ON cs.sls_cust_id = dc.customer_id


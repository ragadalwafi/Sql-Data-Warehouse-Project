/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
	This script creates views for the Gold layer in the data warehouse.
	The Gold layer represents the final dimension and fact tables (Star Schema).
	Each view performs transformations and combines data from the Silver layer
	to produce a clean, enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reporting.
===============================================================================
*/


-- ============================================================================
-- Create Dimension View: gold.dim_customers
-- ============================================================================

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cust_id) AS customer_key,
	ci.cust_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	loc.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CAST(
  CASE 
   WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
   ELSE REPLACE(COALESCE(ca.gen, 'Unkown'), 'Unkown', 'Unknown')
  END AS NVARCHAR(50)
	) AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 loc
ON ci.cst_key = loc.cid;



-- ============================================================================
-- Create Dimension View: gold.dim_products
-- ============================================================================

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY p.prd_start_dt, p.prd_key) AS product_key,
	p.prd_id AS product_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	px.cat AS category,
	px.subcat AS subcategory,
	px.maintenance,
	p.prd_cost AS cost,
	p.prd_line AS product_line,
	p.prd_start_dt AS start_date
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 px
ON p.cat_id = px.id
WHERE p.prd_end_dt IS NULL;



-- ============================================================================
-- Create Fact View: gold.fact_sales
-- ============================================================================

CREATE VIEW gold.fact_sales AS 
SELECT 
sl.sls_ord_num AS order_number, 
cu.customer_key,
p.product_key,
sl.sls_order_dt AS order_date,
sl.sls_ship_dt AS shipping_date,
sl.sls_due_dt AS due_date,
sl.sls_sales AS sales,
sl.sls_quantity AS quantity,
sl.sls_price AS price
FROM silver.crm_sales_details sl 
LEFT JOIN gold.dim_customers cu
ON sl.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products p
ON sl.sls_prd_key = p.product_number;

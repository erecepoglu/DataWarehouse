/* =========================================================================================
   Script Name:   Create_Gold_Layer_Views.sql
   Description:   Creates the consumption-ready presentation layer (Gold Layer) views.
                  This establishes a Star Schema consisting of two dimensions 
                  (dim_customers, dim_products) and one fact view (fact_sales).
   
   Layer:         Gold
   Architecture:  Medallion Architecture
   Dependencies:  silver.crm_cust_info, silver.erp_cust_az12, silver.erp_loc_a101,
                  silver.crm_prd_info, silver.erp_px_cat_g1v2, silver.crm_sales_details
   
   Notes:         - Uses CREATE OR ALTER to ensure the script is idempotent.
                  - WARNING: surrogate keys (customer_key, product_key) are generated 
                    dynamically using ROW_NUMBER() in these views. In a production 
                    environment, consider materializing these into physical tables 
                    to ensure surrogate keys remain persistent.
========================================================================================= */

-- 1. Create Customer Dimension
CREATE OR ALTER VIEW gold.dim_customers AS 
SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id                             AS customer_id,
    ci.cst_key                            AS customer_number,
    ci.cst_firstname                      AS first_name,
    ci.cst_lastname                       AS last_name,
    la.cntry                              AS country,
    ci.cst_material_status                AS marital_status,
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'n/a')
    END                                   AS gender,
    ca.bdate                              AS birth_date,
    ci.cst_create_date                    AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- 2. Create Product Dimension
CREATE OR ALTER VIEW gold.dim_products AS
SELECT  
    ROW_NUMBER() OVER(ORDER BY prd_info.prd_start_dt, prd_info.prd_key) AS product_key,
    prd_info.prd_id        AS product_id,
    prd_info.prd_key       AS product_number,
    prd_info.prd_nm        AS product_name,
    prd_info.cat_id        AS category_id,
    pc.cat                 AS category,
    pc.subcat              AS subcategory,
    pc.maintenance         AS maintenance,
    prd_info.prd_cost      AS product_cost,
    prd_info.prd_line      AS product_line,
    prd_info.prd_start_dt  AS start_date
FROM silver.crm_prd_info prd_info
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON prd_info.cat_id = pc.id
WHERE prd_info.prd_end_dt IS NULL;
GO

-- 3. Create Sales Fact
CREATE OR ALTER VIEW gold.fact_sales AS 
SELECT
    sd.sls_ord_num   AS order_number,
    pr.product_key   AS product_key,
    cu.customer_key  AS customer_key,
    sd.sls_order_dt  AS order_date,
    sd.sls_ship_dt   AS shipping_date,
    sd.sls_due_dt    AS due_date,
    sd.sls_sales     AS sales,
    sd.sls_quantity  AS quantity,
    sd.sls_price     AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

# Data Catalog: Gold Layer

The Gold layer in this medallion architecture represents the presentation-ready, consumption layer. It is organized into a star schema consisting of two dimension views (`dim_customers`, `dim_products`) and one fact view (`fact_sales`).

---

## 1. Dimension View: `gold.dim_customers`

**Description:** A consolidated customer dimension view combining core CRM customer data with ERP-sourced geographic and demographic information.  
**Source Dependencies:** `silver.crm_cust_info`, `silver.erp_cust_az12`, `silver.erp_loc_a101`  
**Grain:** One row per unique `customer_id` (`cst_id`).

| Column Name | Description | Source / Transformation Logic |
| :--- | :--- | :--- |
| **`customer_key`** | Primary Surrogate Key for the customer. | Generated dynamically using `ROW_NUMBER()` ordered by `cst_id`. |
| **`customer_id`** | Natural key / CRM identifier for the customer. | `silver.crm_cust_info.cst_id` |
| **`customer_number`** | Alternate unique identifier / customer number. | `silver.crm_cust_info.cst_key` |
| **`first_name`** | Customer's first name. | `silver.crm_cust_info.cst_firstname` |
| **`last_name`** | Customer's last name. | `silver.crm_cust_info.cst_lastname` |
| **`country`** | Country of residence or operation. | `silver.erp_loc_a101.cntry` |
| **`marital_status`** | Customer's marital status. | `silver.crm_cust_info.cst_material_status` |
| **`gender`** | Customer's gender. | Takes valid gender from CRM; if 'n/a', falls back to ERP (`erp_cust_az12.gen`). Nulls default to 'n/a'. |
| **`birth_date`** | Customer's date of birth. | `silver.erp_cust_az12.bdate` |
| **`create_date`** | Date the customer record was created. | `silver.crm_cust_info.cst_create_date` |

---

## 2. Dimension View: `gold.dim_products`

**Description:** A product dimension view detailing product attributes, hierarchies, and costs. It represents the *current* active state of products by filtering out inactive or discontinued items.  
**Source Dependencies:** `silver.crm_prd_info`, `silver.erp_px_cat_g1v2`  
**Grain:** One row per active product.

| Column Name | Description | Source / Transformation Logic |
| :--- | :--- | :--- |
| **`product_key`** | Primary Surrogate Key for the product. | Generated dynamically using `ROW_NUMBER()` ordered by start date and product key. |
| **`product_id`** | Natural key / system identifier for the product. | `silver.crm_prd_info.prd_id` |
| **`product_number`** | Alternate unique identifier / product number. | `silver.crm_prd_info.prd_key` |
| **`product_name`** | Display name of the product. | `silver.crm_prd_info.prd_nm` |
| **`category_id`** | Identifier linking to the product's category. | `silver.crm_prd_info.cat_id` |
| **`category`** | Top-level product category name. | `silver.erp_px_cat_g1v2.cat` |
| **`subcategory`** | Secondary product categorization name. | `silver.erp_px_cat_g1v2.subcat` |
| **`maintenance`** | Maintenance or service indicator for the category. | `silver.erp_px_cat_g1v2.maintenance` |
| **`product_cost`** | Unit cost of the product. | `silver.crm_prd_info.prd_cost` |
| **`product_line`** | Manufacturing or distribution line. | `silver.crm_prd_info.prd_line` |
| **`start_date`** | Date the product became active. | `silver.crm_prd_info.prd_start_dt` |

> **Note on Transformation Logic:** This view explicitly filters for active products using `WHERE prd_info.prd_end_dt IS NULL`.

---

## 3. Fact View: `gold.fact_sales`

**Description:** The core sales transaction fact view capturing order details, dates, and financial metrics. It resolves natural keys from the Silver layer into surrogate keys (`customer_key`, `product_key`) from the Gold dimensions.  
**Source Dependencies:** `silver.crm_sales_details`, `gold.dim_products`, `gold.dim_customers`  
**Grain:** One row per individual sales order line item.

| Column Name | Description | Source / Transformation Logic |
| :--- | :--- | :--- |
| **`order_number`** | Unique transaction identifier for the sales order. | `silver.crm_sales_details.sls_ord_num` |
| **`product_key`** | Foreign Key referencing `gold.dim_products`. | Retrieved via `LEFT JOIN` on `sd.sls_prd_key = pr.product_number`. |
| **`customer_key`** | Foreign Key referencing `gold.dim_customers`. | Retrieved via `LEFT JOIN` on `sd.sls_cust_id = cu.customer_id`. |
| **`order_date`** | Date the order was placed. | `silver.crm_sales_details.sls_order_dt` |
| **`shipping_date`** | Date the order was shipped. | `silver.crm_sales_details.sls_ship_dt` |
| **`due_date`** | Date the order payment/delivery is due. | `silver.crm_sales_details.sls_due_dt` |
| **`sales`** | Total sales value/revenue for the line item. | `silver.crm_sales_details.sls_sales` |
| **`quantity`** | Number of units purchased. | `silver.crm_sales_details.sls_quantity` |
| **`price`** | Unit price of the product sold. | `silver.crm_sales_details.sls_price` |

# Enterprise Data Warehouse: CRM & ERP Integration 

## 📖 Project Overview
This project establishes a scalable data warehouse using the **Medallion Architecture** (Bronze, Silver, Gold). It integrates customer, product, and sales data from disparate source systems (CRM and ERP) into a clean, presentation-ready Star Schema. 

The primary goal is to provide a unified source of truth for downstream analytics and Business Intelligence (BI) reporting.

---

## 🏗️ Architecture

The data pipeline follows a multi-hop architecture:

* **Bronze Layer (Raw):** Raw data ingested directly from source systems (CRM and ERP) in its original format. *(Assumed upstream)*
* **Silver Layer (Cleansed & Conformed):** Data is cleaned, standardized, and filtered. Source-specific anomalies are addressed, but the data remains largely in its original relational structure.
* **Gold Layer (Presentation):** Business-level transformations are applied. Data is modeled into a Star Schema (Facts and Dimensions) optimized for querying and reporting.

### Source Systems
* **CRM System:** Contains core customer profiles, product catalogs, and transactional sales details.
* **ERP System:** Contains supplementary demographic data, geographic locations, and product categorization hierarchies.

---

## 📊 Data Model: The Gold Layer

The Gold layer utilizes a **Star Schema** to simplify queries and improve performance for analytical workloads.

### 1. `gold.dim_customers` (Dimension)
A consolidated view of customer entities.
* **Sources:** `silver.crm_cust_info`, `silver.erp_cust_az12`, `silver.erp_loc_a101`
* **Key Logic:** Merges CRM profile data with ERP geographic/demographic data. Includes fallback logic for missing demographic info (e.g., defaulting missing genders to the ERP system or 'n/a').

### 2. `gold.dim_products` (Dimension)
A master view of the product catalog.
* **Sources:** `silver.crm_prd_info`, `silver.erp_px_cat_g1v2`
* **Key Logic:** Resolves product categories and subcategories. Explicitly filters for currently active products (`prd_end_dt IS NULL`).

### 3. `gold.fact_sales` (Fact)
The central transaction table recording individual sales order line items.
* **Sources:** `silver.crm_sales_details`
* **Key Logic:** Maps natural business keys to the surrogate keys (`customer_key`, `product_key`) generated in the dimension views to enforce the Star Schema relationships.

---

## ⚙️ Key Technical Decisions & Notes

* **Idempotency:** The deployment scripts utilize `CREATE OR ALTER VIEW` to ensure they can be re-run safely without manually dropping existing objects.
* **Surrogate Keys:** Currently, surrogate keys (`customer_key`, `product_key`) are generated dynamically using `ROW_NUMBER() OVER(ORDER BY...)`. 
    > **Note for Production:** Because these are Views, window-function-based surrogate keys compute on the fly. If historical source data changes, the keys could shift. For a robust production environment, consider materializing these views into physical tables.
* **Data Quality Handling:** `COALESCE` functions are used to handle `NULL` values gracefully during the merge of CRM and ERP data.

---

## 🚀 Setup & Deployment

1. Ensure the **Silver Layer** tables/views are fully populated and accessible in your database environment.
2. Execute the deployment script:
   ```sql
   -- Run the main Gold layer generation script
   EXEC ddl_gold.sql

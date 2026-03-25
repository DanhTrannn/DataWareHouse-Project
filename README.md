# DataWareHouse-Project

This project builds a complete **data warehouse** from the public Olist Brazilian e-commerce dataset. It follows a three-layer architecture (Bronze → Silver → Gold) to ingest raw data, apply cleansing and standardization, and finally model it into a star schema for business analytics.

---

## 📖 Overview

- **Dataset**: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) – 9 CSV files with 100k orders (2016–2018), covering customers, products, payments, reviews, shipping, etc.
- **Goal**:
  - Build a modern data warehouse following industry best practices.
  - Cleanse, standardize, and integrate raw data.
  - Design a star schema to enable analytical queries on customer behavior, product performance, sales trends, and delivery efficiency.

---

## 🏗️ Architecture

The project adopts a **three-layer (Medallion) architecture** implemented entirely in SQL Server

| Layer      | Description |
|------------|-------------|
| **Bronze** | Raw data ingested directly from CSV files, kept as-is. |
| **Silver** | Cleansed and standardized data: type conversion, null handling, deduplication, text normalization. |
| **Gold**   | Business-ready star schema (fact and dimension tables) optimized for analytics. |

*While Medallion Architecture is often associated with data lakehouses, the same logical layering applies perfectly to traditional data warehouses.*

![Architecture Diagram](docs/data_architecture.png)  
*(Place your actual diagram in the `docs/` folder)*

---

## 📂 Detailed Layer Breakdown

### 1. Bronze Layer – Raw Data

**Schema**: `bronze`  
**Tables** (one per CSV file):

| Bronze Table                         | Source File                           |
|--------------------------------------|---------------------------------------|
| `bronze.olist_customers`             | olist_customers_dataset.csv           |
| `bronze.olist_geolocation`           | olist_geolocation_dataset.csv         |
| `bronze.olist_order_items`           | olist_order_items_dataset.csv         |
| `bronze.olist_order_payments`        | olist_order_payments_dataset.csv      |
| `bronze.olist_order_reviews`         | olist_order_reviews_dataset.csv       |
| `bronze.olist_orders`                | olist_orders_dataset.csv              |
| `bronze.olist_products`              | olist_products_dataset.csv            |
| `bronze.olist_sellers`               | olist_sellers_dataset.csv             |
| `bronze.product_category_name_translation` | product_category_name_translation.csv |

**Characteristics**:
- All columns initially stored as `VARCHAR(MAX)` (or `NVARCHAR` for Portuguese text).
- Two audit columns: `ingestion_date DATETIME DEFAULT GETDATE()`, `source_file VARCHAR(255)`.

### 2. Silver Layer – Cleansed & Standardized

**Schema**: `silver`  
**Key transformations**:
- Correct data types (e.g., `DECIMAL`, `INT`, `DATE`).
- Trim and lower text fields.
- Handle NULLs (replace with `'Unknown'`, `0`, or filter out invalid rows).
- Remove duplicates based on natural keys.
- Filter out records that break referential integrity (e.g., orders without items).

**Tables** mirror Bronze structure but with cleaned data:
- `silver.customers`
- `silver.geolocation`
- `silver.order_items`
- `silver.order_payments`
- `silver.order_reviews`
- `silver.orders`
- `silver.products`
- `silver.sellers`
- `silver.product_category_translation`

### 3. Gold Layer – Star Schema

**Schema**: `gold`  
**Fact table**:
- `gold.fact_orders`  
  **Grain**: one row per order item (`order_item_id`).  
  **Columns**:
  - `order_id`, `order_item_id`
  - `customer_id`, `seller_id`, `product_id`
  - `order_date_key`, `approved_date_key`, `delivered_date_key`, `estimated_delivery_date_key`
  - `price`, `freight_value`, `quantity`
  - `payment_value` (aggregated per order)
  - `review_score`

**Dimension tables**:
- `gold.dim_customer` – customer attributes and location.
- `gold.dim_product` – product details, category, dimensions, weight.
- `gold.dim_seller` – seller location.
- `gold.dim_order` – order status (non‑date attributes).
- `gold.dim_date` – date dimension covering 2016–2018 (plus surrounding years).
- `gold.dim_payment_method` – payment method types.

---

## 🚀 Getting Started

### Prerequisites
- SSMS
- SSIS
- Git

### Steps

1. **Clone the repository**  
   ```bash
   git clone https://github.com/DanhTrannn/DataWareHouse-Project.git
   cd DataWareHouse-Project

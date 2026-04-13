# 📘 GUIDE_TV2.md – Hướng dẫn chi tiết Thành viên 2

## Domain: Sản phẩm & Doanh thu

**Phụ trách:**
- Staging: `stg_products`, `stg_category_translation`, `stg_order_items`
- Dimensions: `dim_product_category` (SCD Type 1), `dim_product` (SCD Type 1), `dim_payment_method`
- Facts: `fact_orders` (Incremental Load), `fact_sales`, `fact_sales_year`
- SSIS Packages: 4 packages

---

## BƯỚC 0: Chuẩn bị

### 0.1. Xác nhận Database đã tạo (TV1 thực hiện)

```sql
USE OlistDW;
GO
-- Verify schemas exist
SELECT name FROM sys.schemas WHERE name IN ('staging', 'gold');
```

### 0.2. Tạo SSIS Packages

Trong Solution Explorer của project `OlistDW_ETL`, thêm 4 packages:
- `Extract_Product_Items.dtsx`
- `Load_Dim_Product.dtsx`
- `Load_Fact_Orders.dtsx`
- `Load_Fact_Sales.dtsx`

### 0.3. Sử dụng lại Connection Manager

Dùng chung `OlistDW_OLEDB` Connection Manager đã tạo ở project level.

---

## BƯỚC 1: Tạo Staging Tables (DDL)

### 1.1. staging.stg_products

```sql
IF OBJECT_ID('staging.stg_products', 'U') IS NOT NULL
    DROP TABLE staging.stg_products;
GO

CREATE TABLE staging.stg_products (
    product_id                  VARCHAR(50)    NOT NULL,
    product_category_name       NVARCHAR(100)  NULL,
    product_name_lenght         INT            NULL,  -- typo gốc trong dataset
    product_description_lenght  INT            NULL,  -- typo gốc trong dataset
    product_photos_qty          INT            NULL,
    product_weight_g            DECIMAL(10,2)  NULL,
    product_length_cm           DECIMAL(10,2)  NULL,
    product_height_cm           DECIMAL(10,2)  NULL,
    product_width_cm            DECIMAL(10,2)  NULL
);
GO
```

### 1.2. staging.stg_category_translation

```sql
IF OBJECT_ID('staging.stg_category_translation', 'U') IS NOT NULL
    DROP TABLE staging.stg_category_translation;
GO

CREATE TABLE staging.stg_category_translation (
    product_category_name         NVARCHAR(100) NOT NULL,
    product_category_name_english NVARCHAR(100) NOT NULL
);
GO
```

### 1.3. staging.stg_order_items

```sql
IF OBJECT_ID('staging.stg_order_items', 'U') IS NOT NULL
    DROP TABLE staging.stg_order_items;
GO

CREATE TABLE staging.stg_order_items (
    order_id            VARCHAR(50)   NOT NULL,
    order_item_id       INT           NOT NULL,
    product_id          VARCHAR(50)   NOT NULL,
    seller_id           VARCHAR(50)   NOT NULL,
    shipping_limit_date DATETIME      NULL,
    price               DECIMAL(10,2) NOT NULL,
    freight_value       DECIMAL(10,2) NOT NULL
);
GO
```

---

## BƯỚC 2: Tạo Dimension Tables (DDL)

### 2.1. gold.dim_product_category

```sql
IF OBJECT_ID('gold.dim_product_category', 'U') IS NOT NULL
    DROP TABLE gold.dim_product_category;
GO

CREATE TABLE gold.dim_product_category (
    category_key              INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    category_name_portuguese  NVARCHAR(100)     NOT NULL,
    category_name_english     NVARCHAR(100)     NULL
);
GO

CREATE UNIQUE INDEX UX_dim_prodcat_name
    ON gold.dim_product_category(category_name_portuguese);
GO
```

### 2.2. gold.dim_product

```sql
IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL
    DROP TABLE gold.dim_product;
GO

CREATE TABLE gold.dim_product (
    product_key               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    product_id                VARCHAR(50)        NOT NULL,
    category_key              INT                NULL,
    product_name_length       INT                NULL,
    product_description_length INT               NULL,
    product_photos_qty        INT                NULL,
    product_weight_g          DECIMAL(10,2)      NULL,
    product_length_cm         DECIMAL(10,2)      NULL,
    product_height_cm         DECIMAL(10,2)      NULL,
    product_width_cm          DECIMAL(10,2)      NULL,
    CONSTRAINT FK_dim_product_category FOREIGN KEY (category_key)
        REFERENCES gold.dim_product_category(category_key)
);
GO

CREATE UNIQUE INDEX UX_dim_product_id ON gold.dim_product(product_id);
GO
```

### 2.3. gold.dim_payment_method

```sql
IF OBJECT_ID('gold.dim_payment_method', 'U') IS NOT NULL
    DROP TABLE gold.dim_payment_method;
GO

CREATE TABLE gold.dim_payment_method (
    payment_type  VARCHAR(30)   NOT NULL PRIMARY KEY,
    description   NVARCHAR(100) NULL
);
GO
```

---

## BƯỚC 3: Tạo Fact Tables (DDL)

### 3.1. gold.fact_orders (Transaction Fact – bảng chính của toàn bộ DW)

```sql
IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL
    DROP TABLE gold.fact_orders;
GO

CREATE TABLE gold.fact_orders (
    fact_order_item_id          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    order_id                    VARCHAR(50)        NOT NULL,
    order_item_id               INT                NOT NULL,
    customer_key                INT                NULL,
    seller_key                  INT                NULL,
    product_key                 INT                NULL,
    order_date_key              INT                NULL,
    approved_date_key           INT                NULL,
    delivered_date_key          INT                NULL,
    estimated_delivery_date_key INT                NULL,
    order_status                VARCHAR(30)        NULL,
    price                       DECIMAL(10,2)      NOT NULL,
    freight_value               DECIMAL(10,2)      NOT NULL,
    quantity                    INT                NOT NULL DEFAULT 1,
    review_score                INT                NULL,
    CONSTRAINT FK_fo_customer FOREIGN KEY (customer_key)
        REFERENCES gold.dim_customer(customer_key),
    CONSTRAINT FK_fo_seller FOREIGN KEY (seller_key)
        REFERENCES gold.dim_seller(seller_key),
    CONSTRAINT FK_fo_product FOREIGN KEY (product_key)
        REFERENCES gold.dim_product(product_key),
    CONSTRAINT FK_fo_order_date FOREIGN KEY (order_date_key)
        REFERENCES gold.dim_date(date_key),
    CONSTRAINT FK_fo_status FOREIGN KEY (order_status)
        REFERENCES gold.dim_order_status(order_status)
);
GO

-- Index cho Incremental Load lookup
CREATE UNIQUE INDEX UX_fact_orders_bk ON gold.fact_orders(order_id, order_item_id);
GO

-- Indexes cho FK joins
CREATE INDEX IX_fo_customer ON gold.fact_orders(customer_key);
CREATE INDEX IX_fo_seller   ON gold.fact_orders(seller_key);
CREATE INDEX IX_fo_product  ON gold.fact_orders(product_key);
CREATE INDEX IX_fo_date     ON gold.fact_orders(order_date_key);
GO
```

### 3.2. gold.fact_sales

```sql
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales;
GO

CREATE TABLE gold.fact_sales (
    seller_key       INT            NOT NULL,
    category_key     INT            NOT NULL,
    date_key         INT            NOT NULL,  -- YYYYMM01
    total_revenue    DECIMAL(14,2)  NOT NULL DEFAULT 0,
    total_items_sold INT            NOT NULL DEFAULT 0,
    total_orders     INT            NOT NULL DEFAULT 0,
    avg_review_score DECIMAL(3,2)   NULL,
    CONSTRAINT PK_fact_sales PRIMARY KEY (seller_key, category_key, date_key),
    CONSTRAINT FK_fs_seller FOREIGN KEY (seller_key)
        REFERENCES gold.dim_seller(seller_key),
    CONSTRAINT FK_fs_category FOREIGN KEY (category_key)
        REFERENCES gold.dim_product_category(category_key),
    CONSTRAINT FK_fs_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key)
);
GO
```

### 3.3. gold.fact_sales_year

```sql
IF OBJECT_ID('gold.fact_sales_year', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales_year;
GO

CREATE TABLE gold.fact_sales_year (
    seller_key       INT            NOT NULL,
    category_key     INT            NOT NULL,
    year_key         INT            NOT NULL,  -- YYYY0101
    total_revenue    DECIMAL(14,2)  NOT NULL DEFAULT 0,
    total_items_sold INT            NOT NULL DEFAULT 0,
    total_orders     INT            NOT NULL DEFAULT 0,
    avg_review_score DECIMAL(3,2)   NULL,
    CONSTRAINT PK_fact_sales_year PRIMARY KEY (seller_key, category_key, year_key)
);
GO
```

---

## BƯỚC 4: SSIS Package 1 – `Extract_Product_Items.dtsx`

### 4.1. Control Flow Layout

```
┌─────────────────────────────────┐
│  Execute SQL Task               │
│  "Truncate Staging Tables"      │
└──────────┬──────────────────────┘
           │ (Success)
     ┌─────┼──────────────┐
     ▼     ▼              ▼
┌────────┐ ┌────────────┐ ┌──────────┐
│DFT:    │ │DFT:        │ │DFT:      │
│Load    │ │Load        │ │Load      │
│Products│ │Category    │ │OrderItems│
│        │ │Translation │ │          │
└────────┘ └────────────┘ └──────────┘
```

**Execute SQL Task – Truncate:**
```sql
TRUNCATE TABLE staging.stg_products;
TRUNCATE TABLE staging.stg_category_translation;
TRUNCATE TABLE staging.stg_order_items;
```

### 4.2. Data Flow Task – Load Products (có xử lý NULL)

```
Flat File Source (olist_products_dataset.csv)
        │
        ▼
Data Conversion
        │
        ▼
Derived Column (xử lý NULL)
        │
        ▼
OLE DB Destination (staging.stg_products)
```

**Bước 4.2.1 – Flat File Source:**
1. Tạo Flat File Connection Manager cho `olist_products_dataset.csv`
2. Delimiter: comma
3. Check ✅ "Column names in the first data row"
4. Tab Advanced: kiểm tra 9 cột

**Bước 4.2.2 – Data Conversion:**

| Input Column | Output Alias | Data Type | Length/Precision |
|---|---|---|---|
| product_id | cv_product_id | DT_STR | 50 |
| product_category_name | cv_category_name | DT_WSTR | 100 |
| product_name_lenght | cv_name_len | DT_I4 | — |
| product_description_lenght | cv_desc_len | DT_I4 | — |
| product_photos_qty | cv_photos_qty | DT_I4 | — |
| product_weight_g | cv_weight | DT_NUMERIC | Precision 10, Scale 2 |
| product_length_cm | cv_length | DT_NUMERIC | Precision 10, Scale 2 |
| product_height_cm | cv_height | DT_NUMERIC | Precision 10, Scale 2 |
| product_width_cm | cv_width | DT_NUMERIC | Precision 10, Scale 2 |

> **Lưu ý:** Một số cột numeric có giá trị rỗng trong CSV. Data Conversion sẽ fail nếu gặp empty string → cần cấu hình Error Output thành **Redirect Row** hoặc **Ignore Failure** cho các cột numeric.

**Cách xử lý NULL cho Data Conversion:**
1. Trong Flat File Connection Manager → Advanced tab
2. Cho các cột weight, length, height, width: set `DataType = DT_WSTR, Length = 20` (giữ string)
3. Rồi dùng **Derived Column** để convert an toàn:

**Bước 4.2.3 – Derived Column (xử lý NULL):**

| Derived Column Name | Expression |
|---|---|
| `clean_weight` | `ISNULL(cv_weight) ? (DT_NUMERIC,10,2)0 : (DT_NUMERIC,10,2)cv_weight` |
| `clean_length` | `ISNULL(cv_length) ? (DT_NUMERIC,10,2)0 : (DT_NUMERIC,10,2)cv_length` |
| `clean_height` | `ISNULL(cv_height) ? (DT_NUMERIC,10,2)0 : (DT_NUMERIC,10,2)cv_height` |
| `clean_width` | `ISNULL(cv_width) ? (DT_NUMERIC,10,2)0 : (DT_NUMERIC,10,2)cv_width` |
| `clean_name_len` | `ISNULL(cv_name_len) ? (DT_I4)0 : cv_name_len` |
| `clean_desc_len` | `ISNULL(cv_desc_len) ? (DT_I4)0 : cv_desc_len` |
| `clean_photos` | `ISNULL(cv_photos_qty) ? (DT_I4)0 : cv_photos_qty` |

> **Cách thay thế đơn giản hơn:** Nếu gặp khó khăn với Data Conversion, set toàn bộ cột CSV thành string, rồi dùng Derived Column để cast. Hoặc dùng **Script Component** (Source) để đọc CSV linh hoạt hơn.

**Bước 4.2.4 – OLE DB Destination:**
- Table: `staging.stg_products`
- Map: `cv_product_id` → `product_id`, `cv_category_name` → `product_category_name`, `clean_*` → các cột tương ứng

### 4.3. Data Flow Task – Load Category Translation

Đơn giản:
1. **Flat File Source:** `product_category_name_translation.csv`
2. **Data Conversion:** 2 cột → DT_WSTR(100)
3. **OLE DB Destination:** `staging.stg_category_translation`

### 4.4. Data Flow Task – Load Order Items

1. **Flat File Source:** `olist_order_items_dataset.csv`
2. **Data Conversion:**

| Input | Output | Type |
|---|---|---|
| order_id | cv_order_id | DT_STR, 50 |
| order_item_id | cv_item_id | DT_I4 |
| product_id | cv_product_id | DT_STR, 50 |
| seller_id | cv_seller_id | DT_STR, 50 |
| shipping_limit_date | cv_ship_date | DT_DBTIMESTAMP |
| price | cv_price | DT_NUMERIC, 10, 2 |
| freight_value | cv_freight | DT_NUMERIC, 10, 2 |

3. **OLE DB Destination:** `staging.stg_order_items`

### 4.5. Test Package

```sql
SELECT COUNT(*) FROM staging.stg_products;           -- Expected: ~32,951
SELECT COUNT(*) FROM staging.stg_category_translation; -- Expected: ~71
SELECT COUNT(*) FROM staging.stg_order_items;         -- Expected: ~112,650
```

---

## BƯỚC 5: SSIS Package 2 – `Load_Dim_Product.dtsx`

### 5.1. Control Flow Layout

```
┌──────────────────────────────┐
│ Execute SQL Task             │
│ "Populate dim_payment_method"│
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ DFT: Load                    │
│ dim_product_category         │
│ (SCD Type 1)                 │
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ DFT: Load                    │
│ dim_product                  │
│ (SCD Type 1)                 │
└──────────────────────────────┘
```

### 5.2. Execute SQL Task – Populate dim_payment_method

```sql
IF NOT EXISTS (SELECT 1 FROM gold.dim_payment_method)
BEGIN
    INSERT INTO gold.dim_payment_method (payment_type, description) VALUES
        ('credit_card', N'Credit Card payment'),
        ('boleto',      N'Boleto Bancário (Brazilian payment slip)'),
        ('voucher',     N'Voucher / Gift card'),
        ('debit_card',  N'Debit Card payment'),
        ('not_defined', N'Payment method not specified');
END
```

### 5.3. Data Flow Task – Load dim_product_category (SCD Type 1)

```
OLE DB Source (stg_category_translation)
        │
        ▼
Lookup dim_product_category (match category_name_portuguese)
   ┌────┴────┐
   │         │
Match    No Match ──→ OLE DB Destination (INSERT)
   │
   ▼
Conditional Split (tên English thay đổi?)
   ┌────┴────┐
   │         │
Changed   Unchanged
   │
   ▼
OLE DB Command (UPDATE)
```

**Bước 5.3.1 – OLE DB Source:**
```sql
SELECT
    product_category_name         AS category_name_portuguese,
    product_category_name_english AS category_name_english
FROM staging.stg_category_translation;
```

**Bước 5.3.2 – Lookup:**
1. General: **Redirect rows to no match output**
2. Connection → Table: `gold.dim_product_category`
3. Columns:
   - Join: `category_name_portuguese` → `category_name_portuguese`
   - Output: `category_key` (as `existing_cat_key`), `category_name_english` (as `existing_eng_name`)

**Bước 5.3.3 – No Match → OLE DB Destination:**
- Table: `gold.dim_product_category`
- Map: `category_name_portuguese`, `category_name_english`

**Bước 5.3.4 – Match → Conditional Split:**
- Condition `Is_Changed`:
```
category_name_english != existing_eng_name
```

**Bước 5.3.5 – Changed → OLE DB Command:**
```sql
UPDATE gold.dim_product_category
SET category_name_english = ?
WHERE category_key = ?;
```
- Param_0 ← `category_name_english`
- Param_1 ← `existing_cat_key`

### 5.4. Data Flow Task – Load dim_product (SCD Type 1)

```
OLE DB Source (stg_products JOIN stg_category_translation)
        │
        ▼
Lookup dim_product_category (lấy category_key)
        │
        ▼
Lookup dim_product (match product_id)
   ┌────┴────┐
   │         │
Match    No Match ──→ OLE DB Destination (INSERT)
   │
   ▼
Conditional Split (weight/dimensions thay đổi?)
   ┌────┴────┐
   │         │
Changed   Unchanged
   │
   ▼
OLE DB Command (UPDATE)
```

**Bước 5.4.1 – OLE DB Source:**
```sql
SELECT
    p.product_id,
    p.product_category_name,
    p.product_name_lenght         AS product_name_length,
    p.product_description_lenght  AS product_description_length,
    p.product_photos_qty,
    ISNULL(p.product_weight_g, 0)  AS product_weight_g,
    ISNULL(p.product_length_cm, 0) AS product_length_cm,
    ISNULL(p.product_height_cm, 0) AS product_height_cm,
    ISNULL(p.product_width_cm, 0)  AS product_width_cm
FROM staging.stg_products p;
```

**Bước 5.4.2 – Lookup dim_product_category:**
1. General: **Redirect rows to no match output** (sản phẩm có thể có category NULL)
2. SQL Command:
```sql
SELECT category_key, category_name_portuguese
FROM gold.dim_product_category;
```
3. Join: `product_category_name` → `category_name_portuguese`
4. Output: `category_key` (as `lkp_category_key`)

> **Xử lý No Match:** Sản phẩm không có category → dùng **Union All** để merge, set `lkp_category_key = NULL`.

**Bước 5.4.3 – Lookup dim_product:**
1. General: **Redirect rows to no match output**
2. Table: `gold.dim_product`
3. Join: `product_id` → `product_id`
4. Output: `product_key` (as `existing_prod_key`), `product_weight_g` (as `existing_weight`), `product_length_cm` (as `existing_length`)

**Bước 5.4.4 – No Match → OLE DB Destination (INSERT):**
- Table: `gold.dim_product`
- Map tất cả cột

**Bước 5.4.5 – Match → Conditional Split:**
```
product_weight_g != existing_weight || product_length_cm != existing_length
```

**Bước 5.4.6 – Changed → OLE DB Command (UPDATE):**
```sql
UPDATE gold.dim_product
SET category_key = ?,
    product_name_length = ?,
    product_description_length = ?,
    product_photos_qty = ?,
    product_weight_g = ?,
    product_length_cm = ?,
    product_height_cm = ?,
    product_width_cm = ?
WHERE product_key = ?;
```

---

## BƯỚC 6: SSIS Package 3 – `Load_Fact_Orders.dtsx` ⭐ (Package quan trọng nhất)

### 6.1. Điều kiện tiên quyết

**TẤT CẢ dimensions phải load xong trước:**
- dim_date (TV1)
- dim_geolocation (TV1)
- dim_customer (TV1)
- dim_order_status (TV1)
- dim_product_category (TV2 – bạn)
- dim_product (TV2 – bạn)
- dim_payment_method (TV2 – bạn)
- dim_seller (TV3)

### 6.2. Control Flow

```
┌──────────────────────────────────────┐
│ DFT: Load fact_orders                │
│ (Incremental Load)                   │
└──────────────────────────────────────┘
```

### 6.3. Data Flow – Incremental Load fact_orders

**Đây là Data Flow phức tạp nhất trong toàn bộ project:**

```
OLE DB Source (JOIN staging tables – chỉ lấy records mới)
        │
        ▼
Lookup dim_customer (lấy customer_key)
        │
        ▼
Lookup dim_seller (lấy seller_key)
        │
        ▼
Lookup dim_product (lấy product_key)
        │
        ▼
Derived Column (tính date_keys từ timestamps)
        │
        ▼
Lookup dim_date ×4 (order/approved/delivered/estimated date_key)
        │
        ▼
OLE DB Destination (gold.fact_orders)
```

**Bước 6.3.1 – OLE DB Source (Incremental – chỉ lấy records chưa load):**

```sql
SELECT
    oi.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.seller_id,
    oi.product_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    oi.price,
    oi.freight_value,
    1 AS quantity,
    r.review_score
FROM staging.stg_order_items oi
INNER JOIN staging.stg_orders o
    ON oi.order_id = o.order_id
LEFT JOIN staging.stg_order_reviews r
    ON o.order_id = r.order_id
-- INCREMENTAL: chỉ lấy records chưa có trong fact
WHERE NOT EXISTS (
    SELECT 1 FROM gold.fact_orders f
    WHERE f.order_id = oi.order_id
      AND f.order_item_id = oi.order_item_id
);
```

> **Lưu ý:** `stg_orders` là staging table của TV3. Cần TV3 chạy Extract trước. Trong lần chạy đầu tiên (bảng trống), `NOT EXISTS` sẽ lấy tất cả records.

> **Cách thay thế dùng Lookup trong SSIS:**
> Nếu không muốn dùng `NOT EXISTS` trong SQL, bạn có thể:
> 1. OLE DB Source lấy toàn bộ (không có WHERE)
> 2. Thêm **Lookup** đến `gold.fact_orders` match trên `order_id + order_item_id`
> 3. Redirect **No Match Output** (records mới) → tiếp tục pipeline
> 4. Match Output → bỏ qua (đã tồn tại)

**Bước 6.3.2 – Lookup dim_customer:**
1. SQL Command:
```sql
SELECT customer_key, customer_id
FROM gold.dim_customer
WHERE is_current = 1;
```
2. Join: `customer_id` → `customer_id`
3. Output: `customer_key` (as `lkp_customer_key`)
4. General: **Redirect rows to no match** (set lkp = NULL nếu không tìm thấy)

**Bước 6.3.3 – Lookup dim_seller:**
1. SQL Command:
```sql
SELECT seller_key, seller_id
FROM gold.dim_seller
WHERE is_current = 1;
```
2. Join: `seller_id` → `seller_id`
3. Output: `seller_key` (as `lkp_seller_key`)

**Bước 6.3.4 – Lookup dim_product:**
1. Table: `gold.dim_product`
2. Join: `product_id` → `product_id`
3. Output: `product_key` (as `lkp_product_key`)

**Bước 6.3.5 – Derived Column (tính date_keys):**

| Name | Expression |
|---|---|
| `order_date_key` | `ISNULL(order_purchase_timestamp) ? (DT_I4)19000101 : (DT_I4)(YEAR(order_purchase_timestamp) * 10000 + MONTH(order_purchase_timestamp) * 100 + DAY(order_purchase_timestamp))` |
| `approved_date_key` | `ISNULL(order_approved_at) ? NULL(DT_I4) : (DT_I4)(YEAR(order_approved_at) * 10000 + MONTH(order_approved_at) * 100 + DAY(order_approved_at))` |
| `delivered_date_key` | `ISNULL(order_delivered_customer_date) ? NULL(DT_I4) : (DT_I4)(YEAR(order_delivered_customer_date) * 10000 + MONTH(order_delivered_customer_date) * 100 + DAY(order_delivered_customer_date))` |
| `estimated_date_key` | `ISNULL(order_estimated_delivery_date) ? NULL(DT_I4) : (DT_I4)(YEAR(order_estimated_delivery_date) * 10000 + MONTH(order_estimated_delivery_date) * 100 + DAY(order_estimated_delivery_date))` |

> **Lưu ý SSIS Expression:** `NULL(DT_I4)` dùng để tạo giá trị NULL kiểu integer. Nếu date NULL thì date_key cũng NULL.

**Bước 6.3.6 – OLE DB Destination:**
- Table: `gold.fact_orders`
- Mapping:

| Source | Destination |
|---|---|
| order_id | order_id |
| order_item_id | order_item_id |
| lkp_customer_key | customer_key |
| lkp_seller_key | seller_key |
| lkp_product_key | product_key |
| order_date_key | order_date_key |
| approved_date_key | approved_date_key |
| delivered_date_key | delivered_date_key |
| estimated_date_key | estimated_delivery_date_key |
| order_status | order_status |
| price | price |
| freight_value | freight_value |
| quantity | quantity |
| review_score | review_score |

### 6.4. Test Incremental Load

```sql
-- Lần chạy 1: toàn bộ records
SELECT COUNT(*) FROM gold.fact_orders;  -- Expected: ~112,650

-- Lần chạy 2: không có gì mới → 0 rows inserted
-- (chạy lại package, kiểm tra Data Flow hiển thị 0 rows)
```

---

## BƯỚC 7: SSIS Package 4 – `Load_Fact_Sales.dtsx`

### 7.1. Control Flow

```
┌──────────────────────────────┐
│ Execute SQL Task             │
│ "Truncate fact_sales"        │
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ DFT: Aggregate & Load        │
│ fact_sales (monthly)         │
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ Execute SQL Task             │
│ "Truncate fact_sales_year"   │
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ DFT: Aggregate & Load        │
│ fact_sales_year              │
└──────────────────────────────┘
```

### 7.2. Data Flow – fact_sales (monthly)

**OLE DB Source:**
```sql
SELECT
    fo.seller_key,
    dp.category_key,
    -- date_key = first day of month
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01') AS date_key,
    SUM(fo.price + fo.freight_value)              AS total_revenue,
    COUNT(*)                                       AS total_items_sold,
    COUNT(DISTINCT fo.order_id)                    AS total_orders,
    AVG(CAST(fo.review_score AS DECIMAL(3,2)))     AS avg_review_score
FROM gold.fact_orders fo
INNER JOIN gold.dim_product dp ON fo.product_key = dp.product_key
INNER JOIN gold.dim_date dd    ON fo.order_date_key = dd.date_key
WHERE fo.seller_key IS NOT NULL
  AND dp.category_key IS NOT NULL
GROUP BY
    fo.seller_key,
    dp.category_key,
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01');
```

**OLE DB Destination:** `gold.fact_sales`

### 7.3. Data Flow – fact_sales_year

```sql
SELECT
    fo.seller_key,
    dp.category_key,
    dd.year * 10000 + 101 AS year_key,
    SUM(fo.price + fo.freight_value)              AS total_revenue,
    COUNT(*)                                       AS total_items_sold,
    COUNT(DISTINCT fo.order_id)                    AS total_orders,
    AVG(CAST(fo.review_score AS DECIMAL(3,2)))     AS avg_review_score
FROM gold.fact_orders fo
INNER JOIN gold.dim_product dp ON fo.product_key = dp.product_key
INNER JOIN gold.dim_date dd    ON fo.order_date_key = dd.date_key
WHERE fo.seller_key IS NOT NULL
  AND dp.category_key IS NOT NULL
GROUP BY
    fo.seller_key,
    dp.category_key,
    dd.year * 10000 + 101;
```

---

## BƯỚC 8: SQL Truy vấn phân tích (2–3 câu insight)

### Query 1: Top 10 danh mục sản phẩm doanh thu cao nhất

```sql
SELECT TOP 10
    pc.category_name_english,
    SUM(fs.total_revenue) AS revenue,
    SUM(fs.total_items_sold) AS items_sold,
    SUM(fs.total_orders) AS orders,
    AVG(fs.avg_review_score) AS avg_review
FROM gold.fact_sales fs
INNER JOIN gold.dim_product_category pc ON fs.category_key = pc.category_key
GROUP BY pc.category_name_english
ORDER BY revenue DESC;
```

### Query 2: Xu hướng doanh thu theo tháng (time series)

```sql
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fs.total_revenue) AS monthly_revenue,
    SUM(fs.total_orders) AS monthly_orders,
    SUM(fs.total_revenue) / NULLIF(SUM(fs.total_orders), 0) AS avg_order_value
FROM gold.fact_sales fs
INNER JOIN gold.dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;
```

### Query 3: Top 10 sản phẩm nặng nhất vs nhẹ nhất – so sánh doanh thu

```sql
WITH ProductRevenue AS (
    SELECT
        dp.product_id,
        pc.category_name_english,
        dp.product_weight_g,
        SUM(fo.price) AS total_revenue,
        COUNT(*) AS times_sold
    FROM gold.fact_orders fo
    INNER JOIN gold.dim_product dp ON fo.product_key = dp.product_key
    LEFT JOIN gold.dim_product_category pc ON dp.category_key = pc.category_key
    GROUP BY dp.product_id, pc.category_name_english, dp.product_weight_g
)
SELECT TOP 10 *, 'Heaviest' AS segment
FROM ProductRevenue ORDER BY product_weight_g DESC
UNION ALL
SELECT TOP 10 *, 'Lightest'
FROM ProductRevenue WHERE product_weight_g > 0 ORDER BY product_weight_g ASC;
```

---

## BƯỚC 9: Verify & Test

### 9.1. Row counts

```sql
SELECT 'dim_product_category' AS tbl, COUNT(*) AS cnt FROM gold.dim_product_category
UNION ALL SELECT 'dim_product',          COUNT(*) FROM gold.dim_product
UNION ALL SELECT 'dim_payment_method',   COUNT(*) FROM gold.dim_payment_method
UNION ALL SELECT 'fact_orders',          COUNT(*) FROM gold.fact_orders
UNION ALL SELECT 'fact_sales',           COUNT(*) FROM gold.fact_sales
UNION ALL SELECT 'fact_sales_year',      COUNT(*) FROM gold.fact_sales_year;
```

### 9.2. Kiểm tra Incremental Load

```sql
-- Chạy package Load_Fact_Orders.dtsx lần 2
-- Verify: 0 dòng mới được insert
SELECT COUNT(*) AS before_count FROM gold.fact_orders;
-- (chạy package)
SELECT COUNT(*) AS after_count FROM gold.fact_orders;
-- before_count == after_count → Incremental Load hoạt động đúng
```

### 9.3. Kiểm tra data integrity

```sql
-- fact_orders phải có tổng revenue khớp với staging
SELECT
    SUM(price + freight_value) AS fact_total
FROM gold.fact_orders;

SELECT
    SUM(CAST(price AS DECIMAL(14,2)) + CAST(freight_value AS DECIMAL(14,2))) AS staging_total
FROM staging.stg_order_items;
-- Hai giá trị phải bằng nhau
```

---

## Checklist hoàn thành TV2

- [ ] 3 staging tables DDL chạy thành công
- [ ] 3 dimension tables DDL chạy thành công
- [ ] 3 fact tables DDL chạy thành công
- [ ] dim_payment_method populated (5 rows)
- [ ] Package `Extract_Product_Items.dtsx` chạy xanh
- [ ] Package `Load_Dim_Product.dtsx` chạy xanh (SCD Type 1 hoạt động)
- [ ] Package `Load_Fact_Orders.dtsx` chạy xanh (Incremental Load hoạt động)
- [ ] Package `Load_Fact_Sales.dtsx` chạy xanh
- [ ] Incremental Load verified (chạy lần 2 = 0 new rows)
- [ ] SCD Type 1 verified (update weight/dimensions khi thay đổi)
- [ ] 3 SQL queries chạy đúng
- [ ] Revenue totals khớp giữa staging và fact
- [ ] Không có orphan foreign keys

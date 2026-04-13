# 📘 GUIDE_TV3.md – Hướng dẫn chi tiết Thành viên 3

## Domain: Người bán & Vận chuyển & Thanh toán

**Phụ trách:**
- Staging: `stg_sellers`, `stg_orders`, `stg_order_payments`
- Dimensions: `dim_seller` (SCD Type 2)
- Facts: `fact_order_lifecycle` (Upsert), `fact_delivery` (m+y), `fact_payment_trends` (m+y)
- SSIS Packages: 4 packages
- Master Package: chịu trách nhiệm chính tích hợp toàn bộ

---

## BƯỚC 0: Chuẩn bị

### 0.1. Xác nhận Database đã tạo (TV1 thực hiện)

```sql
USE OlistDW;
GO
SELECT name FROM sys.schemas WHERE name IN ('staging', 'gold');
```

### 0.2. Tạo SSIS Packages

Trong Solution Explorer, thêm 4 packages:
- `Extract_Seller_Order_Payment.dtsx`
- `Load_Dim_Seller.dtsx`
- `Load_Fact_Lifecycle.dtsx`
- `Load_Fact_Delivery_Payment.dtsx`

Và package tổng:
- `Master_ETL.dtsx` (tích hợp tất cả packages của TV1, TV2, TV3)

---

## BƯỚC 1: Tạo Staging Tables (DDL)

### 1.1. staging.stg_sellers

```sql
IF OBJECT_ID('staging.stg_sellers', 'U') IS NOT NULL
    DROP TABLE staging.stg_sellers;
GO

CREATE TABLE staging.stg_sellers (
    seller_id               VARCHAR(50)   NOT NULL,
    seller_zip_code_prefix  VARCHAR(10)   NULL,
    seller_city             NVARCHAR(100) NULL,
    seller_state            VARCHAR(5)    NULL
);
GO
```

### 1.2. staging.stg_orders

```sql
IF OBJECT_ID('staging.stg_orders', 'U') IS NOT NULL
    DROP TABLE staging.stg_orders;
GO

CREATE TABLE staging.stg_orders (
    order_id                       VARCHAR(50)  NOT NULL,
    customer_id                    VARCHAR(50)  NOT NULL,
    order_status                   VARCHAR(30)  NULL,
    order_purchase_timestamp       DATETIME     NULL,
    order_approved_at              DATETIME     NULL,
    order_delivered_carrier_date   DATETIME     NULL,
    order_delivered_customer_date  DATETIME     NULL,
    order_estimated_delivery_date  DATETIME     NULL
);
GO
```

### 1.3. staging.stg_order_payments

```sql
IF OBJECT_ID('staging.stg_order_payments', 'U') IS NOT NULL
    DROP TABLE staging.stg_order_payments;
GO

CREATE TABLE staging.stg_order_payments (
    order_id              VARCHAR(50)   NOT NULL,
    payment_sequential    INT           NOT NULL,
    payment_type          VARCHAR(30)   NOT NULL,
    payment_installments  INT           NULL,
    payment_value         DECIMAL(10,2) NOT NULL
);
GO
```

---

## BƯỚC 2: Tạo Dimension Tables (DDL)

### 2.1. gold.dim_seller (SCD Type 2)

```sql
IF OBJECT_ID('gold.dim_seller', 'U') IS NOT NULL
    DROP TABLE gold.dim_seller;
GO

CREATE TABLE gold.dim_seller (
    seller_key       INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    seller_id        VARCHAR(50)       NOT NULL,
    city             NVARCHAR(100)     NULL,
    state            VARCHAR(5)        NULL,
    geo_key          INT               NULL,
    seller_region    VARCHAR(20)       NULL,
    -- SCD Type 2 columns
    effective_from   DATE              NOT NULL DEFAULT '1900-01-01',
    effective_to     DATE              NOT NULL DEFAULT '9999-12-31',
    is_current       BIT               NOT NULL DEFAULT 1,
    CONSTRAINT FK_dim_seller_geo FOREIGN KEY (geo_key)
        REFERENCES gold.dim_geolocation(geo_key)
);
GO

CREATE INDEX IX_dim_seller_id ON gold.dim_seller(seller_id, is_current);
GO
```

---

## BƯỚC 3: Tạo Fact Tables (DDL)

### 3.1. gold.fact_order_lifecycle (Accumulating Snapshot)

```sql
IF OBJECT_ID('gold.fact_order_lifecycle', 'U') IS NOT NULL
    DROP TABLE gold.fact_order_lifecycle;
GO

CREATE TABLE gold.fact_order_lifecycle (
    fact_lifecycle_id   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    order_id            VARCHAR(50)        NOT NULL,
    customer_key        INT                NULL,
    seller_key          INT                NULL,
    order_date          DATE               NULL,
    approved_date       DATE               NULL,
    delivered_date      DATE               NULL,
    estimated_delivery_date DATE           NULL,
    days_to_approve     INT                NULL,
    days_to_delivery    INT                NULL,
    is_delayed          BIT                NULL,
    order_status        VARCHAR(30)        NULL,
    CONSTRAINT FK_fl_customer FOREIGN KEY (customer_key)
        REFERENCES gold.dim_customer(customer_key),
    CONSTRAINT FK_fl_seller FOREIGN KEY (seller_key)
        REFERENCES gold.dim_seller(seller_key)
);
GO

CREATE UNIQUE INDEX UX_fact_lifecycle_order ON gold.fact_order_lifecycle(order_id);
GO
```

### 3.2. gold.fact_delivery

```sql
IF OBJECT_ID('gold.fact_delivery', 'U') IS NOT NULL
    DROP TABLE gold.fact_delivery;
GO

CREATE TABLE gold.fact_delivery (
    seller_key             INT           NOT NULL,
    date_key               INT           NOT NULL,  -- YYYYMM01
    total_orders_delivered  INT           NOT NULL DEFAULT 0,
    on_time_orders         INT           NOT NULL DEFAULT 0,
    on_time_rate           DECIMAL(5,4)  NULL,
    CONSTRAINT PK_fact_delivery PRIMARY KEY (seller_key, date_key),
    CONSTRAINT FK_fd_seller FOREIGN KEY (seller_key)
        REFERENCES gold.dim_seller(seller_key),
    CONSTRAINT FK_fd_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key)
);
GO
```

### 3.3. gold.fact_delivery_year

```sql
IF OBJECT_ID('gold.fact_delivery_year', 'U') IS NOT NULL
    DROP TABLE gold.fact_delivery_year;
GO

CREATE TABLE gold.fact_delivery_year (
    seller_key             INT           NOT NULL,
    year_key               INT           NOT NULL,  -- YYYY0101
    total_orders_delivered  INT           NOT NULL DEFAULT 0,
    on_time_orders         INT           NOT NULL DEFAULT 0,
    on_time_rate           DECIMAL(5,4)  NULL,
    CONSTRAINT PK_fact_delivery_year PRIMARY KEY (seller_key, year_key)
);
GO
```

### 3.4. gold.fact_payment_trends

```sql
IF OBJECT_ID('gold.fact_payment_trends', 'U') IS NOT NULL
    DROP TABLE gold.fact_payment_trends;
GO

CREATE TABLE gold.fact_payment_trends (
    payment_type      VARCHAR(30)   NOT NULL,
    date_key          INT           NOT NULL,  -- YYYYMM01
    total_payment_value DECIMAL(14,2) NOT NULL DEFAULT 0,
    transaction_count INT           NOT NULL DEFAULT 0,
    order_count       INT           NOT NULL DEFAULT 0,
    CONSTRAINT PK_fact_payment_trends PRIMARY KEY (payment_type, date_key),
    CONSTRAINT FK_fpt_payment FOREIGN KEY (payment_type)
        REFERENCES gold.dim_payment_method(payment_type),
    CONSTRAINT FK_fpt_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key)
);
GO
```

### 3.5. gold.fact_payment_trends_year

```sql
IF OBJECT_ID('gold.fact_payment_trends_year', 'U') IS NOT NULL
    DROP TABLE gold.fact_payment_trends_year;
GO

CREATE TABLE gold.fact_payment_trends_year (
    payment_type        VARCHAR(30)    NOT NULL,
    year_key            INT            NOT NULL,  -- YYYY0101
    total_payment_value DECIMAL(14,2)  NOT NULL DEFAULT 0,
    transaction_count   INT            NOT NULL DEFAULT 0,
    order_count         INT            NOT NULL DEFAULT 0,
    CONSTRAINT PK_fact_payment_trends_year PRIMARY KEY (payment_type, year_key)
);
GO
```

---

## BƯỚC 4: SSIS Package 1 – `Extract_Seller_Order_Payment.dtsx`

### 4.1. Control Flow Layout

```
┌─────────────────────────────────┐
│  Execute SQL Task               │
│  "Truncate Staging Tables"      │
└──────────┬──────────────────────┘
           │ (Success)
     ┌─────┼──────────────┐
     ▼     ▼              ▼
┌────────┐ ┌────────────┐ ┌────────────┐
│DFT:    │ │DFT:        │ │DFT:        │
│Load    │ │Load        │ │Load        │
│Sellers │ │Orders      │ │Payments    │
└────────┘ └────────────┘ └────────────┘
```

**Execute SQL Task – Truncate:**
```sql
TRUNCATE TABLE staging.stg_sellers;
TRUNCATE TABLE staging.stg_orders;
TRUNCATE TABLE staging.stg_order_payments;
```

### 4.2. Data Flow Task – Load Sellers

```
Flat File Source (olist_sellers_dataset.csv)
        │
        ▼
Data Conversion
        │
        ▼
OLE DB Destination (staging.stg_sellers)
```

**Data Conversion:**

| Input Column | Output Alias | Data Type | Length |
|---|---|---|---|
| seller_id | cv_seller_id | DT_STR | 50 |
| seller_zip_code_prefix | cv_zip | DT_STR | 10 |
| seller_city | cv_city | DT_WSTR | 100 |
| seller_state | cv_state | DT_STR | 5 |

### 4.3. Data Flow Task – Load Orders (xử lý timestamps)

```
Flat File Source (olist_orders_dataset.csv)
        │
        ▼
Data Conversion
        │
        ▼
Derived Column (xử lý NULL timestamps)
        │
        ▼
OLE DB Destination (staging.stg_orders)
```

**Bước 4.3.1 – Flat File Source:**
1. Tạo Connection Manager cho `olist_orders_dataset.csv`
2. Delimiter: comma
3. Tab Advanced: **ĐẶT TẤT CẢ timestamp columns thành DT_WSTR, length 30**
   - Lý do: một số timestamp có giá trị rỗng, nếu set DT_DBTIMESTAMP sẽ fail

Các cột cần set string: `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`

**Bước 4.3.2 – Data Conversion:**

| Input Column | Output Alias | Data Type |
|---|---|---|
| order_id | cv_order_id | DT_STR, 50 |
| customer_id | cv_customer_id | DT_STR, 50 |
| order_status | cv_status | DT_STR, 30 |

> Timestamps giữ nguyên string, xử lý ở Derived Column.

**Bước 4.3.3 – Derived Column (convert timestamps an toàn):**

| Name | Expression |
|---|---|
| `dt_purchase` | `LEN(TRIM(order_purchase_timestamp)) > 0 ? (DT_DBTIMESTAMP)order_purchase_timestamp : NULL(DT_DBTIMESTAMP)` |
| `dt_approved` | `LEN(TRIM(order_approved_at)) > 0 ? (DT_DBTIMESTAMP)order_approved_at : NULL(DT_DBTIMESTAMP)` |
| `dt_carrier` | `LEN(TRIM(order_delivered_carrier_date)) > 0 ? (DT_DBTIMESTAMP)order_delivered_carrier_date : NULL(DT_DBTIMESTAMP)` |
| `dt_customer` | `LEN(TRIM(order_delivered_customer_date)) > 0 ? (DT_DBTIMESTAMP)order_delivered_customer_date : NULL(DT_DBTIMESTAMP)` |
| `dt_estimated` | `LEN(TRIM(order_estimated_delivery_date)) > 0 ? (DT_DBTIMESTAMP)order_estimated_delivery_date : NULL(DT_DBTIMESTAMP)` |

**Bước 4.3.4 – OLE DB Destination:**
- Table: `staging.stg_orders`
- Map: `cv_order_id` → `order_id`, `cv_customer_id` → `customer_id`, `cv_status` → `order_status`, `dt_purchase` → `order_purchase_timestamp`, `dt_approved` → `order_approved_at`, v.v.

### 4.4. Data Flow Task – Load Payments

```
Flat File Source (olist_order_payments_dataset.csv)
        │
        ▼
Data Conversion
        │
        ▼
OLE DB Destination (staging.stg_order_payments)
```

**Data Conversion:**

| Input | Output | Type |
|---|---|---|
| order_id | cv_order_id | DT_STR, 50 |
| payment_sequential | cv_seq | DT_I4 |
| payment_type | cv_type | DT_STR, 30 |
| payment_installments | cv_install | DT_I4 |
| payment_value | cv_value | DT_NUMERIC, 10, 2 |

### 4.5. Test Package

```sql
SELECT COUNT(*) FROM staging.stg_sellers;         -- Expected: ~3,095
SELECT COUNT(*) FROM staging.stg_orders;           -- Expected: ~99,441
SELECT COUNT(*) FROM staging.stg_order_payments;   -- Expected: ~103,886
```

---

## BƯỚC 5: SSIS Package 2 – `Load_Dim_Seller.dtsx`

### 5.1. Điều kiện tiên quyết

`dim_geolocation` (TV1) **phải load xong trước** vì dim_seller cần FK `geo_key`.

### 5.2. Control Flow

```
┌──────────────────────────────┐
│ DFT: Load dim_seller         │
│ (SCD Type 2)                 │
└──────────────────────────────┘
```

### 5.3. Data Flow – SCD Type 2 cho dim_seller

**Đây là SCD Type 2 phức tạp, tương tự dim_customer của TV1:**

```
OLE DB Source (stg_sellers)
        │
        ▼
Lookup dim_geolocation (lấy geo_key từ zip_code_prefix)
        │
        ▼
Derived Column (thêm seller_region)
        │
        ▼
Lookup dim_seller (match seller_id WHERE is_current=1)
   ┌────┴────┐
   │         │
Match    No Match
   │         │
   ▼         ▼
Cond.    Derived Column (thêm SCD columns)
Split         │
   │          ▼
   │     OLE DB Destination (INSERT mới)
   │
┌──┴──┐
│     │
Changed Unchanged (bỏ qua)
│
▼
Multicast
┌───┴───┐
▼       ▼
OLE DB  Derived Column (SCD cols)
Command      │
(Expire)     ▼
        OLE DB Destination (INSERT new version)
```

**Bước 5.3.1 – OLE DB Source:**
```sql
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM staging.stg_sellers;
```

**Bước 5.3.2 – Lookup dim_geolocation:**
1. General: **Redirect rows to no match output**
2. Table: `gold.dim_geolocation`
3. Join: `seller_zip_code_prefix` → `zip_code_prefix`
4. Output: `geo_key` (as `lkp_geo_key`)

> No Match → dùng **Union All** merge, set `lkp_geo_key = NULL`

**Bước 5.3.3 – Derived Column (seller_region):**

```
(DT_STR, 20, 1252)(
    seller_state == "SP" || seller_state == "RJ" ||
    seller_state == "MG" || seller_state == "ES"
        ? "Sudeste"
    : seller_state == "PR" || seller_state == "SC" || seller_state == "RS"
        ? "Sul"
    : seller_state == "BA" || seller_state == "PE" ||
      seller_state == "CE" || seller_state == "MA" ||
      seller_state == "PB" || seller_state == "RN" ||
      seller_state == "AL" || seller_state == "PI" || seller_state == "SE"
        ? "Nordeste"
    : seller_state == "AM" || seller_state == "PA" ||
      seller_state == "RO" || seller_state == "TO" ||
      seller_state == "AC" || seller_state == "AP" || seller_state == "RR"
        ? "Norte"
    : seller_state == "GO" || seller_state == "MT" ||
      seller_state == "MS" || seller_state == "DF"
        ? "Centro-Oeste"
    : "Unknown"
)
```

**Bước 5.3.4 – Lookup dim_seller (kiểm tra tồn tại):**
1. General: **Redirect rows to no match output**
2. SQL Command:
```sql
SELECT seller_key, seller_id, city, state, geo_key
FROM gold.dim_seller
WHERE is_current = 1;
```
3. Join: `seller_id` → `seller_id`
4. Output: `seller_key` (as `existing_seller_key`), `city` (as `existing_city`), `state` (as `existing_state`)

**Bước 5.3.5 – No Match → Derived Column → INSERT:**

Thêm Derived Column trước OLE DB Destination:

| Name | Expression |
|---|---|
| `scd_effective_from` | `(DT_DBDATE)GETDATE()` |
| `scd_effective_to` | `(DT_DBDATE)"9999-12-31"` |
| `scd_is_current` | `(DT_BOOL)TRUE` |

OLE DB Destination → `gold.dim_seller`:

| Source | Destination |
|---|---|
| seller_id | seller_id |
| seller_city | city |
| seller_state | state |
| lkp_geo_key | geo_key |
| seller_region | seller_region |
| scd_effective_from | effective_from |
| scd_effective_to | effective_to |
| scd_is_current | is_current |

**Bước 5.3.6 – Match → Conditional Split:**
- Condition `Is_Changed`:
```
seller_city != existing_city || seller_state != existing_state
```
- Default: `Unchanged`

**Bước 5.3.7 – Changed → Multicast:**
1. Kéo **Multicast** transformation từ `Is_Changed` output
2. Output 1 → **OLE DB Command** (expire bản ghi cũ)
3. Output 2 → **Derived Column** → **OLE DB Destination** (insert version mới)

**OLE DB Command (Expire):**
```sql
UPDATE gold.dim_seller
SET is_current = 0, effective_to = CAST(GETDATE() AS DATE)
WHERE seller_key = ?;
```
- Param_0 ← `existing_seller_key`

**Derived Column (cho insert new version):**
- `new_effective_from` = `(DT_DBDATE)GETDATE()`
- `new_effective_to` = `(DT_DBDATE)"9999-12-31"`
- `new_is_current` = `(DT_BOOL)TRUE`

**OLE DB Destination (Insert new version):**
- Map tương tự bước 5.3.5, dùng dữ liệu mới từ source

### 5.4. Test SCD Type 2

```sql
-- Kiểm tra tổng sellers
SELECT COUNT(*) FROM gold.dim_seller;                    -- Expected: ~3,095
SELECT COUNT(*) FROM gold.dim_seller WHERE is_current=1; -- Expected: ~3,095

-- Kiểm tra: nếu có seller thay đổi, sẽ có nhiều version
SELECT seller_id, COUNT(*) AS versions
FROM gold.dim_seller
GROUP BY seller_id
HAVING COUNT(*) > 1;
```

---

## BƯỚC 6: SSIS Package 3 – `Load_Fact_Lifecycle.dtsx` ⭐ (Upsert – Accumulating Snapshot)

### 6.1. Điều kiện tiên quyết

- `dim_customer` (TV1) đã load
- `dim_seller` (TV3 – bạn) đã load

### 6.2. Control Flow

```
┌──────────────────────────────────────┐
│ DFT: Load fact_order_lifecycle       │
│ (Upsert: Insert new + Update exist)  │
└──────────────────────────────────────┘
```

### 6.3. Data Flow – Upsert Logic

```
OLE DB Source (stg_orders)
        │
        ▼
Lookup dim_customer (lấy customer_key)
        │
        ▼
Lookup dim_seller (lấy seller_key – qua stg_order_items)
        │
        ▼
Derived Column (tính days_to_approve, days_to_delivery, is_delayed)
        │
        ▼
Lookup fact_order_lifecycle (match order_id)
   ┌────┴────┐
   │         │
Match    No Match ──→ OLE DB Destination (INSERT)
   │
   ▼
Conditional Split (timestamps thay đổi?)
   ┌────┴────┐
   │         │
Updated   Unchanged
   │
   ▼
OLE DB Command (UPDATE)
```

**Bước 6.3.1 – OLE DB Source:**

```sql
SELECT
    o.order_id,
    o.customer_id,
    -- Lấy seller_id từ order_item đầu tiên (1 order có thể nhiều sellers)
    (SELECT TOP 1 oi.seller_id
     FROM staging.stg_order_items oi
     WHERE oi.order_id = o.order_id) AS seller_id,
    o.order_status,
    CAST(o.order_purchase_timestamp AS DATE)      AS order_date,
    CAST(o.order_approved_at AS DATE)             AS approved_date,
    CAST(o.order_delivered_customer_date AS DATE)  AS delivered_date,
    CAST(o.order_estimated_delivery_date AS DATE)  AS estimated_delivery_date
FROM staging.stg_orders o;
```

> **Lưu ý:** Mỗi order có thể có nhiều sellers (qua order_items). Ở đây lấy seller đầu tiên. Nếu cần chính xác hơn, có thể lấy seller có nhiều items nhất.

**Bước 6.3.2 – Lookup dim_customer:**
```sql
SELECT customer_key, customer_id
FROM gold.dim_customer
WHERE is_current = 1;
```
Join: `customer_id` → `customer_id`
Output: `customer_key` (as `lkp_customer_key`)

**Bước 6.3.3 – Lookup dim_seller:**
```sql
SELECT seller_key, seller_id
FROM gold.dim_seller
WHERE is_current = 1;
```
Join: `seller_id` → `seller_id`
Output: `seller_key` (as `lkp_seller_key`)

**Bước 6.3.4 – Derived Column (tính measures):**

| Name | Expression |
|---|---|
| `calc_days_to_approve` | `ISNULL(approved_date) \|\| ISNULL(order_date) ? NULL(DT_I4) : (DT_I4)DATEDIFF("dd", order_date, approved_date)` |
| `calc_days_to_delivery` | `ISNULL(delivered_date) \|\| ISNULL(order_date) ? NULL(DT_I4) : (DT_I4)DATEDIFF("dd", order_date, delivered_date)` |
| `calc_is_delayed` | `ISNULL(delivered_date) \|\| ISNULL(estimated_delivery_date) ? NULL(DT_BOOL) : delivered_date > estimated_delivery_date ? (DT_BOOL)TRUE : (DT_BOOL)FALSE` |

> **Lưu ý SSIS:** Hàm `DATEDIFF` trong SSIS Expression dùng syntax: `DATEDIFF("dd", date1, date2)`. Nếu gặp lỗi, thay thế bằng Script Component.

> **Cách thay thế dùng SQL trong OLE DB Source (đơn giản hơn):**
> Tính trực tiếp trong SQL thay vì Derived Column:
```sql
SELECT
    o.order_id,
    o.customer_id,
    (SELECT TOP 1 oi.seller_id FROM staging.stg_order_items oi WHERE oi.order_id = o.order_id) AS seller_id,
    o.order_status,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date,
    CAST(o.order_approved_at AS DATE) AS approved_date,
    CAST(o.order_delivered_customer_date AS DATE) AS delivered_date,
    CAST(o.order_estimated_delivery_date AS DATE) AS estimated_delivery_date,
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_approved_at) AS days_to_approve,
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) AS days_to_delivery,
    CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1
        WHEN o.order_delivered_customer_date IS NULL THEN NULL
        ELSE 0
    END AS is_delayed
FROM staging.stg_orders o;
```

**Bước 6.3.5 – Lookup fact_order_lifecycle (kiểm tra Upsert):**
1. General: **Redirect rows to no match output**
2. Table: `gold.fact_order_lifecycle`
3. Join: `order_id` → `order_id`
4. Output: `fact_lifecycle_id` (as `existing_lifecycle_id`), `order_status` (as `existing_status`), `delivered_date` (as `existing_delivered`)

**Bước 6.3.6 – No Match → OLE DB Destination (INSERT):**
- Table: `gold.fact_order_lifecycle`
- Map tất cả cột

**Bước 6.3.7 – Match → Conditional Split:**
- Condition `Is_Updated`: kiểm tra nếu status hoặc timestamps thay đổi:
```
order_status != existing_status ||
(!ISNULL(delivered_date) && ISNULL(existing_delivered))
```
- Default: `Unchanged`

**Bước 6.3.8 – Updated → OLE DB Command:**
```sql
UPDATE gold.fact_order_lifecycle
SET order_status = ?,
    approved_date = ?,
    delivered_date = ?,
    days_to_approve = ?,
    days_to_delivery = ?,
    is_delayed = ?
WHERE fact_lifecycle_id = ?;
```

Parameter Mappings:

| Param | Source Column |
|---|---|
| Param_0 | order_status |
| Param_1 | approved_date |
| Param_2 | delivered_date |
| Param_3 | calc_days_to_approve (hoặc days_to_approve từ SQL) |
| Param_4 | calc_days_to_delivery |
| Param_5 | calc_is_delayed |
| Param_6 | existing_lifecycle_id |

### 6.4. Test Upsert

```sql
-- Lần 1: insert toàn bộ
SELECT COUNT(*) FROM gold.fact_order_lifecycle;  -- Expected: ~99,441

-- Lần 2: chỉ update nếu có thay đổi, không insert trùng
-- Chạy lại → verify count không tăng
```

---

## BƯỚC 7: SSIS Package 4 – `Load_Fact_Delivery_Payment.dtsx`

### 7.1. Control Flow

```
┌──────────────────────────┐   ┌──────────────────────────────┐
│ Execute SQL Task         │   │ Execute SQL Task             │
│ "Truncate fact_delivery" │   │ "Truncate fact_payment_trends│
└──────────┬───────────────┘   └──────────┬───────────────────┘
           │                              │
┌──────────▼───────────────┐   ┌──────────▼───────────────────┐
│ DFT: Load                │   │ DFT: Load                    │
│ fact_delivery (monthly)  │   │ fact_payment_trends (monthly) │
└──────────┬───────────────┘   └──────────┬───────────────────┘
           │                              │
┌──────────▼───────────────┐   ┌──────────▼───────────────────┐
│ Execute SQL Task         │   │ Execute SQL Task             │
│ "Truncate _year"         │   │ "Truncate _year"             │
└──────────┬───────────────┘   └──────────┬───────────────────┘
           │                              │
┌──────────▼───────────────┐   ┌──────────▼───────────────────┐
│ DFT: Load                │   │ DFT: Load                    │
│ fact_delivery_year       │   │ fact_payment_trends_year     │
└──────────────────────────┘   └──────────────────────────────┘
```

> Hai nhánh (Delivery | Payment) có thể chạy **song song** bằng cách không nối Precedence Constraint giữa chúng.

### 7.2. fact_delivery (monthly)

**Truncate:**
```sql
TRUNCATE TABLE gold.fact_delivery;
```

**OLE DB Source:**
```sql
SELECT
    fo.seller_key,
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01') AS date_key,
    COUNT(DISTINCT fo.order_id) AS total_orders_delivered,
    SUM(CASE WHEN fl.is_delayed = 0 THEN 1 ELSE 0 END) AS on_time_orders,
    CAST(
        SUM(CASE WHEN fl.is_delayed = 0 THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(DISTINCT fo.order_id), 0)
        AS DECIMAL(5,4)
    ) AS on_time_rate
FROM gold.fact_orders fo
INNER JOIN gold.dim_date dd ON fo.order_date_key = dd.date_key
INNER JOIN gold.fact_order_lifecycle fl ON fo.order_id = fl.order_id
WHERE fo.seller_key IS NOT NULL
  AND fl.order_status = 'delivered'
GROUP BY
    fo.seller_key,
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01');
```

**OLE DB Destination:** `gold.fact_delivery`

### 7.3. fact_delivery_year

```sql
SELECT
    fo.seller_key,
    dd.year * 10000 + 101 AS year_key,
    COUNT(DISTINCT fo.order_id) AS total_orders_delivered,
    SUM(CASE WHEN fl.is_delayed = 0 THEN 1 ELSE 0 END) AS on_time_orders,
    CAST(
        SUM(CASE WHEN fl.is_delayed = 0 THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(DISTINCT fo.order_id), 0)
        AS DECIMAL(5,4)
    ) AS on_time_rate
FROM gold.fact_orders fo
INNER JOIN gold.dim_date dd ON fo.order_date_key = dd.date_key
INNER JOIN gold.fact_order_lifecycle fl ON fo.order_id = fl.order_id
WHERE fo.seller_key IS NOT NULL
  AND fl.order_status = 'delivered'
GROUP BY fo.seller_key, dd.year * 10000 + 101;
```

### 7.4. fact_payment_trends (monthly)

**Truncate:**
```sql
TRUNCATE TABLE gold.fact_payment_trends;
```

**OLE DB Source:**
```sql
SELECT
    p.payment_type,
    CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMM') + '01') AS date_key,
    SUM(p.payment_value)       AS total_payment_value,
    COUNT(*)                    AS transaction_count,
    COUNT(DISTINCT p.order_id)  AS order_count
FROM staging.stg_order_payments p
INNER JOIN staging.stg_orders o ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY
    p.payment_type,
    CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMM') + '01');
```

**OLE DB Destination:** `gold.fact_payment_trends`

### 7.5. fact_payment_trends_year

```sql
SELECT
    p.payment_type,
    YEAR(o.order_purchase_timestamp) * 10000 + 101 AS year_key,
    SUM(p.payment_value)       AS total_payment_value,
    COUNT(*)                    AS transaction_count,
    COUNT(DISTINCT p.order_id)  AS order_count
FROM staging.stg_order_payments p
INNER JOIN staging.stg_orders o ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY
    p.payment_type,
    YEAR(o.order_purchase_timestamp) * 10000 + 101;
```

---

## BƯỚC 8: Master Package – `Master_ETL.dtsx`

### 8.1. Tổng quan

TV3 chịu trách nhiệm chính tạo Master Package để gọi tất cả child packages theo thứ tự đúng.

### 8.2. Control Flow Layout

```
┌─────────────────────────────────────────────────────────────┐
│                    Master_ETL.dtsx                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ══════════ STEP 1: Extract to Staging (Song song) ═══════  │
│  ┌───────────────┐  ┌───────────────┐  ┌────────────────┐  │
│  │ Execute Pkg   │  │ Execute Pkg   │  │ Execute Pkg    │  │
│  │ Extract_      │  │ Extract_      │  │ Extract_Seller │  │
│  │ Customer_Geo  │  │ Product_Items │  │ _Order_Payment │  │
│  │ (TV1)         │  │ (TV2)         │  │ (TV3)          │  │
│  └───────┬───────┘  └───────┬───────┘  └───────┬────────┘  │
│          │                  │                   │           │
│          └──────────────────┼───────────────────┘           │
│                             │ (All Success)                 │
│  ═══════ STEP 2: Load Dimensions (Song song*) ════════════  │
│  ┌───────────────┐  ┌───────────────┐                       │
│  │ Execute Pkg   │  │ Execute Pkg   │                       │
│  │ Load_Dim_Date │  │ Load_Dim_     │                       │
│  │ _Geo_Customer │  │ Product       │                       │
│  │ (TV1)         │  │ (TV2)         │                       │
│  └───────┬───────┘  └───────┬───────┘                       │
│          │                  │                               │
│          │ (TV1 Success)    │                               │
│  ┌───────▼───────┐         │                               │
│  │ Execute Pkg   │         │                               │
│  │ Load_Dim_     │         │                               │
│  │ Seller (TV3)  │◄────────┘ (*)dim_seller phụ thuộc       │
│  └───────┬───────┘         dim_geolocation từ TV1           │
│          │                                                  │
│          │ (All Dims Success)                               │
│  ═════════ STEP 3: Load Transaction Facts ══════════════    │
│  ┌───────────────┐  ┌───────────────┐                       │
│  │ Execute Pkg   │  │ Execute Pkg   │                       │
│  │ Load_Fact_    │  │ Load_Fact_    │                       │
│  │ Orders (TV2)  │  │ Lifecycle     │                       │
│  │               │  │ (TV3)         │                       │
│  └───────┬───────┘  └───────┬───────┘                       │
│          │                  │                               │
│          │ (All Success)    │                               │
│  ════════ STEP 4: Aggregated Facts (Song song) ═══════════  │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐│
│  │ Execute Pkg  │ │ Execute Pkg  │ │ Execute Pkg          ││
│  │ Load_Fact_   │ │ Load_Fact_   │ │ Load_Fact_Delivery_  ││
│  │ Customer_    │ │ Sales        │ │ Payment (TV3)        ││
│  │ Orders (TV1) │ │ (TV2)        │ │                      ││
│  └──────────────┘ └──────────────┘ └──────────────────────┘│
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 8.3. Cách tạo Execute Package Task

Cho mỗi child package:
1. Kéo **Execute Package Task** vào Control Flow
2. Double-click → Package tab:
   - ReferenceType: **Project Reference**
   - PackageNameFromProjectReference: chọn tên package (vd: `Extract_Customer_Geo.dtsx`)
3. Đặt tên rõ ràng (vd: `Step1_Extract_Customer_Geo`)

### 8.4. Cấu hình Precedence Constraints

**Song song (không nối):**
- Step 1: 3 Extract packages không nối với nhau → chạy song song

**Tuần tự (nối mũi tên xanh):**
- Step 1 tất cả → Step 2 bắt đầu
- Step 2 Load_Dim_Date_Geo_Customer → Step 2 Load_Dim_Seller (dependency)
- Step 2 tất cả → Step 3
- Step 3 tất cả → Step 4

**Cách tạo Precedence Constraint:**
1. Click vào Execute Package Task nguồn
2. Kéo mũi tên xanh xuống task đích
3. Mặc định: Constraint = `Success`, Evaluation = `Constraint`

**Để nhiều task phải thành công trước khi task tiếp theo chạy:**
1. Nối mũi tên từ TẤT CẢ task nguồn vào task đích
2. Double-click mũi tên → chọn **Logical AND** (tất cả phải success)

### 8.5. Cấu hình Logging (khuyến nghị)

1. Menu SSIS → Logging
2. Chọn Provider: **SSIS log provider for SQL Server**
3. Events: `OnPreExecute`, `OnPostExecute`, `OnError`, `OnWarning`
4. Giúp debug khi pipeline fail

---

## BƯỚC 9: SQL Truy vấn phân tích (2–3 câu insight)

### Query 1: Top 10 sellers giao hàng đúng hạn nhất

```sql
SELECT TOP 10
    ds.seller_id,
    ds.city,
    ds.state,
    ds.seller_region,
    fd.total_orders_delivered,
    fd.on_time_orders,
    fd.on_time_rate
FROM gold.fact_delivery_year fd
INNER JOIN gold.dim_seller ds ON fd.seller_key = ds.seller_key
    AND ds.is_current = 1
WHERE fd.total_orders_delivered >= 10  -- ít nhất 10 đơn
ORDER BY fd.on_time_rate DESC, fd.total_orders_delivered DESC;
```

### Query 2: Xu hướng sử dụng phương thức thanh toán theo thời gian

```sql
SELECT
    dd.year,
    dd.month,
    pm.payment_type,
    pm.description,
    fpt.total_payment_value,
    fpt.transaction_count,
    fpt.order_count,
    CAST(fpt.total_payment_value * 100.0 /
        SUM(fpt.total_payment_value) OVER (PARTITION BY dd.year, dd.month)
        AS DECIMAL(5,2)) AS pct_of_monthly_value
FROM gold.fact_payment_trends fpt
INNER JOIN gold.dim_payment_method pm ON fpt.payment_type = pm.payment_type
INNER JOIN gold.dim_date dd ON fpt.date_key = dd.date_key
ORDER BY dd.year, dd.month, fpt.total_payment_value DESC;
```

### Query 3: Phân tích vòng đời đơn hàng – thời gian xử lý trung bình

```sql
SELECT
    fl.order_status,
    COUNT(*) AS total_orders,
    AVG(fl.days_to_approve)  AS avg_days_to_approve,
    AVG(fl.days_to_delivery) AS avg_days_to_delivery,
    SUM(CASE WHEN fl.is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    CAST(
        SUM(CASE WHEN fl.is_delayed = 1 THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0) * 100
        AS DECIMAL(5,2)
    ) AS delay_rate_pct
FROM gold.fact_order_lifecycle fl
GROUP BY fl.order_status
ORDER BY total_orders DESC;
```

---

## BƯỚC 10: Verify & Test

### 10.1. Row counts

```sql
SELECT 'dim_seller'              AS tbl, COUNT(*) AS cnt FROM gold.dim_seller
UNION ALL SELECT 'fact_order_lifecycle',  COUNT(*) FROM gold.fact_order_lifecycle
UNION ALL SELECT 'fact_delivery',         COUNT(*) FROM gold.fact_delivery
UNION ALL SELECT 'fact_delivery_year',    COUNT(*) FROM gold.fact_delivery_year
UNION ALL SELECT 'fact_payment_trends',   COUNT(*) FROM gold.fact_payment_trends
UNION ALL SELECT 'fact_payment_trends_yr',COUNT(*) FROM gold.fact_payment_trends_year;
```

### 10.2. Kiểm tra SCD Type 2

```sql
SELECT seller_id, COUNT(*) AS versions
FROM gold.dim_seller
GROUP BY seller_id
HAVING COUNT(*) > 1;
```

### 10.3. Kiểm tra Upsert (chạy lại lifecycle)

```sql
-- Ghi nhận count trước
SELECT COUNT(*) AS before_ct FROM gold.fact_order_lifecycle;
-- Chạy lại Load_Fact_Lifecycle.dtsx
SELECT COUNT(*) AS after_ct FROM gold.fact_order_lifecycle;
-- before_ct == after_ct → không insert trùng
```

### 10.4. Kiểm tra payment totals

```sql
-- Tổng payment phải khớp staging
SELECT SUM(total_payment_value) AS fact_total FROM gold.fact_payment_trends;
SELECT SUM(payment_value)       AS stg_total  FROM staging.stg_order_payments;
-- Có thể lệch nhỏ do orders không có timestamp bị loại
```

### 10.5. Test Master Package

1. Mở `Master_ETL.dtsx`
2. F5 hoặc chuột phải → Execute Package
3. Verify tất cả task chuyển xanh theo thứ tự
4. Nếu task nào đỏ → double-click xem Error message → fix

---

## Checklist hoàn thành TV3

- [ ] 3 staging tables DDL chạy thành công
- [ ] dim_seller DDL chạy thành công (có SCD Type 2 columns)
- [ ] 5 fact tables DDL chạy thành công
- [ ] Package `Extract_Seller_Order_Payment.dtsx` chạy xanh
- [ ] Package `Load_Dim_Seller.dtsx` chạy xanh (SCD Type 2 hoạt động)
- [ ] Package `Load_Fact_Lifecycle.dtsx` chạy xanh (Upsert hoạt động)
- [ ] Package `Load_Fact_Delivery_Payment.dtsx` chạy xanh
- [ ] Upsert verified (chạy lần 2 không insert trùng)
- [ ] SCD Type 2 verified (effective_from/to, is_current đúng)
- [ ] Master Package `Master_ETL.dtsx` chạy end-to-end thành công
- [ ] 3 SQL queries chạy đúng, có kết quả insight
- [ ] Payment totals khớp giữa staging và fact
- [ ] Logging enabled trong Master Package

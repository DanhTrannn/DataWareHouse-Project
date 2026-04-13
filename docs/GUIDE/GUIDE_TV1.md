# 📘 GUIDE_TV1.md – Hướng dẫn chi tiết Thành viên 1

## Domain: Khách hàng & Địa lý & Thời gian

**Phụ trách:**
- Staging: `stg_customers`, `stg_geolocation`, `stg_order_reviews`
- Dimensions: `dim_date`, `dim_geolocation` (SCD Type 1), `dim_customer` (SCD Type 2), `dim_order_status`
- Facts: `fact_customer_orders`, `fact_customer_orders_year`
- SSIS Packages: 3 packages

---

## BƯỚC 0: Chuẩn bị môi trường (Công việc chung – TV1 chịu trách nhiệm chính)

### 0.1. Tạo Database

Mở SSMS → New Query → chạy:

```sql
CREATE DATABASE OlistDW;
GO
USE OlistDW;
GO
```

### 0.2. Tạo Schemas

```sql
CREATE SCHEMA staging;
GO
CREATE SCHEMA gold;
GO
```

### 0.3. Tạo SSIS Project

1. Mở Visual Studio → File → New → Project
2. Chọn **Integration Services Project**
3. Đặt tên: `OlistDW_ETL`
4. Trong Solution Explorer, tạo 3 package files:
   - `Extract_Customer_Geo.dtsx`
   - `Load_Dim_Date_Geo_Customer.dtsx`
   - `Load_Fact_Customer_Orders.dtsx`

### 0.4. Tạo Connection Managers

Trong SSIS Project, tạo 2 Connection Managers dùng chung:

**OLE DB Connection (SQL Server):**
1. Chuột phải Connection Managers → New OLE DB Connection
2. Server name: `localhost` (hoặc tên server)
3. Database: `OlistDW`
4. Đặt tên: `OlistDW_OLEDB`

**Flat File Connections (tạo sau ở từng Data Flow):**
- Sẽ tạo riêng cho mỗi CSV file

---

## BƯỚC 1: Tạo Staging Tables (DDL)

Chạy các script SQL sau trong SSMS:

### 1.1. staging.stg_customers

```sql
IF OBJECT_ID('staging.stg_customers', 'U') IS NOT NULL
    DROP TABLE staging.stg_customers;
GO

CREATE TABLE staging.stg_customers (
    customer_id             VARCHAR(50)   NOT NULL,
    customer_unique_id      VARCHAR(50)   NOT NULL,
    customer_zip_code_prefix VARCHAR(10)  NULL,
    customer_city           NVARCHAR(100) NULL,
    customer_state          VARCHAR(5)    NULL
);
GO
```

### 1.2. staging.stg_geolocation

```sql
IF OBJECT_ID('staging.stg_geolocation', 'U') IS NOT NULL
    DROP TABLE staging.stg_geolocation;
GO

CREATE TABLE staging.stg_geolocation (
    geolocation_zip_code_prefix VARCHAR(10)    NOT NULL,
    geolocation_lat             DECIMAL(10,6)  NULL,
    geolocation_lng             DECIMAL(10,6)  NULL,
    geolocation_city            NVARCHAR(100)  NULL,
    geolocation_state           VARCHAR(5)     NULL
);
GO
```

### 1.3. staging.stg_order_reviews

```sql
IF OBJECT_ID('staging.stg_order_reviews', 'U') IS NOT NULL
    DROP TABLE staging.stg_order_reviews;
GO

CREATE TABLE staging.stg_order_reviews (
    review_id                VARCHAR(50)   NOT NULL,
    order_id                 VARCHAR(50)   NOT NULL,
    review_score             INT           NULL,
    review_comment_title     NVARCHAR(200) NULL,
    review_comment_message   NVARCHAR(MAX) NULL,
    review_creation_date     DATETIME      NULL,
    review_answer_timestamp  DATETIME      NULL
);
GO
```

---

## BƯỚC 2: Tạo Dimension Tables (DDL)

### 2.1. gold.dim_date

```sql
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_date (
    date_key          INT          NOT NULL PRIMARY KEY,  -- format: YYYYMMDD
    full_date         DATE         NOT NULL,
    year              INT          NOT NULL,
    quarter           INT          NOT NULL,
    month             INT          NOT NULL,
    month_name        VARCHAR(20)  NOT NULL,
    day_of_month      INT          NOT NULL,
    day_of_week       INT          NOT NULL,
    day_name          VARCHAR(20)  NOT NULL,
    is_weekend        BIT          NOT NULL DEFAULT 0,
    is_holiday_brazil BIT          NOT NULL DEFAULT 0,
    season_brazil     VARCHAR(20)  NULL
);
GO
```

### 2.2. gold.dim_geolocation

```sql
IF OBJECT_ID('gold.dim_geolocation', 'U') IS NOT NULL
    DROP TABLE gold.dim_geolocation;
GO

CREATE TABLE gold.dim_geolocation (
    geo_key            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    zip_code_prefix    VARCHAR(10)       NOT NULL,
    city               NVARCHAR(100)     NULL,
    state              VARCHAR(5)        NULL,
    region             VARCHAR(20)       NULL,
    latitude           DECIMAL(10,6)     NULL,
    longitude          DECIMAL(10,6)     NULL
);
GO

CREATE UNIQUE INDEX UX_dim_geolocation_zip ON gold.dim_geolocation(zip_code_prefix);
GO
```

### 2.3. gold.dim_customer (SCD Type 2)

```sql
IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL
    DROP TABLE gold.dim_customer;
GO

CREATE TABLE gold.dim_customer (
    customer_key       INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    customer_id        VARCHAR(50)       NOT NULL,
    customer_unique_id VARCHAR(50)       NOT NULL,
    city               NVARCHAR(100)     NULL,
    state              VARCHAR(5)        NULL,
    geo_key            INT               NULL,
    -- SCD Type 2 columns
    effective_from     DATE              NOT NULL DEFAULT '1900-01-01',
    effective_to       DATE              NOT NULL DEFAULT '9999-12-31',
    is_current         BIT               NOT NULL DEFAULT 1,
    CONSTRAINT FK_dim_customer_geo FOREIGN KEY (geo_key)
        REFERENCES gold.dim_geolocation(geo_key)
);
GO

CREATE INDEX IX_dim_customer_unique ON gold.dim_customer(customer_unique_id, is_current);
GO
```

### 2.4. gold.dim_order_status

```sql
IF OBJECT_ID('gold.dim_order_status', 'U') IS NOT NULL
    DROP TABLE gold.dim_order_status;
GO

CREATE TABLE gold.dim_order_status (
    order_status  VARCHAR(30)  NOT NULL PRIMARY KEY,
    description   NVARCHAR(100) NULL
);
GO
```

---

## BƯỚC 3: Tạo Fact Tables (DDL)

### 3.1. gold.fact_customer_orders

```sql
IF OBJECT_ID('gold.fact_customer_orders', 'U') IS NOT NULL
    DROP TABLE gold.fact_customer_orders;
GO

CREATE TABLE gold.fact_customer_orders (
    customer_key     INT            NOT NULL,
    order_status     VARCHAR(30)    NOT NULL,
    date_key         INT            NOT NULL,  -- first day of month: YYYYMM01
    total_orders     INT            NOT NULL DEFAULT 0,
    total_items      INT            NOT NULL DEFAULT 0,
    total_spent      DECIMAL(12,2)  NOT NULL DEFAULT 0,
    avg_review_score DECIMAL(3,2)   NULL,
    CONSTRAINT PK_fact_customer_orders
        PRIMARY KEY (customer_key, order_status, date_key),
    CONSTRAINT FK_fco_customer FOREIGN KEY (customer_key)
        REFERENCES gold.dim_customer(customer_key),
    CONSTRAINT FK_fco_status FOREIGN KEY (order_status)
        REFERENCES gold.dim_order_status(order_status),
    CONSTRAINT FK_fco_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key)
);
GO
```

### 3.2. gold.fact_customer_orders_year

```sql
IF OBJECT_ID('gold.fact_customer_orders_year', 'U') IS NOT NULL
    DROP TABLE gold.fact_customer_orders_year;
GO

CREATE TABLE gold.fact_customer_orders_year (
    customer_key     INT            NOT NULL,
    order_status     VARCHAR(30)    NOT NULL,
    year_key         INT            NOT NULL,  -- YYYY0101
    total_orders     INT            NOT NULL DEFAULT 0,
    total_items      INT            NOT NULL DEFAULT 0,
    total_spent      DECIMAL(12,2)  NOT NULL DEFAULT 0,
    avg_review_score DECIMAL(3,2)   NULL,
    CONSTRAINT PK_fact_customer_orders_year
        PRIMARY KEY (customer_key, order_status, year_key)
);
GO
```

---

## BƯỚC 4: SSIS Package 1 – `Extract_Customer_Geo.dtsx`

### 4.1. Thiết lập Control Flow

Mở package `Extract_Customer_Geo.dtsx` trong Visual Studio:

```
Control Flow Layout:
┌─────────────────────────────────┐
│  Execute SQL Task               │
│  "Truncate Staging Tables"      │
└──────────┬──────────────────────┘
           │ (Success)
     ┌─────┼──────────────┐
     ▼     ▼              ▼
┌────────┐ ┌────────────┐ ┌──────────────┐
│DFT:    │ │DFT:        │ │DFT:          │
│Load    │ │Load        │ │Load          │
│Customer│ │Geolocation │ │Reviews       │
└────────┘ └────────────┘ └──────────────┘
```

**Execute SQL Task – Truncate Staging:**
1. Kéo **Execute SQL Task** vào Control Flow
2. Double-click → General tab:
   - Name: `Truncate Staging Tables`
   - Connection: `OlistDW_OLEDB`
   - SQLStatement:
```sql
TRUNCATE TABLE staging.stg_customers;
TRUNCATE TABLE staging.stg_geolocation;
TRUNCATE TABLE staging.stg_order_reviews;
```

3. Click OK

### 4.2. Data Flow Task – Load Customers

1. Kéo **Data Flow Task** vào Control Flow, đặt tên `DFT - Load Customers`
2. Nối **Precedence Constraint** (mũi tên xanh) từ Truncate → DFT
3. Double-click vào DFT để vào Data Flow tab

**Trong Data Flow:**

```
Flat File Source (olist_customers_dataset.csv)
        │
        ▼
Data Conversion
        │
        ▼
OLE DB Destination (staging.stg_customers)
```

**Bước 4.2.1 – Flat File Source:**
1. Kéo **Flat File Source** vào canvas
2. Double-click → New → Flat File Connection Manager:
   - Name: `FF_Customers`
   - File: chọn file `olist_customers_dataset.csv`
   - Format: Delimited
   - Header row delimiter: `{CR}{LF}`
   - Check ✅ "Column names in the first data row"
   - Tab Columns: verify 5 cột hiển thị đúng
   - Tab Advanced: set tất cả OutputColumnWidth = 100 cho string columns
3. Click OK

**Bước 4.2.2 – Data Conversion:**
1. Kéo **Data Conversion** vào, nối từ Flat File Source
2. Double-click, chọn chuyển đổi:

| Input Column | Output Alias | Data Type | Length |
|---|---|---|---|
| customer_id | cv_customer_id | string [DT_STR] | 50 |
| customer_unique_id | cv_customer_unique_id | string [DT_STR] | 50 |
| customer_zip_code_prefix | cv_zip_code_prefix | string [DT_STR] | 10 |
| customer_city | cv_customer_city | Unicode string [DT_WSTR] | 100 |
| customer_state | cv_customer_state | string [DT_STR] | 5 |

**Bước 4.2.3 – OLE DB Destination:**
1. Kéo **OLE DB Destination**, nối từ Data Conversion
2. Double-click:
   - Connection Manager: `OlistDW_OLEDB`
   - Table: `staging.stg_customers`
   - Tab Mappings: map các cột `cv_*` → cột đích tương ứng

### 4.3. Data Flow Task – Load Geolocation (có Dedup)

1. Thêm **Data Flow Task** mới: `DFT - Load Geolocation`
2. Nối Precedence Constraint từ Truncate

**Trong Data Flow:**

```
Flat File Source (olist_geolocation_dataset.csv)
        │
        ▼
Data Conversion
        │
        ▼
Sort (zip_code_prefix ASC)
        │
        ▼
Aggregate (GROUP BY zip_code_prefix, AVG lat, AVG lng, FIRST city, FIRST state)
        │
        ▼
OLE DB Destination (staging.stg_geolocation)
```

**Bước 4.3.1 – Flat File Source:**
- Tương tự customers, tạo Connection Manager cho `olist_geolocation_dataset.csv`

**Bước 4.3.2 – Data Conversion:**

| Input Column | Output Alias | Data Type | Length/Precision |
|---|---|---|---|
| geolocation_zip_code_prefix | cv_zip | string [DT_STR] | 10 |
| geolocation_lat | cv_lat | numeric [DT_NUMERIC] | Precision 10, Scale 6 |
| geolocation_lng | cv_lng | numeric [DT_NUMERIC] | Precision 10, Scale 6 |
| geolocation_city | cv_city | Unicode string [DT_WSTR] | 100 |
| geolocation_state | cv_state | string [DT_STR] | 5 |

**Bước 4.3.3 – Sort:**
1. Kéo **Sort** transformation
2. Sort by: `cv_zip` ASC
3. Check ✅ "Remove rows with duplicate sort values" → loại bỏ trùng cơ bản

> **Lưu ý:** Dataset geolocation có nhiều dòng trùng zip_code_prefix. Sort + Remove duplicates giữ lại dòng đầu tiên mỗi zip. Nếu muốn lấy AVG lat/lng chính xác hơn, dùng Aggregate thay vì Sort dedup.

**Phương án thay thế (chính xác hơn) – dùng Aggregate:**
1. Bỏ Sort, kéo **Aggregate** transformation
2. Cấu hình:

| Input Column | Output Alias | Operation |
|---|---|---|
| cv_zip | agg_zip | Group By |
| cv_lat | agg_lat | Average |
| cv_lng | agg_lng | Average |
| cv_city | agg_city | Group By |
| cv_state | agg_state | Group By |

**Bước 4.3.4 – OLE DB Destination:**
- Table: `staging.stg_geolocation`
- Map: `agg_zip` → `geolocation_zip_code_prefix`, v.v.

### 4.4. Data Flow Task – Load Reviews

Tương tự pattern:
1. Flat File Source → `olist_order_reviews_dataset.csv`
2. Data Conversion (review_score → DT_I4, timestamps → DT_DBTIMESTAMP)
3. OLE DB Destination → `staging.stg_order_reviews`

### 4.5. Test Package

1. Chuột phải package → **Execute Package** (hoặc F5)
2. Kiểm tra tất cả DFT chuyển xanh (success)
3. Verify bằng SQL:

```sql
SELECT COUNT(*) AS cnt FROM staging.stg_customers;       -- Expected: ~99,441
SELECT COUNT(*) AS cnt FROM staging.stg_geolocation;     -- Expected: ~19,015 (sau dedup)
SELECT COUNT(*) AS cnt FROM staging.stg_order_reviews;   -- Expected: ~99,224
```

---

## BƯỚC 5: SSIS Package 2 – `Load_Dim_Date_Geo_Customer.dtsx`

### 5.1. Control Flow Layout

```
┌──────────────────────┐
│ Execute SQL Task     │
│ "Populate dim_date"  │
└──────────┬───────────┘
           │
┌──────────▼───────────┐
│ Execute SQL Task     │
│ "Populate dim_order  │
│  _status"            │
└──────────┬───────────┘
           │
┌──────────▼───────────┐
│ DFT: Load            │
│ dim_geolocation      │
│ (SCD Type 1)         │
└──────────┬───────────┘
           │
┌──────────▼───────────┐
│ DFT: Load            │
│ dim_customer         │
│ (SCD Type 2)         │
└──────────────────────┘
```

### 5.2. Execute SQL Task – Populate dim_date

```sql
-- Sinh calendar table từ 2016-01-01 đến 2019-12-31
-- Chỉ chạy nếu bảng trống

IF NOT EXISTS (SELECT 1 FROM gold.dim_date)
BEGIN
    DECLARE @start DATE = '2016-01-01';
    DECLARE @end   DATE = '2019-12-31';

    ;WITH DateCTE AS (
        SELECT @start AS dt
        UNION ALL
        SELECT DATEADD(DAY, 1, dt) FROM DateCTE WHERE dt < @end
    )
    INSERT INTO gold.dim_date (
        date_key, full_date, year, quarter, month, month_name,
        day_of_month, day_of_week, day_name, is_weekend, is_holiday_brazil, season_brazil
    )
    SELECT
        CONVERT(INT, FORMAT(dt, 'yyyyMMdd'))       AS date_key,
        dt                                          AS full_date,
        YEAR(dt)                                    AS year,
        DATEPART(QUARTER, dt)                       AS quarter,
        MONTH(dt)                                   AS month,
        DATENAME(MONTH, dt)                         AS month_name,
        DAY(dt)                                     AS day_of_month,
        DATEPART(WEEKDAY, dt)                       AS day_of_week,
        DATENAME(WEEKDAY, dt)                       AS day_name,
        CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
        -- Ngày lễ Brazil chính
        CASE
            WHEN MONTH(dt) = 1  AND DAY(dt) = 1  THEN 1  -- Ano Novo
            WHEN MONTH(dt) = 4  AND DAY(dt) = 21 THEN 1  -- Tiradentes
            WHEN MONTH(dt) = 5  AND DAY(dt) = 1  THEN 1  -- Dia do Trabalho
            WHEN MONTH(dt) = 9  AND DAY(dt) = 7  THEN 1  -- Independência
            WHEN MONTH(dt) = 10 AND DAY(dt) = 12 THEN 1  -- Nossa Senhora Aparecida
            WHEN MONTH(dt) = 11 AND DAY(dt) = 2  THEN 1  -- Finados
            WHEN MONTH(dt) = 11 AND DAY(dt) = 15 THEN 1  -- Proclamação da República
            WHEN MONTH(dt) = 12 AND DAY(dt) = 25 THEN 1  -- Natal
            ELSE 0
        END AS is_holiday_brazil,
        -- Mùa ở Brazil (Nam bán cầu)
        CASE
            WHEN MONTH(dt) IN (12, 1, 2)  THEN 'Summer'
            WHEN MONTH(dt) IN (3, 4, 5)   THEN 'Autumn'
            WHEN MONTH(dt) IN (6, 7, 8)   THEN 'Winter'
            WHEN MONTH(dt) IN (9, 10, 11) THEN 'Spring'
        END AS season_brazil
    FROM DateCTE
    OPTION (MAXRECURSION 1500);
END
```

**Cấu hình trong SSIS:**
1. Kéo **Execute SQL Task**
2. Name: `Populate dim_date`
3. Connection: `OlistDW_OLEDB`
4. Paste SQL ở trên vào SQLStatement

### 5.3. Execute SQL Task – Populate dim_order_status

```sql
IF NOT EXISTS (SELECT 1 FROM gold.dim_order_status)
BEGIN
    INSERT INTO gold.dim_order_status (order_status, description) VALUES
        ('created',      N'Order has been created'),
        ('approved',     N'Payment approved'),
        ('invoiced',     N'Invoice issued'),
        ('processing',   N'Order is being processed'),
        ('shipped',      N'Order shipped to carrier'),
        ('delivered',    N'Order delivered to customer'),
        ('unavailable',  N'Product unavailable'),
        ('canceled',     N'Order canceled');
END
```

### 5.4. Data Flow Task – Load dim_geolocation (SCD Type 1)

```
OLE DB Source (stg_geolocation)
        │
        ▼
Derived Column (thêm "region")
        │
        ▼
Lookup (dim_geolocation – match zip_code_prefix)
   ┌────┴────┐
   │         │
Match    No Match
   │         │
   ▼         ▼
Conditional  OLE DB Destination
Split        (INSERT mới)
(Changed?)
   │
   ▼
OLE DB Command
(UPDATE)
```

**Bước 5.4.1 – OLE DB Source:**
1. Connection: `OlistDW_OLEDB`
2. SQL Command:
```sql
SELECT DISTINCT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM staging.stg_geolocation;
```

**Bước 5.4.2 – Derived Column (thêm region):**
1. Kéo **Derived Column** transformation
2. Thêm cột mới `region` với Expression:

```
(DT_STR, 20, 1252)(
    geolocation_state == "SP" || geolocation_state == "RJ" ||
    geolocation_state == "MG" || geolocation_state == "ES"
        ? "Sudeste"
    : geolocation_state == "PR" || geolocation_state == "SC" || geolocation_state == "RS"
        ? "Sul"
    : geolocation_state == "BA" || geolocation_state == "PE" ||
      geolocation_state == "CE" || geolocation_state == "MA" ||
      geolocation_state == "PB" || geolocation_state == "RN" ||
      geolocation_state == "AL" || geolocation_state == "PI" || geolocation_state == "SE"
        ? "Nordeste"
    : geolocation_state == "AM" || geolocation_state == "PA" ||
      geolocation_state == "RO" || geolocation_state == "TO" ||
      geolocation_state == "AC" || geolocation_state == "AP" || geolocation_state == "RR"
        ? "Norte"
    : geolocation_state == "GO" || geolocation_state == "MT" ||
      geolocation_state == "MS" || geolocation_state == "DF"
        ? "Centro-Oeste"
    : "Unknown"
)
```

**Bước 5.4.3 – Lookup (kiểm tra đã tồn tại):**
1. Kéo **Lookup** transformation
2. General tab: **Redirect rows to no match output** (không fail khi không tìm thấy)
3. Connection tab:
   - Table: `gold.dim_geolocation`
4. Columns tab:
   - Input: `geolocation_zip_code_prefix` → Lookup: `zip_code_prefix`
   - Tick Output: `geo_key` (as `existing_geo_key`), `latitude` (as `existing_lat`), `longitude` (as `existing_lng`), `city` (as `existing_city`)

**Bước 5.4.4 – No Match Output → OLE DB Destination (INSERT mới):**
1. Kéo **OLE DB Destination** từ Lookup "No Match Output"
2. Table: `gold.dim_geolocation`
3. Mapping:
   - `geolocation_zip_code_prefix` → `zip_code_prefix`
   - `geolocation_city` → `city`
   - `geolocation_state` → `state`
   - `region` → `region`
   - `geolocation_lat` → `latitude`
   - `geolocation_lng` → `longitude`

**Bước 5.4.5 – Match Output → Conditional Split → OLE DB Command (UPDATE):**
1. Kéo **Conditional Split** từ Lookup "Match Output"
2. Condition Name: `Is_Changed`
3. Condition:
```
geolocation_lat != existing_lat || geolocation_lng != existing_lng
```
4. Default output: `Unchanged` (bỏ qua)

5. Kéo **OLE DB Command** từ output `Is_Changed`
6. Connection: `OlistDW_OLEDB`
7. SQL Command:
```sql
UPDATE gold.dim_geolocation
SET latitude = ?, longitude = ?, city = ?
WHERE geo_key = ?;
```
8. Column Mappings:
   - Param_0 ← `geolocation_lat`
   - Param_1 ← `geolocation_lng`
   - Param_2 ← `geolocation_city`
   - Param_3 ← `existing_geo_key`

### 5.5. Data Flow Task – Load dim_customer (SCD Type 2)

**Đây là phần phức tạp nhất – SCD Type 2:**

```
OLE DB Source (stg_customers)
        │
        ▼
Lookup dim_geolocation (lấy geo_key)
        │
        ▼
Lookup dim_customer (match customer_unique_id WHERE is_current=1)
   ┌────┴────┐
   │         │
Match    No Match ──→ OLE DB Destination (INSERT mới, is_current=1)
   │
   ▼
Conditional Split
   ┌────┴────┐
   │         │
Changed   Unchanged (bỏ qua)
   │
   ▼
OLE DB Command (UPDATE: set is_current=0, effective_to=GETDATE)
   │
   ▼
OLE DB Destination (INSERT bản ghi mới, is_current=1)
```

**Bước 5.5.1 – OLE DB Source:**
```sql
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM staging.stg_customers;
```

**Bước 5.5.2 – Lookup dim_geolocation (lấy geo_key):**
1. Kéo **Lookup**, set "Redirect rows to no match output"
2. Table: `gold.dim_geolocation`
3. Join: `customer_zip_code_prefix` → `zip_code_prefix`
4. Output: `geo_key` (as `lkp_geo_key`)

> **Lưu ý:** Nếu zip không tìm thấy → No Match → vẫn INSERT customer nhưng geo_key = NULL. Dùng **Union All** để merge cả 2 output trước khi tiếp tục, set `lkp_geo_key = NULL` cho no-match.

**Bước 5.5.3 – Lookup dim_customer (kiểm tra tồn tại):**
1. Kéo **Lookup** thứ 2
2. SQL Command (thay vì chọn table):
```sql
SELECT customer_key, customer_unique_id, city, state, geo_key
FROM gold.dim_customer
WHERE is_current = 1;
```
3. Join: `customer_unique_id` → `customer_unique_id`
4. Output: `customer_key` (as `existing_cust_key`), `city` (as `existing_city`), `state` (as `existing_state`)

**Bước 5.5.4 – No Match → INSERT mới:**
```
OLE DB Destination → gold.dim_customer
Mapping:
  customer_id           → customer_id
  customer_unique_id    → customer_unique_id
  customer_city         → city
  customer_state        → state
  lkp_geo_key           → geo_key
  (Default values)      → effective_from = GETDATE(), effective_to = '9999-12-31', is_current = 1
```

> **Tip:** Dùng **Derived Column** trước OLE DB Destination để thêm:
> - `effective_from` = `(DT_DBDATE)GETDATE()`
> - `effective_to` = `(DT_DBDATE)"9999-12-31"`
> - `is_current` = `(DT_BOOL)TRUE`

**Bước 5.5.5 – Match → Conditional Split:**
- Condition `Is_Changed`:
```
customer_city != existing_city || customer_state != existing_state
```

**Bước 5.5.6 – Changed → Expire bản ghi cũ:**
1. **OLE DB Command:**
```sql
UPDATE gold.dim_customer
SET is_current = 0, effective_to = CAST(GETDATE() AS DATE)
WHERE customer_key = ?;
```
2. Param_0 ← `existing_cust_key`

**Bước 5.5.7 – Changed → INSERT bản ghi mới:**
Sau OLE DB Command, kéo thêm **OLE DB Destination** để INSERT dòng mới với `is_current = 1`, `effective_from = GETDATE()`.

> **Lưu ý quan trọng:** Trong SSIS, một output chỉ nối được 1 destination. Để vừa UPDATE vừa INSERT từ cùng 1 output `Is_Changed`, bạn cần dùng **Multicast** transformation để nhân đôi dòng, rồi 1 nhánh đi OLE DB Command (expire cũ), 1 nhánh đi OLE DB Destination (insert mới).

```
Conditional Split [Is_Changed]
        │
        ▼
    Multicast
   ┌────┴────┐
   ▼         ▼
OLE DB     Derived Column (thêm effective_from, is_current)
Command          │
(Expire)         ▼
           OLE DB Destination (Insert new version)
```

---

## BƯỚC 6: SSIS Package 3 – `Load_Fact_Customer_Orders.dtsx`

### 6.1. Điều kiện tiên quyết

Package này **CHỈ chạy sau khi:**
- `fact_orders` (TV2) đã được load xong
- Tất cả dimensions đã load xong

### 6.2. Control Flow

```
┌──────────────────────────────────────┐
│ Execute SQL Task                     │
│ "Truncate fact_customer_orders"      │
└──────────────┬───────────────────────┘
               │
┌──────────────▼───────────────────────┐
│ DFT: Aggregate & Load               │
│ fact_customer_orders                 │
└──────────────┬───────────────────────┘
               │
┌──────────────▼───────────────────────┐
│ Execute SQL Task                     │
│ "Truncate fact_customer_orders_year" │
└──────────────┬───────────────────────┘
               │
┌──────────────▼───────────────────────┐
│ DFT: Aggregate & Load               │
│ fact_customer_orders_year            │
└──────────────────────────────────────┘
```

### 6.3. Execute SQL Task – Truncate

```sql
TRUNCATE TABLE gold.fact_customer_orders;
```

### 6.4. Data Flow Task – Load fact_customer_orders

**OLE DB Source – SQL Command:**

```sql
SELECT
    fo.customer_key,
    fo.order_status,
    -- date_key = first day of month
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01') AS date_key,
    COUNT(DISTINCT fo.order_id)  AS total_orders,
    COUNT(*)                     AS total_items,
    SUM(fo.price + fo.freight_value) AS total_spent,
    AVG(CAST(fo.review_score AS DECIMAL(3,2))) AS avg_review_score
FROM gold.fact_orders fo
INNER JOIN gold.dim_date dd ON fo.order_date_key = dd.date_key
GROUP BY
    fo.customer_key,
    fo.order_status,
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01');
```

**OLE DB Destination:**
- Table: `gold.fact_customer_orders`
- Map tất cả cột tương ứng

### 6.5. Tương tự cho fact_customer_orders_year

```sql
SELECT
    fo.customer_key,
    fo.order_status,
    dd.year * 10000 + 101 AS year_key,  -- YYYY0101
    COUNT(DISTINCT fo.order_id)  AS total_orders,
    COUNT(*)                     AS total_items,
    SUM(fo.price + fo.freight_value) AS total_spent,
    AVG(CAST(fo.review_score AS DECIMAL(3,2))) AS avg_review_score
FROM gold.fact_orders fo
INNER JOIN gold.dim_date dd ON fo.order_date_key = dd.date_key
GROUP BY
    fo.customer_key,
    fo.order_status,
    dd.year * 10000 + 101;
```

---

## BƯỚC 7: SQL Truy vấn phân tích (2–3 câu insight)

### Query 1: Top 10 khách hàng chi tiêu nhiều nhất

```sql
SELECT TOP 10
    dc.customer_unique_id,
    dc.city,
    dc.state,
    SUM(fco.total_spent) AS lifetime_spent,
    SUM(fco.total_orders) AS lifetime_orders,
    AVG(fco.avg_review_score) AS avg_review
FROM gold.fact_customer_orders fco
INNER JOIN gold.dim_customer dc ON fco.customer_key = dc.customer_key
    AND dc.is_current = 1
GROUP BY dc.customer_unique_id, dc.city, dc.state
ORDER BY lifetime_spent DESC;
```

### Query 2: Phân bố khách hàng theo vùng địa lý và xu hướng hàng tháng

```sql
SELECT
    dg.region,
    dg.state,
    dd.year,
    dd.month,
    COUNT(DISTINCT dc.customer_unique_id) AS unique_customers,
    SUM(fco.total_spent) AS region_revenue
FROM gold.fact_customer_orders fco
INNER JOIN gold.dim_customer dc ON fco.customer_key = dc.customer_key
INNER JOIN gold.dim_geolocation dg ON dc.geo_key = dg.geo_key
INNER JOIN gold.dim_date dd ON fco.date_key = dd.date_key
GROUP BY dg.region, dg.state, dd.year, dd.month
ORDER BY dd.year, dd.month, region_revenue DESC;
```

### Query 3: Tỷ lệ đánh giá trung bình theo trạng thái đơn hàng

```sql
SELECT
    dos.order_status,
    dos.description,
    COUNT(*) AS total_records,
    AVG(fco.avg_review_score) AS avg_score,
    SUM(fco.total_orders) AS total_orders
FROM gold.fact_customer_orders fco
INNER JOIN gold.dim_order_status dos ON fco.order_status = dos.order_status
GROUP BY dos.order_status, dos.description
ORDER BY avg_score DESC;
```

---

## BƯỚC 8: Verify & Test

### 8.1. Kiểm tra row count

```sql
SELECT 'dim_date'          AS tbl, COUNT(*) AS cnt FROM gold.dim_date
UNION ALL
SELECT 'dim_geolocation',         COUNT(*) FROM gold.dim_geolocation
UNION ALL
SELECT 'dim_customer',            COUNT(*) FROM gold.dim_customer
UNION ALL
SELECT 'dim_order_status',        COUNT(*) FROM gold.dim_order_status
UNION ALL
SELECT 'fact_customer_orders',    COUNT(*) FROM gold.fact_customer_orders
UNION ALL
SELECT 'fact_customer_orders_yr', COUNT(*) FROM gold.fact_customer_orders_year;
```

### 8.2. Kiểm tra SCD Type 2

```sql
-- Tìm khách hàng có nhiều hơn 1 version (đã thay đổi city/state)
SELECT customer_unique_id, COUNT(*) AS versions
FROM gold.dim_customer
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
ORDER BY versions DESC;
```

### 8.3. Kiểm tra referential integrity

```sql
-- Kiểm tra fact_customer_orders không có orphan keys
SELECT 'Missing customer' AS issue, COUNT(*)
FROM gold.fact_customer_orders f
LEFT JOIN gold.dim_customer d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL

UNION ALL

SELECT 'Missing date', COUNT(*)
FROM gold.fact_customer_orders f
LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;
```

---

## Checklist hoàn thành TV1

- [ ] Database + schemas đã tạo
- [ ] 3 staging tables DDL chạy thành công
- [ ] 4 dimension tables DDL chạy thành công
- [ ] 2 fact tables DDL chạy thành công
- [ ] dim_date populated (1461 rows: 2016–2019)
- [ ] dim_order_status populated (8 rows)
- [ ] Package `Extract_Customer_Geo.dtsx` chạy xanh
- [ ] Package `Load_Dim_Date_Geo_Customer.dtsx` chạy xanh
- [ ] Package `Load_Fact_Customer_Orders.dtsx` chạy xanh
- [ ] dim_geolocation SCD Type 1 hoạt động
- [ ] dim_customer SCD Type 2 hoạt động (có effective_from/to, is_current)
- [ ] 3 SQL queries chạy đúng, có kết quả insight
- [ ] Verify row counts hợp lý
- [ ] Không có orphan foreign keys

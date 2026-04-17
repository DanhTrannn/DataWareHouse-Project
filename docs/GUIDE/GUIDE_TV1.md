# 📘 GUIDE_TV1.md – Hướng dẫn chi tiết Thành viên 1

## Domain: Khách hàng & Địa lý & Thời gian

**Phụ trách:**
- Staging: `stg_customers`, `stg_geolocation`, `stg_order_reviews`
- Dimensions: `dim_date` (Pre-populate), `dim_geolocation` (SCD Type 1), `dim_customer` (SCD Type 2), `dim_order_status` (Pre-populate)
- Facts: `fact_customer_orders` (month + year)
- SSIS Packages: 3 packages
- **Công việc chung:** Tạo Database + Schemas cho toàn team

---
---

# PHẦN A – CHUẨN BỊ (TV1 LÀ NGƯỜI KHỞI TẠO PROJECT)

---

## BƯỚC 0: Tạo môi trường cho toàn team

### 0.1. Tạo Database trong SQL Server

1. Mở **SQL Server Management Studio (SSMS)**
2. Kết nối tới SQL Server instance (vd: `localhost`, `.\SQLEXPRESS`, hoặc `DESKTOP-XXXX\SQLEXPRESS`)
3. Click **New Query** → paste và chạy:

```sql
-- Tạo database chính cho Data Warehouse
CREATE DATABASE OlistDW;
GO

-- Chuyển sang database vừa tạo
USE OlistDW;
GO
```

4. Verify: trong Object Explorer bên trái → Refresh → thấy `OlistDW` trong danh sách Databases.

### 0.2. Tạo Schemas

Vẫn trong cửa sổ query, chạy tiếp:

```sql
USE OlistDW;
GO

-- Schema cho vùng đệm staging (truncate-reload)
CREATE SCHEMA staging;
GO

-- Schema cho Gold layer (star schema)
CREATE SCHEMA gold;
GO
```

Verify:
```sql
SELECT name FROM sys.schemas WHERE name IN ('staging', 'gold');
-- Phải trả về 2 dòng
```

> **Thông báo cho TV2 và TV3:** Sau khi tạo xong, gửi thông tin kết nối cho team:
> - Server name: `...`
> - Database: `OlistDW`
> - Authentication: Windows / SQL Server

### 0.3. Chuẩn bị file CSV

Tải dataset từ Kaggle về, đặt tất cả file vào **cùng 1 thư mục** (ví dụ `D:\OlistData\`). TV1 cần 3 file:

| File | Tên đầy đủ |
|---|---|
| Customers | `olist_customers_dataset.csv` |
| Geolocation | `olist_geolocation_dataset.csv` |
| Reviews | `olist_order_reviews_dataset.csv` |

### 0.4. Tạo SSIS Project

1. Mở **Visual Studio** (hoặc SQL Server Data Tools – SSDT)
2. File → New → Project
3. Trong dialog New Project:
   - Template: tìm **Integration Services Project** (nếu không thấy → cần cài SQL Server Data Tools extension)
   - Name: `OlistDW_ETL`
   - Location: chọn thư mục làm việc
   - Solution name: `OlistDW_ETL`
4. Click **Create** (hoặc **OK**)
5. Visual Studio tạo project với 1 package mặc định `Package.dtsx`

### 0.5. Tạo 3 SSIS Packages

Trong **Solution Explorer** (panel bên phải):
1. Chuột phải vào `Package.dtsx` → Rename → đổi tên thành `Extract_Customer_Geo.dtsx`
2. Chuột phải thư mục **SSIS Packages** → **New SSIS Package** → đổi tên:
   - `Load_Dim_Date_Geo_Customer.dtsx`
3. Lặp lại:
   - `Load_Fact_Customer_Orders.dtsx`

### 0.6. Tạo OLE DB Connection Manager (dùng chung cho tất cả packages)

1. Double-click vào bất kỳ package nào để mở
2. Ở panel dưới cùng, vùng **Connection Managers**
3. Chuột phải vùng trống → **New OLE DB Connection...**
4. Trong dialog **Configure OLE DB Connection Manager** → click **New...**
5. Cấu hình:

| Thuộc tính | Giá trị |
|---|---|
| Server name | `localhost` hoặc `.\SQLEXPRESS` (tùy cấu hình) |
| Authentication | **Windows Authentication** |
| Select or enter a database name | Chọn **OlistDW** từ dropdown |

6. Click **Test Connection** → phải hiện "Test connection succeeded"
7. Click **OK** → Click **OK**
8. Connection xuất hiện ở panel dưới → chuột phải → **Rename** → `OlistDW_OLEDB`
9. **Quan trọng:** Chuột phải connection → **Convert to Project Connection** → connection sẽ dùng chung cho TẤT CẢ packages trong project (TV2, TV3 cũng dùng được)

---
---

# PHẦN B – TẠO BẢNG TRONG SQL SERVER

---

## BƯỚC 1: Tạo Staging Tables

Mở SSMS → New Query → đảm bảo kết nối database `OlistDW`:

### 1.1. staging.stg_customers

```sql
USE OlistDW;
GO

IF OBJECT_ID('staging.stg_customers', 'U') IS NOT NULL
    DROP TABLE staging.stg_customers;
GO

CREATE TABLE staging.stg_customers (
    customer_id              VARCHAR(50)   NOT NULL,
    customer_unique_id       VARCHAR(50)   NOT NULL,
    customer_zip_code_prefix VARCHAR(10)   NULL,
    customer_city            NVARCHAR(100) NULL,
    customer_state           VARCHAR(5)    NULL
);
GO

-- Verify
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'stg_customers';
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

## BƯỚC 2: Tạo Dimension Tables

### 2.1. gold.dim_date

```sql
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_date (
    date_key          INT          NOT NULL PRIMARY KEY,  -- format YYYYMMDD
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
    -- SCD Type 2: theo dõi lịch sử thay đổi city/state
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
    order_status  VARCHAR(30)   NOT NULL PRIMARY KEY,
    description   NVARCHAR(100) NULL
);
GO
```

---

## BƯỚC 3: Tạo Fact Tables

### 3.1. gold.fact_customer_orders (monthly)

```sql
IF OBJECT_ID('gold.fact_customer_orders', 'U') IS NOT NULL
    DROP TABLE gold.fact_customer_orders;
GO

CREATE TABLE gold.fact_customer_orders (
    customer_key     INT            NOT NULL,
    order_status     VARCHAR(30)    NOT NULL,
    date_key         INT            NOT NULL,
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
    year_key         INT            NOT NULL,
    total_orders     INT            NOT NULL DEFAULT 0,
    total_items      INT            NOT NULL DEFAULT 0,
    total_spent      DECIMAL(12,2)  NOT NULL DEFAULT 0,
    avg_review_score DECIMAL(3,2)   NULL,
    CONSTRAINT PK_fact_customer_orders_year
        PRIMARY KEY (customer_key, order_status, year_key)
);
GO
```

**Kiểm tra tổng:**
```sql
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('staging', 'gold')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

---
---

# PHẦN C – SSIS PACKAGE 1: EXTRACT TO STAGING

---

## BƯỚC 4: Xây dựng `Extract_Customer_Geo.dtsx`

### 4.1. Mở Package

Double-click `Extract_Customer_Geo.dtsx` trong Solution Explorer → mở tab **Control Flow**.

### 4.2. Thêm Execute SQL Task – Truncate Staging

1. Trong **SSIS Toolbox** (panel trái), kéo **Execute SQL Task** vào vùng Control Flow
2. Click vào task → panel Properties bên phải → đổi **Name**: `EST - Truncate Staging Tables`
3. Double-click task → mở editor:

| Thuộc tính | Giá trị |
|---|---|
| Connection | Chọn `OlistDW_OLEDB` |
| SQLSourceType | `Direct input` |
| SQLStatement | Click `...` → paste SQL |

```sql
TRUNCATE TABLE staging.stg_customers;
TRUNCATE TABLE staging.stg_geolocation;
TRUNCATE TABLE staging.stg_order_reviews;
```

4. Click **OK**

---

### 4.3. Data Flow Task 1 – Load Customers

#### 4.3.1. Thêm Data Flow Task

1. Kéo **Data Flow Task** từ Toolbox → đổi tên `DFT - Load Customers`
2. **Nối Precedence Constraint:** Click vào `EST - Truncate Staging Tables` → kéo mũi tên xanh xuống `DFT - Load Customers`
3. Double-click DFT → chuyển sang tab **Data Flow**

#### 4.3.2. Flat File Source

1. Kéo **Flat File Source** từ Toolbox vào canvas
2. Double-click → click **New...** để tạo Connection Manager

**Flat File Connection Manager Editor – Tab General:**

| Thuộc tính | Giá trị |
|---|---|
| Connection manager name | `FF_Customers` |
| File name | Click Browse → `D:\OlistData\olist_customers_dataset.csv` |
| Locale | `English (United States)` |
| Code page | `65001 (UTF-8)` |
| Format | `Delimited` |
| Text qualifier | `"` |
| Header row delimiter | `{CR}{LF}` |
| ✅ Column names in the first data row | Check |

**Tab Columns:**
- Kiểm tra 5 cột: `customer_id`, `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state`
- Column delimiter: `Comma {,}`

**Tab Advanced** – chỉnh DataType cho chính xác:

| Column Name | DataType | OutputColumnWidth |
|---|---|---|
| customer_id | string [DT_STR] | 50 |
| customer_unique_id | string [DT_STR] | 50 |
| customer_zip_code_prefix | string [DT_STR] | 10 |
| customer_city | Unicode string [DT_WSTR] | 100 |
| customer_state | string [DT_STR] | 5 |

> **Cách chỉnh:** Click tên cột ở panel trái → sửa DataType và OutputColumnWidth ở panel phải.

3. Click **OK** → Click **OK**

#### 4.3.3. OLE DB Destination

1. Kéo **OLE DB Destination** → nối mũi tên xanh từ Flat File Source
2. Double-click:

| Thuộc tính | Giá trị |
|---|---|
| OLE DB connection manager | `OlistDW_OLEDB` |
| Data access mode | `Table or view - fast load` |
| Name of the table or view | `[staging].[stg_customers]` |
| Table lock | ✅ Checked |

**Tab Mappings:** SSIS tự map nếu tên cột giống. Kiểm tra 5 cột map đúng.

3. Click **OK**

> **Không cần Data Conversion** vì đã set đúng DataType ở Flat File Connection Manager.

---

### 4.4. Data Flow Task 2 – Load Geolocation (có Dedup)

#### 4.4.1. Quay lại Control Flow → kéo Data Flow Task → đổi tên `DFT - Load Geolocation`

#### 4.4.2. Nối từ Truncate task (chạy song song với Load Customers)

#### 4.4.3. Vào Data Flow

**Vấn đề:** File `olist_geolocation_dataset.csv` có ~1 triệu dòng với nhiều dòng trùng `zip_code_prefix`. Cần dedup trước khi load vào staging.

**Layout:**

```
Flat File Source (olist_geolocation_dataset.csv)
        │
        ▼
Sort (zip_code_prefix ASC, Remove duplicates ✅)
        │
        ▼
OLE DB Destination (staging.stg_geolocation)
```

**Flat File Source:**
1. Tạo Connection Manager mới: `FF_Geolocation` → file `olist_geolocation_dataset.csv`
2. Tab Advanced:

| Column Name | DataType | OutputColumnWidth |
|---|---|---|
| geolocation_zip_code_prefix | string [DT_STR] | 10 |
| geolocation_lat | string [DT_STR] | 20 |
| geolocation_lng | string [DT_STR] | 20 |
| geolocation_city | Unicode string [DT_WSTR] | 100 |
| geolocation_state | string [DT_STR] | 5 |

> **Lưu ý:** Set lat/lng thành string trước, sẽ convert ở Derived Column.

**Derived Column (convert lat/lng):**
1. Kéo **Derived Column** → nối từ Flat File Source
2. Thêm 2 cột:

| Name | Expression |
|---|---|
| `cv_lat` | `(DT_NUMERIC,10,6)geolocation_lat` |
| `cv_lng` | `(DT_NUMERIC,10,6)geolocation_lng` |

3. **OK**

**Sort (dedup):**
1. Kéo **Sort** → nối từ Derived Column
2. Double-click:
   - Tick ✅ `geolocation_zip_code_prefix` → Sort Order: Ascending
   - Check ✅ **"Remove rows with duplicate sort values"**
3. **OK**

> Sort + Remove duplicates giữ lại **dòng đầu tiên** cho mỗi zip_code_prefix. Cách này đơn giản và phù hợp cho staging.

**OLE DB Destination:**
1. Kéo → nối từ Sort
2. Table: `[staging].[stg_geolocation]`
3. Mappings:

| Input Column | Destination Column |
|---|---|
| geolocation_zip_code_prefix | geolocation_zip_code_prefix |
| cv_lat | geolocation_lat |
| cv_lng | geolocation_lng |
| geolocation_city | geolocation_city |
| geolocation_state | geolocation_state |

4. **OK**

---

### 4.5. Data Flow Task 3 – Load Reviews

#### 4.5.1. Quay lại Control Flow → kéo Data Flow Task → đổi tên `DFT - Load Reviews`

#### 4.5.2. Nối từ Truncate (chạy song song với 2 DFT kia)

#### 4.5.3. Vào Data Flow

**Flat File Source:**
1. Tạo Connection Manager: `FF_Reviews` → file `olist_order_reviews_dataset.csv`
2. Tab Advanced – set TẤT CẢ thành string (vì có cột timestamp có thể rỗng):

| Column Name | DataType | OutputColumnWidth |
|---|---|---|
| review_id | string [DT_STR] | 50 |
| order_id | string [DT_STR] | 50 |
| review_score | string [DT_STR] | 10 |
| review_comment_title | Unicode string [DT_WSTR] | 200 |
| review_comment_message | Unicode string [DT_WSTR] | 4000 |
| review_creation_date | string [DT_STR] | 30 |
| review_answer_timestamp | string [DT_STR] | 30 |

**Derived Column (convert an toàn):**
1. Kéo **Derived Column** → nối từ Flat File Source
2. Thêm 3 cột:

| Name | Expression |
|---|---|
| `cv_score` | `LEN(TRIM(review_score)) == 0 ? NULL(DT_I4) : (DT_I4)review_score` |
| `cv_creation` | `LEN(TRIM(review_creation_date)) > 0 ? (DT_DBTIMESTAMP)review_creation_date : NULL(DT_DBTIMESTAMP)` |
| `cv_answer` | `LEN(TRIM(review_answer_timestamp)) > 0 ? (DT_DBTIMESTAMP)review_answer_timestamp : NULL(DT_DBTIMESTAMP)` |

3. **OK**

**OLE DB Destination:**
1. Table: `[staging].[stg_order_reviews]`
2. Mappings:

| Input Column | Destination Column |
|---|---|
| review_id | review_id |
| order_id | order_id |
| cv_score | review_score |
| review_comment_title | review_comment_title |
| review_comment_message | review_comment_message |
| cv_creation | review_creation_date |
| cv_answer | review_answer_timestamp |

---

### 4.6. Kiểm tra Control Flow hoàn chỉnh

```
         ┌──────────────────────────────────────┐
         │  EST - Truncate Staging Tables        │
         └──────┬──────────┬──────────┬──────────┘
                │          │          │
         (Success)   (Success)   (Success)
                │          │          │
                ▼          ▼          ▼
         ┌──────────┐ ┌──────────┐ ┌──────────┐
         │DFT - Load│ │DFT - Load│ │DFT - Load│
         │Customers │ │Geoloc.   │ │Reviews   │
         └──────────┘ └──────────┘ └──────────┘
```

### 4.7. Chạy test Package

1. Chuột phải `Extract_Customer_Geo.dtsx` → **Execute Package** (hoặc F5)
2. Truncate → xanh; 3 DFTs → vàng (song song) → xanh
3. Click **Stop Debugging**
4. Verify:

```sql
SELECT 'stg_customers'    AS tbl, COUNT(*) AS rows_loaded FROM staging.stg_customers
UNION ALL
SELECT 'stg_geolocation',        COUNT(*) FROM staging.stg_geolocation
UNION ALL
SELECT 'stg_order_reviews',      COUNT(*) FROM staging.stg_order_reviews;
```

Kỳ vọng:

| tbl | rows_loaded |
|---|---|
| stg_customers | ~99,441 |
| stg_geolocation | ~19,015 (sau dedup) |
| stg_order_reviews | ~99,224 |

---
---

# PHẦN D – SSIS PACKAGE 2: LOAD DIMENSIONS

---

## BƯỚC 5: Xây dựng `Load_Dim_Date_Geo_Customer.dtsx`

### 5.1. Mở Package → Control Flow

Double-click `Load_Dim_Date_Geo_Customer.dtsx`.

Layout tổng:

```
┌──────────────────────────────┐
│ EST - Populate dim_date      │
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ EST - Populate dim_order_    │
│ status                       │
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ DFT - Load dim_geolocation   │
│ (SCD Type 1)                 │
└──────────┬───────────────────┘
           │
┌──────────▼───────────────────┐
│ DFT - Load dim_customer      │
│ (SCD Type 2)                 │
└──────────────────────────────┘
```

> Thứ tự tuần tự vì: dim_customer cần geo_key từ dim_geolocation.

---

### 5.2. Task 1 – Execute SQL: Populate dim_date

1. Kéo **Execute SQL Task** → đổi tên `EST - Populate dim_date`
2. Double-click:
   - Connection: `OlistDW_OLEDB`
   - SQLStatement:

```sql
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
        day_of_month, day_of_week, day_name, is_weekend,
        is_holiday_brazil, season_brazil
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
        CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END,
        CASE
            WHEN MONTH(dt) = 1  AND DAY(dt) = 1  THEN 1
            WHEN MONTH(dt) = 4  AND DAY(dt) = 21 THEN 1
            WHEN MONTH(dt) = 5  AND DAY(dt) = 1  THEN 1
            WHEN MONTH(dt) = 9  AND DAY(dt) = 7  THEN 1
            WHEN MONTH(dt) = 10 AND DAY(dt) = 12 THEN 1
            WHEN MONTH(dt) = 11 AND DAY(dt) = 2  THEN 1
            WHEN MONTH(dt) = 11 AND DAY(dt) = 15 THEN 1
            WHEN MONTH(dt) = 12 AND DAY(dt) = 25 THEN 1
            ELSE 0
        END,
        CASE
            WHEN MONTH(dt) IN (12, 1, 2)  THEN 'Summer'
            WHEN MONTH(dt) IN (3, 4, 5)   THEN 'Autumn'
            WHEN MONTH(dt) IN (6, 7, 8)   THEN 'Winter'
            WHEN MONTH(dt) IN (9, 10, 11) THEN 'Spring'
        END
    FROM DateCTE
    OPTION (MAXRECURSION 1500);
END
```

3. Click **OK**

> Script sinh 1461 dòng (2016-01-01 đến 2019-12-31). `IF NOT EXISTS` đảm bảo chỉ chạy 1 lần.

---

### 5.3. Task 2 – Execute SQL: Populate dim_order_status

1. Kéo **Execute SQL Task** → đổi tên `EST - Populate dim_order_status`
2. Nối: `EST - Populate dim_date` → task này
3. Connection: `OlistDW_OLEDB`
4. SQL:

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

5. Click **OK**

---

### 5.4. Task 3 – Data Flow: Load dim_geolocation (SCD Type 1)

#### 5.4.1. Thêm DFT → đổi tên `DFT - Load dim_geolocation`

Nối: `EST - Populate dim_order_status` → DFT

#### 5.4.2. Vào Data Flow – tổng quan:

```
OLE DB Source (stg_geolocation – DISTINCT)
        │
        ▼
Derived Column (thêm region)
        │
        ▼
Lookup dim_geolocation (match zip_code_prefix)
   ┌────┴────┐
   │         │
Match    No Match ──→ OLE DB Destination (INSERT)
   │
   ▼
Conditional Split (lat/lng thay đổi?)
   ┌────┴────┐
   │         │
Changed   Unchanged (bỏ qua)
   │
   ▼
OLE DB Command (UPDATE)
```

#### 5.4.3. Component 1 – OLE DB Source

1. Kéo **OLE DB Source** → double-click:
   - Connection: `OlistDW_OLEDB`
   - Data access mode: `SQL command`
   - SQL:

```sql
SELECT DISTINCT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM staging.stg_geolocation;
```

2. Click **Preview** → verify → **OK**

#### 5.4.4. Component 2 – Derived Column (thêm region)

1. Kéo **Derived Column** → nối từ OLE DB Source
2. Thêm cột mới `region`:

| Name | Expression |
|---|---|
| `region` | *(copy expression bên dưới)* |

```
(DT_STR,20,1252)(
    geolocation_state == "SP" || geolocation_state == "RJ" || geolocation_state == "MG" || geolocation_state == "ES"
    ? "Sudeste"
    : geolocation_state == "PR" || geolocation_state == "SC" || geolocation_state == "RS"
    ? "Sul"
    : geolocation_state == "BA" || geolocation_state == "PE" || geolocation_state == "CE" || geolocation_state == "MA" || geolocation_state == "PB" || geolocation_state == "RN" || geolocation_state == "AL" || geolocation_state == "PI" || geolocation_state == "SE"
    ? "Nordeste"
    : geolocation_state == "AM" || geolocation_state == "PA" || geolocation_state == "RO" || geolocation_state == "TO" || geolocation_state == "AC" || geolocation_state == "AP" || geolocation_state == "RR"
    ? "Norte"
    : geolocation_state == "GO" || geolocation_state == "MT" || geolocation_state == "MS" || geolocation_state == "DF"
    ? "Centro-Oeste"
    : "Unknown"
)
```

3. **OK**

#### 5.4.5. Component 3 – Lookup dim_geolocation

1. Kéo **Lookup** → nối từ Derived Column
2. Double-click:

**Tab General:** Redirect rows to no match output

**Tab Connection:**
- OLE DB connection: `OlistDW_OLEDB`
- Use a table or view: `gold.dim_geolocation`

**Tab Columns:**
- Join: `geolocation_zip_code_prefix` → `zip_code_prefix`
- Output:
  - ✅ `geo_key` → Alias: `existing_geo_key`
  - ✅ `latitude` → Alias: `existing_lat`
  - ✅ `longitude` → Alias: `existing_lng`
  - ✅ `city` → Alias: `existing_city`

3. **OK**

#### 5.4.6. No Match → OLE DB Destination (INSERT)

1. Kéo **OLE DB Destination**
2. Nối: Lookup → **Lookup No Match Output** → OLE DB Dest
3. Table: `[gold].[dim_geolocation]`
4. Mappings:

| Input Column | Destination Column |
|---|---|
| geolocation_zip_code_prefix | zip_code_prefix |
| geolocation_city | city |
| geolocation_state | state |
| region | region |
| geolocation_lat | latitude |
| geolocation_lng | longitude |

> `geo_key` không map (IDENTITY).

#### 5.4.7. Match → Conditional Split → OLE DB Command (UPDATE)

**Conditional Split:**
1. Kéo **Conditional Split** → nối từ Lookup **Match Output**
2. Condition:

| Output Name | Condition |
|---|---|
| `Is_Changed` | `geolocation_lat != existing_lat \|\| geolocation_lng != existing_lng` |
| Default | `Unchanged` |

**OLE DB Command:**
1. Kéo **OLE DB Command** → nối từ output `Is_Changed`
2. Connection: `OlistDW_OLEDB`
3. SqlCommand:

```sql
UPDATE gold.dim_geolocation
SET latitude = ?, longitude = ?, city = ?
WHERE geo_key = ?;
```

4. Column Mappings:

| Input Column | Destination Column |
|---|---|
| geolocation_lat | Param_0 |
| geolocation_lng | Param_1 |
| geolocation_city | Param_2 |
| existing_geo_key | Param_3 |

---

### 5.5. Task 4 – Data Flow: Load dim_customer ⭐ (SCD Type 2)

**Đây là phần phức tạp nhất toàn bộ pipeline của TV1.**

#### 5.5.1. Quay lại Control Flow → kéo DFT → đổi tên `DFT - Load dim_customer (SCD Type 2)`

Nối: `DFT - Load dim_geolocation` → DFT mới

#### 5.5.2. Vào Data Flow – tổng quan:

```
OLE DB Source (stg_customers)
        │
        ▼
Lookup dim_geolocation (lấy geo_key)
   ├── No Match → DerCol(NULL) ──→ Union All ←── Match
        │
        ▼
Lookup dim_customer (match customer_unique_id WHERE is_current=1)
   ┌────┴────┐
   │         │
Match    No Match
   │         │
   │         ▼
   │    Derived Column (SCD cols) → OLE DB Dest (INSERT NEW)
   │
   ▼
Conditional Split
   ┌────┴────┐
   │         │
Changed   Unchanged (bỏ qua)
   │
   ▼
Multicast
┌───┴───┐
▼       ▼
OLE DB  Derived Column (SCD cols)
Command      │
(EXPIRE)     ▼
        OLE DB Dest (INSERT NEW VERSION)
```

#### 5.5.3. Component 1 – OLE DB Source

```sql
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM staging.stg_customers;
```

#### 5.5.4. Component 2 – Lookup dim_geolocation (lấy geo_key)

1. Kéo **Lookup** → nối từ OLE DB Source
2. General: **Redirect rows to no match output**
3. Connection – SQL:

```sql
SELECT geo_key, zip_code_prefix
FROM gold.dim_geolocation;
```

4. Columns: Join `customer_zip_code_prefix` → `zip_code_prefix`, Output: ✅ `geo_key` → `lkp_geo_key`

**Xử lý No Match:**
1. Kéo **Derived Column** → nối Lookup **No Match Output**
2. Thêm: `lkp_geo_key` | `NULL(DT_I4)`
3. Kéo **Union All** → nối Match + No Match
4. Map `lkp_geo_key` từ cả 2

#### 5.5.5. Component 3 – Lookup dim_customer (kiểm tra tồn tại)

1. Kéo **Lookup** → nối từ Union All
2. General: **Redirect rows to no match output**
3. Connection – SQL:

```sql
SELECT customer_key, customer_unique_id, city, state, geo_key
FROM gold.dim_customer
WHERE is_current = 1;
```

4. Columns:
   - Join: `customer_unique_id` → `customer_unique_id`
   - Output:
     - ✅ `customer_key` → `existing_cust_key`
     - ✅ `city` → `existing_city`
     - ✅ `state` → `existing_state`

#### 5.5.6. No Match → INSERT customer mới

**Derived Column (SCD cols):**
1. Kéo **Derived Column** → nối Lookup **No Match Output**
2. Thêm 3 cột:

| Name | Expression |
|---|---|
| `scd_effective_from` | `(DT_DBDATE)GETDATE()` |
| `scd_effective_to` | `(DT_DBDATE)"9999-12-31"` |
| `scd_is_current` | `(DT_BOOL)TRUE` |

**OLE DB Destination:**
1. Kéo → nối từ Derived Column
2. Table: `[gold].[dim_customer]`
3. Mappings:

| Input Column | Destination Column |
|---|---|
| customer_id | customer_id |
| customer_unique_id | customer_unique_id |
| customer_city | city |
| customer_state | state |
| lkp_geo_key | geo_key |
| scd_effective_from | effective_from |
| scd_effective_to | effective_to |
| scd_is_current | is_current |

> `customer_key` không map (IDENTITY).

#### 5.5.7. Match → Conditional Split

1. Kéo **Conditional Split** → nối Lookup **Match Output**
2. Condition:

| Output Name | Condition |
|---|---|
| `Is_Changed` | `customer_city != existing_city \|\| customer_state != existing_state` |
| Default | `Unchanged` |

#### 5.5.8. Changed → Multicast → Expire + Insert

**Tại sao cần Multicast?** SCD Type 2 yêu cầu 2 hành động cho mỗi dòng thay đổi:
1. UPDATE bản ghi cũ: `is_current = 0`, `effective_to = today`
2. INSERT bản ghi mới với dữ liệu mới, `is_current = 1`

SSIS không cho 1 output nối tới 2 destinations → Multicast nhân đôi.

**Multicast:**
1. Kéo **Multicast** → nối từ Conditional Split output `Is_Changed`

**Nhánh 1 – OLE DB Command (Expire bản ghi cũ):**
1. Kéo **OLE DB Command** → nối từ Multicast (Output 0)
2. Connection: `OlistDW_OLEDB`
3. SqlCommand:

```sql
UPDATE gold.dim_customer
SET is_current = 0, effective_to = CAST(GETDATE() AS DATE)
WHERE customer_key = ?;
```

4. Column Mappings: `existing_cust_key` → `Param_0`

**Nhánh 2 – INSERT bản ghi mới (new version):**
1. Kéo **Derived Column** → nối từ Multicast (Output 1)
2. Thêm 3 cột:

| Name | Expression |
|---|---|
| `new_effective_from` | `(DT_DBDATE)GETDATE()` |
| `new_effective_to` | `(DT_DBDATE)"9999-12-31"` |
| `new_is_current` | `(DT_BOOL)TRUE` |

3. Kéo **OLE DB Destination** → nối từ Derived Column
4. Table: `[gold].[dim_customer]`
5. Mappings: tương tự bước 5.5.6, dùng cột `new_*` cho SCD columns

> **Đặt tên rõ:** Rename 2 OLE DB Destinations:
> - `OLE DB Dest - INSERT New Customer`
> - `OLE DB Dest - INSERT Changed Version`

### 5.6. Chạy test

```sql
SELECT 'dim_date'         AS tbl, COUNT(*) AS cnt FROM gold.dim_date
UNION ALL SELECT 'dim_order_status',  COUNT(*) FROM gold.dim_order_status
UNION ALL SELECT 'dim_geolocation',   COUNT(*) FROM gold.dim_geolocation
UNION ALL SELECT 'dim_customer',      COUNT(*) FROM gold.dim_customer;
```

Kỳ vọng:

| tbl | cnt |
|---|---|
| dim_date | 1461 |
| dim_order_status | 8 |
| dim_geolocation | ~19,015 |
| dim_customer | ~99,441 |

```sql
-- Verify SCD Type 2: lần đầu tất cả is_current = 1
SELECT is_current, COUNT(*) FROM gold.dim_customer GROUP BY is_current;
-- Expected: is_current=1 → ~99,441
```

---
---

# PHẦN E – SSIS PACKAGE 3: LOAD FACT_CUSTOMER_ORDERS

---

## BƯỚC 6: Xây dựng `Load_Fact_Customer_Orders.dtsx`

### 6.1. Điều kiện tiên quyết

⚠️ Package này **CHỈ chạy sau khi:**
- `gold.fact_orders` (TV2) đã load xong
- Tất cả dimensions đã load xong

Kiểm tra:
```sql
SELECT
    (SELECT COUNT(*) FROM gold.fact_orders)    AS fact_orders,
    (SELECT COUNT(*) FROM gold.dim_customer WHERE is_current = 1) AS dim_customer,
    (SELECT COUNT(*) FROM gold.dim_date)       AS dim_date;
-- Tất cả phải > 0
```

> **Nếu `gold.fact_orders` chưa có dữ liệu (TV2 chưa chạy):** Dùng query thay thế ở bước 6.4 bên dưới.

### 6.2. Mở Package → Control Flow

```
┌──────────────────────────────────┐
│ EST - Truncate fact_customer_    │
│ orders                           │
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│ DFT - Load fact_customer_orders  │
│ (monthly)                        │
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│ EST - Truncate fact_customer_    │
│ orders_year                      │
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│ DFT - Load fact_customer_orders  │
│ _year                            │
└──────────────────────────────────┘
```

### 6.3. Task 1 – Truncate

1. Kéo **Execute SQL Task** → đổi tên `EST - Truncate fact_customer_orders`
2. SQL: `TRUNCATE TABLE gold.fact_customer_orders;`

### 6.4. Task 2 – Data Flow: fact_customer_orders (monthly)

1. Kéo DFT → đổi tên `DFT - Load fact_customer_orders`
2. Nối từ Truncate
3. Vào Data Flow:

**OLE DB Source** – dùng query từ **staging tables** (để không phụ thuộc fact_orders của TV2):

```sql
SELECT
    dc.customer_key,
    o.order_status,
    CONVERT(INT, FORMAT(CAST(o.order_purchase_timestamp AS DATE), 'yyyyMM') + '01') AS date_key,
    COUNT(DISTINCT o.order_id)  AS total_orders,
    COUNT(*)                     AS total_items,
    SUM(oi.price + oi.freight_value) AS total_spent,
    AVG(CAST(r.review_score AS DECIMAL(3,2))) AS avg_review_score
FROM staging.stg_order_items oi
INNER JOIN staging.stg_orders o
    ON oi.order_id = o.order_id
LEFT JOIN staging.stg_order_reviews r
    ON o.order_id = r.order_id
INNER JOIN gold.dim_customer dc
    ON o.customer_id = dc.customer_id
    AND dc.is_current = 1
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY
    dc.customer_key,
    o.order_status,
    CONVERT(INT, FORMAT(CAST(o.order_purchase_timestamp AS DATE), 'yyyyMM') + '01');
```

> **Lưu ý:** Query này dùng `staging.stg_orders` (TV3) và `staging.stg_order_items` (TV2). Cần đảm bảo cả 2 đã chạy Extract trước.

> **Query thay thế nếu `gold.fact_orders` ĐÃ có dữ liệu** (dùng khi chạy trong Master Package):
> ```sql
> SELECT
>     fo.customer_key,
>     fo.order_status,
>     CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01') AS date_key,
>     COUNT(DISTINCT fo.order_id)  AS total_orders,
>     COUNT(*)                     AS total_items,
>     SUM(fo.price + fo.freight_value) AS total_spent,
>     AVG(CAST(fo.review_score AS DECIMAL(3,2))) AS avg_review_score
> FROM gold.fact_orders fo
> INNER JOIN gold.dim_date dd ON fo.order_date_key = dd.date_key
> GROUP BY fo.customer_key, fo.order_status,
>     CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01');
> ```

**OLE DB Destination:**
1. Table: `[gold].[fact_customer_orders]`
2. Mappings:

| Input Column | Destination Column |
|---|---|
| customer_key | customer_key |
| order_status | order_status |
| date_key | date_key |
| total_orders | total_orders |
| total_items | total_items |
| total_spent | total_spent |
| avg_review_score | avg_review_score |

### 6.5. Task 3 + 4 – fact_customer_orders_year

**Truncate:** `TRUNCATE TABLE gold.fact_customer_orders_year;`

**OLE DB Source:**

```sql
SELECT
    dc.customer_key,
    o.order_status,
    YEAR(o.order_purchase_timestamp) * 10000 + 101 AS year_key,
    COUNT(DISTINCT o.order_id)  AS total_orders,
    COUNT(*)                     AS total_items,
    SUM(oi.price + oi.freight_value) AS total_spent,
    AVG(CAST(r.review_score AS DECIMAL(3,2))) AS avg_review_score
FROM staging.stg_order_items oi
INNER JOIN staging.stg_orders o
    ON oi.order_id = o.order_id
LEFT JOIN staging.stg_order_reviews r
    ON o.order_id = r.order_id
INNER JOIN gold.dim_customer dc
    ON o.customer_id = dc.customer_id
    AND dc.is_current = 1
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY
    dc.customer_key,
    o.order_status,
    YEAR(o.order_purchase_timestamp) * 10000 + 101;
```

**OLE DB Destination:** `[gold].[fact_customer_orders_year]`

### 6.6. Chạy test

```sql
SELECT 'fact_customer_orders'     AS tbl, COUNT(*) AS cnt FROM gold.fact_customer_orders
UNION ALL
SELECT 'fact_customer_orders_year',       COUNT(*) FROM gold.fact_customer_orders_year;
```

---
---

# PHẦN F – SQL TRUY VẤN PHÂN TÍCH

---

## BƯỚC 7: Viết 3 câu truy vấn insight

### Query 1: Top 10 khách hàng chi tiêu nhiều nhất

```sql
SELECT TOP 10
    dc.customer_unique_id,
    dc.city,
    dc.state,
    dg.region,
    SUM(fco.total_spent)   AS lifetime_spent,
    SUM(fco.total_orders)  AS lifetime_orders,
    AVG(fco.avg_review_score) AS avg_review
FROM gold.fact_customer_orders fco
INNER JOIN gold.dim_customer dc
    ON fco.customer_key = dc.customer_key AND dc.is_current = 1
LEFT JOIN gold.dim_geolocation dg
    ON dc.geo_key = dg.geo_key
GROUP BY dc.customer_unique_id, dc.city, dc.state, dg.region
ORDER BY lifetime_spent DESC;
```

### Query 2: Phân bố khách hàng & doanh thu theo vùng địa lý

```sql
SELECT
    dg.region,
    dg.state,
    COUNT(DISTINCT dc.customer_unique_id) AS unique_customers,
    SUM(fco.total_spent)   AS total_revenue,
    SUM(fco.total_orders)  AS total_orders,
    AVG(fco.avg_review_score) AS avg_review
FROM gold.fact_customer_orders fco
INNER JOIN gold.dim_customer dc
    ON fco.customer_key = dc.customer_key AND dc.is_current = 1
INNER JOIN gold.dim_geolocation dg
    ON dc.geo_key = dg.geo_key
GROUP BY dg.region, dg.state
ORDER BY total_revenue DESC;
```

### Query 3: Đánh giá trung bình theo trạng thái đơn hàng

```sql
SELECT
    dos.order_status,
    dos.description,
    SUM(fco.total_orders) AS total_orders,
    AVG(fco.avg_review_score) AS avg_score,
    SUM(fco.total_spent) AS total_revenue
FROM gold.fact_customer_orders fco
INNER JOIN gold.dim_order_status dos
    ON fco.order_status = dos.order_status
GROUP BY dos.order_status, dos.description
ORDER BY avg_score DESC;
```

---
---

# PHẦN G – VERIFY & TEST

---

## BƯỚC 8: Kiểm tra toàn bộ

### 8.1. Row counts

```sql
SELECT 'dim_date'              AS tbl, COUNT(*) AS cnt FROM gold.dim_date
UNION ALL SELECT 'dim_geolocation',     COUNT(*) FROM gold.dim_geolocation
UNION ALL SELECT 'dim_customer',        COUNT(*) FROM gold.dim_customer
UNION ALL SELECT 'dim_order_status',    COUNT(*) FROM gold.dim_order_status
UNION ALL SELECT 'fact_customer_orders',COUNT(*) FROM gold.fact_customer_orders
UNION ALL SELECT 'fact_cust_orders_yr', COUNT(*) FROM gold.fact_customer_orders_year;
```

### 8.2. Kiểm tra SCD Type 2

```sql
-- Tất cả customers phải có ít nhất 1 bản ghi is_current = 1
SELECT
    (SELECT COUNT(DISTINCT customer_unique_id) FROM gold.dim_customer) AS unique_custs,
    (SELECT COUNT(DISTINCT customer_unique_id) FROM gold.dim_customer WHERE is_current = 1) AS current_custs;
-- Hai số phải bằng nhau

-- Tìm customers có nhiều versions
SELECT customer_unique_id, COUNT(*) AS versions
FROM gold.dim_customer
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
ORDER BY versions DESC;
```

### 8.3. Kiểm tra SCD Type 1 (geolocation)

```sql
-- Chạy lại Load_Dim_Date_Geo_Customer.dtsx
-- dim_geolocation count không đổi (chỉ update lat/lng nếu khác)
SELECT COUNT(*) FROM gold.dim_geolocation;
```

### 8.4. Kiểm tra FK integrity

```sql
SELECT 'orphan_customer' AS issue, COUNT(*)
FROM gold.fact_customer_orders f
LEFT JOIN gold.dim_customer d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL
UNION ALL
SELECT 'orphan_date', COUNT(*)
FROM gold.fact_customer_orders f
LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL
UNION ALL
SELECT 'orphan_status', COUNT(*)
FROM gold.fact_customer_orders f
LEFT JOIN gold.dim_order_status d ON f.order_status = d.order_status
WHERE d.order_status IS NULL;
-- Tất cả phải = 0
```

### 8.5. Kiểm tra dim_date completeness

```sql
SELECT
    MIN(full_date) AS earliest,
    MAX(full_date) AS latest,
    COUNT(*) AS total_days,
    SUM(CAST(is_weekend AS INT)) AS weekend_days,
    SUM(CAST(is_holiday_brazil AS INT)) AS holidays
FROM gold.dim_date;
-- Expected: 2016-01-01 to 2019-12-31, 1461 days
```

---
---

# CHECKLIST HOÀN THÀNH TV1

- [ ] **Database:** `OlistDW` tạo thành công
- [ ] **Schemas:** `staging` và `gold` tạo thành công
- [ ] **DDL:** 3 staging tables tạo thành công
- [ ] **DDL:** 4 dimension tables tạo thành công (dim_customer có SCD Type 2 columns)
- [ ] **DDL:** 2 fact tables tạo thành công
- [ ] **Connection:** `OlistDW_OLEDB` Project Connection tạo thành công
- [ ] **Package 1:** `Extract_Customer_Geo.dtsx` – 3 DFT xanh, row counts đúng
- [ ] **Package 2:** `Load_Dim_Date_Geo_Customer.dtsx`:
  - [ ] dim_date populated (1461 rows)
  - [ ] dim_order_status populated (8 rows)
  - [ ] dim_geolocation SCD Type 1 hoạt động
  - [ ] dim_customer SCD Type 2 hoạt động (Multicast → Expire + Insert)
- [ ] **Package 3:** `Load_Fact_Customer_Orders.dtsx` – fact tables load thành công
- [ ] **SCD Type 2:** dim_customer có effective_from/to, is_current đúng
- [ ] **SQL Queries:** 3 queries chạy đúng, trả về insight hợp lý
- [ ] **FK Integrity:** Không có orphan foreign keys
- [ ] **Thông báo team:** Server name + Database name gửi cho TV2, TV3

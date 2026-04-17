# 📘 GUIDE_TV3.md – Hướng dẫn chi tiết Thành viên 3

## Domain: Người bán & Vận chuyển & Thanh toán

**Phụ trách:**
- Staging: `stg_sellers`, `stg_orders`, `stg_order_payments`
- Dimensions: `dim_seller` (SCD Type 2)
- Facts: `fact_order_lifecycle` (Upsert), `fact_delivery` (month + year), `fact_payment_trends` (month + year)
- SSIS Packages: 4 packages + Master Package
- Master Package: chịu trách nhiệm chính tích hợp toàn bộ pipeline

---
---

# PHẦN A – CHUẨN BỊ

---

## BƯỚC 0: Kiểm tra môi trường

### 0.1. Xác nhận Database + Schemas đã tồn tại

Mở SSMS (SQL Server Management Studio), kết nối tới SQL Server instance, chạy:

```sql
USE OlistDW;
GO
SELECT name FROM sys.schemas WHERE name IN ('staging', 'gold');
```

Kết quả phải trả về 2 dòng: `staging` và `gold`. Nếu không có, nhờ TV1 chạy script tạo database trước.

### 0.2. Chuẩn bị file CSV

Đảm bảo 3 file CSV sau đã được tải về từ Kaggle và đặt vào **cùng 1 thư mục** (ví dụ `D:\OlistData\`):

| File | Tên đầy đủ |
|---|---|
| Sellers | `olist_sellers_dataset.csv` |
| Orders | `olist_orders_dataset.csv` |
| Payments | `olist_order_payments_dataset.csv` |

> **Quan trọng:** Ghi nhớ đường dẫn thư mục này, sẽ dùng khi tạo Flat File Connection Manager.

### 0.3. Mở SSIS Project trong Visual Studio

1. Mở **Visual Studio** (hoặc SQL Server Data Tools)
2. Mở Solution/Project `OlistDW_ETL` mà team đã tạo
3. Nếu chưa có project:
   - File → New → Project
   - Chọn template: **Integration Services Project**
   - Name: `OlistDW_ETL`
   - Location: chọn thư mục làm việc
   - Click **OK**

### 0.4. Tạo 5 SSIS Packages

Trong **Solution Explorer** (panel bên phải):
1. Chuột phải vào thư mục **SSIS Packages**
2. Chọn **New SSIS Package**
3. Đổi tên (chuột phải → Rename) thành:
   - `Extract_Seller_Order_Payment.dtsx`
4. Lặp lại tạo thêm 4 packages:
   - `Load_Dim_Seller.dtsx`
   - `Load_Fact_Lifecycle.dtsx`
   - `Load_Fact_Delivery_Payment.dtsx`
   - `Master_ETL.dtsx`

### 0.5. Tạo OLE DB Connection Manager (dùng chung)

Connection Manager là cầu nối giữa SSIS và SQL Server, tạo 1 lần dùng cho tất cả packages:

1. Trong Visual Studio, ở panel dưới cùng có vùng **Connection Managers**
2. Chuột phải vào vùng trống → **New OLE DB Connection...**
3. Trong dialog **Configure OLE DB Connection Manager**:
   - Click **New...**
   - Server name: nhập tên SQL Server (vd: `localhost` hoặc `.\SQLEXPRESS` hoặc `DESKTOP-XXXX\SQLEXPRESS`)
   - Authentication: **Windows Authentication** (hoặc SQL Server Authentication nếu dùng user/pass)
   - Select or enter a database name: chọn **OlistDW**
   - Click **Test Connection** → phải hiện "Test connection succeeded"
   - Click **OK** → Click **OK**
4. Connection vừa tạo xuất hiện ở panel dưới, đổi tên thành `OlistDW_OLEDB`:
   - Chuột phải connection → **Rename** → gõ `OlistDW_OLEDB`

> **Tip:** Nếu muốn connection dùng chung cho tất cả packages trong project, chuột phải connection → **Convert to Project Connection**.

---
---

# PHẦN B – TẠO BẢNG TRONG SQL SERVER

---

## BƯỚC 1: Tạo Staging Tables

Mở SSMS → New Query (đảm bảo đang kết nối đúng database `OlistDW`) → paste và chạy từng block:

### 1.1. staging.stg_sellers

```sql
USE OlistDW;
GO

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

-- Verify
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'stg_sellers';
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

**Kiểm tra:** Expand database OlistDW → Tables → phải thấy 3 bảng trong schema `staging`.

---

## BƯỚC 2: Tạo Dimension Table

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
    -- SCD Type 2 columns: theo dõi lịch sử thay đổi
    effective_from   DATE              NOT NULL DEFAULT '1900-01-01',
    effective_to     DATE              NOT NULL DEFAULT '9999-12-31',
    is_current       BIT               NOT NULL DEFAULT 1,
    -- Foreign Key tới dim_geolocation (do TV1 tạo)
    CONSTRAINT FK_dim_seller_geo FOREIGN KEY (geo_key)
        REFERENCES gold.dim_geolocation(geo_key)
);
GO

-- Index giúp Lookup nhanh hơn trong SSIS
CREATE INDEX IX_dim_seller_id ON gold.dim_seller(seller_id, is_current);
GO
```

> **Nếu lỗi FK:** Bảng `gold.dim_geolocation` chưa tồn tại → nhờ TV1 tạo trước, hoặc tạm bỏ dòng CONSTRAINT FK, thêm sau.

---

## BƯỚC 3: Tạo Fact Tables

### 3.1. gold.fact_order_lifecycle

```sql
IF OBJECT_ID('gold.fact_order_lifecycle', 'U') IS NOT NULL
    DROP TABLE gold.fact_order_lifecycle;
GO

CREATE TABLE gold.fact_order_lifecycle (
    fact_lifecycle_id       INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    order_id                VARCHAR(50)        NOT NULL,
    customer_key            INT                NULL,
    seller_key              INT                NULL,
    order_date              DATE               NULL,
    approved_date           DATE               NULL,
    delivered_date          DATE               NULL,
    estimated_delivery_date DATE               NULL,
    days_to_approve         INT                NULL,
    days_to_delivery        INT                NULL,
    is_delayed              BIT                NULL,
    order_status            VARCHAR(30)        NULL,
    CONSTRAINT FK_fl_customer FOREIGN KEY (customer_key)
        REFERENCES gold.dim_customer(customer_key),
    CONSTRAINT FK_fl_seller FOREIGN KEY (seller_key)
        REFERENCES gold.dim_seller(seller_key)
);
GO

CREATE UNIQUE INDEX UX_fact_lifecycle_order ON gold.fact_order_lifecycle(order_id);
GO
```

### 3.2. gold.fact_delivery (monthly)

```sql
IF OBJECT_ID('gold.fact_delivery', 'U') IS NOT NULL
    DROP TABLE gold.fact_delivery;
GO

CREATE TABLE gold.fact_delivery (
    seller_key             INT           NOT NULL,
    date_key               INT           NOT NULL,
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
    year_key               INT           NOT NULL,
    total_orders_delivered  INT           NOT NULL DEFAULT 0,
    on_time_orders         INT           NOT NULL DEFAULT 0,
    on_time_rate           DECIMAL(5,4)  NULL,
    CONSTRAINT PK_fact_delivery_year PRIMARY KEY (seller_key, year_key)
);
GO
```

### 3.4. gold.fact_payment_trends (monthly)

```sql
IF OBJECT_ID('gold.fact_payment_trends', 'U') IS NOT NULL
    DROP TABLE gold.fact_payment_trends;
GO

CREATE TABLE gold.fact_payment_trends (
    payment_type        VARCHAR(30)    NOT NULL,
    date_key            INT            NOT NULL,
    total_payment_value DECIMAL(14,2)  NOT NULL DEFAULT 0,
    transaction_count   INT            NOT NULL DEFAULT 0,
    order_count         INT            NOT NULL DEFAULT 0,
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
    year_key            INT            NOT NULL,
    total_payment_value DECIMAL(14,2)  NOT NULL DEFAULT 0,
    transaction_count   INT            NOT NULL DEFAULT 0,
    order_count         INT            NOT NULL DEFAULT 0,
    CONSTRAINT PK_fact_payment_trends_year PRIMARY KEY (payment_type, year_key)
);
GO
```

**Kiểm tra cuối cùng:**
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

## BƯỚC 4: Xây dựng `Extract_Seller_Order_Payment.dtsx`

### 4.1. Mở Package

Double-click vào `Extract_Seller_Order_Payment.dtsx` trong Solution Explorer → mở ra tab **Control Flow**.

### 4.2. Thêm Execute SQL Task – Truncate Staging

**Mục đích:** Xóa toàn bộ dữ liệu cũ trong staging trước khi load mới (chiến lược Truncate-Reload).

1. Trong **SSIS Toolbox** (panel bên trái), tìm section **Favorites** hoặc **Other Tasks**
2. Kéo **Execute SQL Task** vào vùng Control Flow
3. Click vào task vừa kéo → nhìn panel Properties (bên phải) → đổi **Name** thành: `EST - Truncate Staging Tables`
4. Double-click vào task → mở **Execute SQL Task Editor**:

| Thuộc tính | Giá trị |
|---|---|
| Name | `EST - Truncate Staging Tables` |
| Connection | Chọn `OlistDW_OLEDB` từ dropdown |
| SQLSourceType | `Direct input` |
| SQLStatement | Click vào ô `...` bên phải, paste SQL dưới đây |

```sql
TRUNCATE TABLE staging.stg_sellers;
TRUNCATE TABLE staging.stg_orders;
TRUNCATE TABLE staging.stg_order_payments;
```

5. Click **OK**

> **Nếu Connection dropdown trống:** Quay lại panel Connection Managers ở dưới, kiểm tra đã tạo `OlistDW_OLEDB` chưa. Nếu tạo ở package khác, cần tạo lại hoặc dùng Project Connection.

---

### 4.3. Data Flow Task 1 – Load Sellers

#### 4.3.1. Thêm Data Flow Task

1. Từ Toolbox, kéo **Data Flow Task** vào Control Flow
2. Đổi tên thành: `DFT - Load Sellers`
3. **Nối Precedence Constraint:** Click vào task `EST - Truncate Staging Tables` → thấy mũi tên xanh nhỏ xuất hiện ở cạnh dưới → kéo mũi tên đó xuống `DFT - Load Sellers` → thả ra → xuất hiện đường mũi tên xanh nối 2 task
4. Double-click vào `DFT - Load Sellers` → chuyển sang tab **Data Flow**

#### 4.3.2. Flat File Source

1. Trong Toolbox (bây giờ hiện các component Data Flow), tìm section **Other Sources**
2. Kéo **Flat File Source** vào canvas Data Flow
3. Double-click vào Flat File Source → mở editor
4. Click **New...** để tạo Flat File Connection Manager
5. Trong **Flat File Connection Manager Editor**:

**Tab General:**

| Thuộc tính | Giá trị |
|---|---|
| Connection manager name | `FF_Sellers` |
| File name | Click Browse → tìm đến `D:\OlistData\olist_sellers_dataset.csv` |
| Locale | `English (United States)` |
| Code page | `65001 (UTF-8)` |
| Format | `Delimited` |
| Text qualifier | `"` (dấu ngoặc kép) |
| Header row delimiter | `{CR}{LF}` |
| Header rows to skip | `0` |
| ✅ Column names in the first data row | Check |

**Tab Columns:**
- Preview hiện ra bảng dữ liệu → kiểm tra 4 cột: `seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state`
- Row delimiter: `{CR}{LF}`
- Column delimiter: `Comma {,}`
- Nếu dữ liệu hiển thị đúng → OK

**Tab Advanced:**
Chỉnh lại DataType và OutputColumnWidth cho từng cột:

| Column | DataType | OutputColumnWidth |
|---|---|---|
| seller_id | string [DT_STR] | 50 |
| seller_zip_code_prefix | string [DT_STR] | 10 |
| seller_city | Unicode string [DT_WSTR] | 100 |
| seller_state | string [DT_STR] | 5 |

> **Cách chỉnh:** Click vào tên cột ở panel trái → panel phải hiện Properties → sửa DataType và OutputColumnWidth

6. Click **OK** để đóng Connection Manager → Click **OK** đóng Flat File Source

#### 4.3.3. OLE DB Destination

1. Kéo **OLE DB Destination** từ Toolbox vào canvas (section **Other Destinations**)
2. **Nối:** Click vào Flat File Source → kéo mũi tên xanh xuống OLE DB Destination
3. Double-click OLE DB Destination:

**Tab Connection Manager:**

| Thuộc tính | Giá trị |
|---|---|
| OLE DB connection manager | `OlistDW_OLEDB` |
| Data access mode | `Table or view - fast load` |
| Name of the table or view | Chọn `[staging].[stg_sellers]` từ dropdown |
| Keep identity | Unchecked |
| Table lock | Checked ✅ |
| Check constraints | Unchecked |

**Tab Mappings:**
SSIS tự động map nếu tên cột giống nhau. Kiểm tra:

| Input Column | Destination Column |
|---|---|
| seller_id | seller_id |
| seller_zip_code_prefix | seller_zip_code_prefix |
| seller_city | seller_city |
| seller_state | seller_state |

Nếu có cột bị `<ignore>`, click vào dropdown và chọn cột đích đúng.

4. Click **OK**

> **Lưu ý:** Ở đây chúng ta KHÔNG cần Data Conversion vì đã set đúng DataType ở Flat File Connection Manager. Nếu gặp lỗi type mismatch, thêm **Data Conversion** giữa Source và Destination (xem bước 4.4.3 bên dưới).

---

### 4.4. Data Flow Task 2 – Load Orders (phức tạp nhất do xử lý NULL timestamps)

#### 4.4.1. Quay lại Control Flow

Click tab **Control Flow** ở trên cùng.

#### 4.4.2. Thêm Data Flow Task

1. Kéo **Data Flow Task** mới → đổi tên `DFT - Load Orders`
2. **Nối:** Kéo mũi tên xanh từ `EST - Truncate Staging Tables` xuống `DFT - Load Orders`

> Bây giờ 2 Data Flow Tasks (`Load Sellers` và `Load Orders`) đều nối từ Truncate task → chúng sẽ chạy **song song** khi package execute.

3. Double-click `DFT - Load Orders` → vào Data Flow

#### 4.4.3. Flat File Source cho Orders

1. Kéo **Flat File Source** vào canvas
2. Double-click → New → tạo Connection Manager mới:

**Tab General:**

| Thuộc tính | Giá trị |
|---|---|
| Connection manager name | `FF_Orders` |
| File name | `D:\OlistData\olist_orders_dataset.csv` |
| Format | Delimited |
| ✅ Column names in the first data row | Check |

**Tab Advanced – CỰC KỲ QUAN TRỌNG:**

File `olist_orders_dataset.csv` có 8 cột. Một số cột timestamp có giá trị rỗng (empty string). Nếu set DataType là DT_DBTIMESTAMP, SSIS sẽ **lỗi ngay** khi gặp giá trị rỗng.

**Giải pháp:** Set TẤT CẢ cột timestamp thành **string**, rồi convert an toàn ở Derived Column.

Cấu hình từng cột:

| Column Name | DataType | OutputColumnWidth |
|---|---|---|
| order_id | string [DT_STR] | 50 |
| customer_id | string [DT_STR] | 50 |
| order_status | string [DT_STR] | 30 |
| order_purchase_timestamp | **string [DT_STR]** | **30** |
| order_approved_at | **string [DT_STR]** | **30** |
| order_delivered_carrier_date | **string [DT_STR]** | **30** |
| order_delivered_customer_date | **string [DT_STR]** | **30** |
| order_estimated_delivery_date | **string [DT_STR]** | **30** |

3. Click **OK** → Click **OK**

#### 4.4.4. Derived Column – Convert timestamps an toàn

1. Kéo **Derived Column** từ Toolbox (section **Common**)
2. Nối: Flat File Source → Derived Column
3. Double-click Derived Column
4. Thêm 5 cột mới (chọn **\<add as new column\>** ở cột "Derived Column"):

**Cột 1:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `dt_purchase` |
| Derived Column | `<add as new column>` |
| Expression | `LEN(TRIM(order_purchase_timestamp)) > 0 ? (DT_DBTIMESTAMP)order_purchase_timestamp : NULL(DT_DBTIMESTAMP)` |

**Cột 2:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `dt_approved` |
| Expression | `LEN(TRIM(order_approved_at)) > 0 ? (DT_DBTIMESTAMP)order_approved_at : NULL(DT_DBTIMESTAMP)` |

**Cột 3:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `dt_carrier` |
| Expression | `LEN(TRIM(order_delivered_carrier_date)) > 0 ? (DT_DBTIMESTAMP)order_delivered_carrier_date : NULL(DT_DBTIMESTAMP)` |

**Cột 4:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `dt_customer` |
| Expression | `LEN(TRIM(order_delivered_customer_date)) > 0 ? (DT_DBTIMESTAMP)order_delivered_customer_date : NULL(DT_DBTIMESTAMP)` |

**Cột 5:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `dt_estimated` |
| Expression | `LEN(TRIM(order_estimated_delivery_date)) > 0 ? (DT_DBTIMESTAMP)order_estimated_delivery_date : NULL(DT_DBTIMESTAMP)` |

> **Giải thích logic:** `LEN(TRIM(...)) > 0` kiểm tra chuỗi không rỗng → nếu có giá trị thì cast sang DATETIME; nếu rỗng thì trả NULL.

5. Click **OK**

#### 4.4.5. OLE DB Destination cho Orders

1. Kéo **OLE DB Destination** → nối từ Derived Column
2. Double-click:
   - Connection: `OlistDW_OLEDB`
   - Table: `[staging].[stg_orders]`
3. Tab **Mappings** – map thủ công:

| Input Column | Destination Column |
|---|---|
| order_id | order_id |
| customer_id | customer_id |
| order_status | order_status |
| dt_purchase | order_purchase_timestamp |
| dt_approved | order_approved_at |
| dt_carrier | order_delivered_carrier_date |
| dt_customer | order_delivered_customer_date |
| dt_estimated | order_estimated_delivery_date |

> **Quan trọng:** Map các cột `dt_*` (output từ Derived Column) chứ KHÔNG map các cột string gốc.

4. Click **OK**

---

### 4.5. Data Flow Task 3 – Load Payments

#### 4.5.1. Quay lại Control Flow → kéo Data Flow Task mới → đổi tên `DFT - Load Payments`

#### 4.5.2. Nối từ Truncate task (chạy song song với 2 DFT kia)

#### 4.5.3. Vào Data Flow → thiết lập:

**Flat File Source:**
- Connection Manager mới: `FF_Payments` → file `olist_order_payments_dataset.csv`
- Tab Advanced:

| Column Name | DataType | OutputColumnWidth |
|---|---|---|
| order_id | string [DT_STR] | 50 |
| payment_sequential | string [DT_STR] | 10 |
| payment_type | string [DT_STR] | 30 |
| payment_installments | string [DT_STR] | 10 |
| payment_value | string [DT_STR] | 20 |

> **Lưu ý:** Set tất cả thành string rồi dùng Data Conversion để chuyển kiểu an toàn.

**Data Conversion:**
1. Kéo **Data Conversion** từ Toolbox → nối từ Flat File Source
2. Double-click → cấu hình:

| Input Column | Output Alias | Data Type | Precision | Scale |
|---|---|---|---|---|
| ✅ order_id | cv_order_id | string [DT_STR], 50 | — | — |
| ✅ payment_sequential | cv_seq | four-byte signed integer [DT_I4] | — | — |
| ✅ payment_type | cv_type | string [DT_STR], 30 | — | — |
| ✅ payment_installments | cv_install | four-byte signed integer [DT_I4] | — | — |
| ✅ payment_value | cv_value | numeric [DT_NUMERIC] | 10 | 2 |

3. Click **OK**

> **Cách thao tác:** Tick checkbox cột cần convert ở panel trên → dòng mới xuất hiện ở panel dưới → sửa Output Alias và Data Type tại dòng đó.

**OLE DB Destination:**
1. Kéo → nối từ Data Conversion
2. Table: `[staging].[stg_order_payments]`
3. Mappings:

| Input Column | Destination Column |
|---|---|
| cv_order_id | order_id |
| cv_seq | payment_sequential |
| cv_type | payment_type |
| cv_install | payment_installments |
| cv_value | payment_value |

---

### 4.6. Kiểm tra Control Flow hoàn chỉnh

Quay lại tab **Control Flow**. Layout phải trông như sau:

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
         │Sellers   │ │Orders    │ │Payments  │
         └──────────┘ └──────────┘ └──────────┘
```

3 Data Flow Tasks đều nối từ Truncate → chạy **song song** khi Truncate thành công.

### 4.7. Chạy test Package

1. **Chuột phải** vào `Extract_Seller_Order_Payment.dtsx` trong Solution Explorer → **Execute Package**
   (hoặc click vào package rồi nhấn **F5**, hoặc click nút ▶ Start trên toolbar)
2. Quan sát:
   - Truncate task chuyển **vàng** (đang chạy) → **xanh** (thành công)
   - 3 Data Flow Tasks chuyển vàng (cùng lúc) → xanh
   - Nếu task nào chuyển **đỏ** → double-click để xem lỗi ở tab Progress
3. Click **Stop Debugging** (nút ■ trên toolbar) để thoát debug mode

4. Mở SSMS → verify:

```sql
SELECT 'stg_sellers'        AS tbl, COUNT(*) AS rows_loaded FROM staging.stg_sellers
UNION ALL
SELECT 'stg_orders',               COUNT(*) FROM staging.stg_orders
UNION ALL
SELECT 'stg_order_payments',       COUNT(*) FROM staging.stg_order_payments;
```

Kết quả kỳ vọng:

| tbl | rows_loaded |
|---|---|
| stg_sellers | ~3,095 |
| stg_orders | ~99,441 |
| stg_order_payments | ~103,886 |

> **Nếu stg_orders = 0 hoặc lỗi:** Kiểm tra lại Derived Column expression – phần timestamp conversion là nguyên nhân phổ biến nhất.

---
---

# PHẦN D – SSIS PACKAGE 2: LOAD DIMENSION (SCD TYPE 2)

---

## BƯỚC 5: Xây dựng `Load_Dim_Seller.dtsx`

### 5.1. Điều kiện tiên quyết

⚠️ Bảng `gold.dim_geolocation` (TV1) **PHẢI có dữ liệu trước**. Kiểm tra:

```sql
SELECT COUNT(*) FROM gold.dim_geolocation;
-- Phải > 0 (kỳ vọng ~19,000 rows)
```

Nếu = 0, nhờ TV1 chạy packages Extract + Load Dimension trước.

### 5.2. Mở Package

Double-click `Load_Dim_Seller.dtsx` → tab Control Flow.

### 5.3. Thêm Data Flow Task

1. Kéo **Data Flow Task** vào Control Flow
2. Đổi tên: `DFT - Load dim_seller (SCD Type 2)`
3. Double-click để vào **Data Flow**

### 5.4. Component 1 – OLE DB Source

**Mục đích:** Đọc dữ liệu seller từ staging.

1. Kéo **OLE DB Source** từ Toolbox
2. Double-click → cấu hình:

| Thuộc tính | Giá trị |
|---|---|
| OLE DB connection manager | `OlistDW_OLEDB` |
| Data access mode | `SQL command` |
| SQL command text | Paste SQL bên dưới |

```sql
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM staging.stg_sellers;
```

3. Click **Preview...** → kiểm tra dữ liệu hiện ra đúng
4. Click **OK**

### 5.5. Component 2 – Lookup dim_geolocation (lấy geo_key)

**Mục đích:** Tra cứu `geo_key` từ `dim_geolocation` dựa trên `zip_code_prefix`. Nếu seller có zip code không tồn tại trong dim_geolocation, ta vẫn muốn insert seller đó (với geo_key = NULL).

1. Kéo **Lookup** từ Toolbox → nối mũi tên xanh từ OLE DB Source
2. Double-click Lookup → mở editor:

**Tab General:**

| Thuộc tính | Giá trị |
|---|---|
| Cache mode | `Full cache` |
| Connection type | `OLE DB connection manager` |
| Specify how to handle rows with no matching entries | **Redirect rows to no match output** ← QUAN TRỌNG |

> `Redirect rows to no match output` = SSIS không báo lỗi khi không tìm thấy, mà chuyển dòng đó sang output riêng.

**Tab Connection:**

| Thuộc tính | Giá trị |
|---|---|
| OLE DB connection manager | `OlistDW_OLEDB` |
| Use results of an SQL query | Chọn radio button này |
| SQL query | Paste SQL dưới đây |

```sql
SELECT geo_key, zip_code_prefix
FROM gold.dim_geolocation;
```

**Tab Columns:**
- Panel trái (Available Input Columns): kéo `seller_zip_code_prefix`
- Panel phải (Available Lookup Columns): thả lên `zip_code_prefix`
- → Tạo đường nối giữa 2 cột (join condition)
- Tick ✅ `geo_key` ở panel phải → Output Alias: `lkp_geo_key`

3. Click **OK**

**Xử lý No Match Output:**

Seller có zip code không tìm thấy → ta muốn vẫn giữ lại, chỉ set `lkp_geo_key = NULL`:

4. Kéo **Derived Column** mới vào canvas
5. Click vào **Lookup** component → thấy 2 mũi tên output:
   - Mũi tên xanh = **Match Output** (mặc định)
   - Cần tạo nối cho **No Match Output**: Click mũi tên xanh → kéo xuống Derived Column mới → dialog hỏi output → chọn **Lookup No Match Output**
6. Double-click Derived Column:
   - Thêm cột mới: `lkp_geo_key` | Expression: `NULL(DT_I4)` | chọn `<add as new column>`
7. Click **OK**

8. Kéo **Union All** vào canvas
9. Nối: Lookup (Match Output, mũi tên xanh còn lại) → Union All
10. Nối: Derived Column (No Match) → Union All
11. Double-click Union All → map cột cho khớp:

| Union All Output | Lookup Match Input | Derived Column Input |
|---|---|---|
| seller_id | seller_id | seller_id |
| seller_zip_code_prefix | seller_zip_code_prefix | seller_zip_code_prefix |
| seller_city | seller_city | seller_city |
| seller_state | seller_state | seller_state |
| lkp_geo_key | lkp_geo_key | lkp_geo_key |

### 5.6. Component 3 – Derived Column (thêm seller_region)

**Mục đích:** Tính `seller_region` dựa trên state (phân vùng Brazil).

1. Kéo **Derived Column** → nối từ Union All
2. Double-click → thêm cột mới:

| Derived Column Name | Expression |
|---|---|
| `seller_region` | *(xem expression dưới đây)* |

```
(DT_STR,20,1252)(
    seller_state == "SP" || seller_state == "RJ" || seller_state == "MG" || seller_state == "ES"
    ? "Sudeste"
    : seller_state == "PR" || seller_state == "SC" || seller_state == "RS"
    ? "Sul"
    : seller_state == "BA" || seller_state == "PE" || seller_state == "CE" || seller_state == "MA" || seller_state == "PB" || seller_state == "RN" || seller_state == "AL" || seller_state == "PI" || seller_state == "SE"
    ? "Nordeste"
    : seller_state == "AM" || seller_state == "PA" || seller_state == "RO" || seller_state == "TO" || seller_state == "AC" || seller_state == "AP" || seller_state == "RR"
    ? "Norte"
    : seller_state == "GO" || seller_state == "MT" || seller_state == "MS" || seller_state == "DF"
    ? "Centro-Oeste"
    : "Unknown"
)
```

> **Tip copy expression:** Copy toàn bộ expression trên, paste vào ô Expression trong Derived Column editor. Nếu gặp lỗi syntax → kiểm tra không có line break thừa.

3. Click **OK**

### 5.7. Component 4 – Lookup dim_seller (kiểm tra đã tồn tại chưa)

**Mục đích:** So sánh seller_id trong staging với dim_seller hiện tại → phân biệt New vs Existing.

1. Kéo **Lookup** mới → nối từ Derived Column (seller_region)
2. Double-click:

**Tab General:**
- Cache mode: `Full cache`
- Specify how to handle rows with no matching entries: **Redirect rows to no match output**

**Tab Connection:**
- Use results of an SQL query:

```sql
SELECT
    seller_key,
    seller_id,
    city,
    state,
    geo_key
FROM gold.dim_seller
WHERE is_current = 1;
```

**Tab Columns:**
- Join: `seller_id` (input) → `seller_id` (lookup)
- Tick Output:
  - `seller_key` → Output Alias: `existing_seller_key`
  - `city` → Output Alias: `existing_city`
  - `state` → Output Alias: `existing_state`

3. Click **OK**

### 5.8. Xử lý No Match Output → INSERT seller mới

**Luồng No Match = seller chưa có trong dim → INSERT bản ghi mới.**

#### 5.8.1. Derived Column cho SCD columns (No Match path)

1. Kéo **Derived Column** mới
2. Nối: Lookup dim_seller → **Lookup No Match Output** → Derived Column
3. Double-click, thêm 3 cột:

| Name | Expression | Data Type |
|---|---|---|
| `scd_effective_from` | `(DT_DBDATE)GETDATE()` | date |
| `scd_effective_to` | `(DT_DBDATE)"9999-12-31"` | date |
| `scd_is_current` | `(DT_BOOL)TRUE` | boolean |

4. Click **OK**

#### 5.8.2. OLE DB Destination (INSERT mới)

1. Kéo **OLE DB Destination** → nối từ Derived Column (SCD columns)
2. Double-click:
   - Connection: `OlistDW_OLEDB`
   - Table: `[gold].[dim_seller]`
3. Tab Mappings:

| Input Column | Destination Column |
|---|---|
| seller_id | seller_id |
| seller_city | city |
| seller_state | state |
| lkp_geo_key | geo_key |
| seller_region | seller_region |
| scd_effective_from | effective_from |
| scd_effective_to | effective_to |
| scd_is_current | is_current |

> **Không map `seller_key`** vì nó là IDENTITY (auto-increment).

4. Click **OK**

### 5.9. Xử lý Match Output → Kiểm tra có thay đổi không

**Luồng Match = seller đã tồn tại → cần kiểm tra city/state có thay đổi không.**

#### 5.9.1. Conditional Split

1. Kéo **Conditional Split** → nối từ Lookup dim_seller (**Lookup Match Output**, mũi tên xanh)
2. Double-click:

| Output Name | Condition |
|---|---|
| `Is_Changed` | `seller_city != existing_city \|\| seller_state != existing_state` |
| (Default output name) | `Unchanged` |

3. Click **OK**

Giải thích: Nếu city hoặc state thay đổi → output `Is_Changed`. Còn lại → `Unchanged` (bỏ qua, không làm gì).

#### 5.9.2. Multicast (nhân đôi dòng Changed)

**Tại sao cần Multicast?** Vì với SCD Type 2, mỗi dòng thay đổi cần thực hiện **2 hành động**:
1. UPDATE bản ghi cũ: set `is_current = 0`, `effective_to = today`
2. INSERT bản ghi mới: với dữ liệu mới, `is_current = 1`

SSIS không cho phép 1 output nối tới 2 destination → dùng Multicast để clone dòng.

1. Kéo **Multicast** → nối từ Conditional Split output `Is_Changed`
2. (Không cần cấu hình, Multicast tự tạo 2 outputs giống nhau)

#### 5.9.3. Nhánh 1: OLE DB Command (Expire bản ghi cũ)

1. Kéo **OLE DB Command** từ Toolbox (section **Other Transforms**) → nối từ Multicast (Output 0)
2. Double-click:

**Tab Connection Managers:**
- Connection Manager: `OlistDW_OLEDB`

**Tab Component Properties:**
- SqlCommand: paste SQL dưới đây:

```sql
UPDATE gold.dim_seller
SET is_current = 0,
    effective_to = CAST(GETDATE() AS DATE)
WHERE seller_key = ?;
```

**Tab Column Mappings:**

| Input Column | Destination Column |
|---|---|
| existing_seller_key | Param_0 |

3. Click **OK**

> **Giải thích:** `?` trong SQL tương ứng với `Param_0`. Truyền `existing_seller_key` vào → UPDATE đúng bản ghi cũ.

#### 5.9.4. Nhánh 2: INSERT bản ghi mới (version mới)

1. Kéo **Derived Column** → nối từ Multicast (Output 1)
2. Thêm 3 cột SCD (tương tự bước 5.8.1):

| Name | Expression |
|---|---|
| `new_effective_from` | `(DT_DBDATE)GETDATE()` |
| `new_effective_to` | `(DT_DBDATE)"9999-12-31"` |
| `new_is_current` | `(DT_BOOL)TRUE` |

3. Kéo **OLE DB Destination** → nối từ Derived Column
4. Table: `[gold].[dim_seller]`
5. Mappings:

| Input Column | Destination Column |
|---|---|
| seller_id | seller_id |
| seller_city | city |
| seller_state | state |
| lkp_geo_key | geo_key |
| seller_region | seller_region |
| new_effective_from | effective_from |
| new_effective_to | effective_to |
| new_is_current | is_current |

> **Lưu ý đặt tên:** Nếu có 2 OLE DB Destinations cùng ghi vào dim_seller, đổi tên:
> - `OLE DB Destination - Insert New Seller`
> - `OLE DB Destination - Insert Changed Version`

### 5.10. Tổng quan Data Flow hoàn chỉnh

```
OLE DB Source (stg_sellers)
        │
        ▼
Lookup dim_geolocation ─── No Match ──→ Derived Column (geo_key=NULL)
        │ (Match)                                │
        │                                        │
        └───────────────► Union All ◄────────────┘
                              │
                              ▼
                  Derived Column (seller_region)
                              │
                              ▼
               Lookup dim_seller (is_current=1)
               ┌──────────────┴──────────────┐
               │ (Match)                      │ (No Match)
               ▼                              ▼
        Conditional Split              Derived Column (SCD cols)
        ┌──────┴──────┐                      │
        │             │                      ▼
   Is_Changed    Unchanged          OLE DB Dest (INSERT NEW)
        │          (bỏ qua)
        ▼
    Multicast
    ┌────┴────┐
    ▼         ▼
OLE DB     Derived Column (SCD cols)
Command         │
(EXPIRE)        ▼
           OLE DB Dest (INSERT NEW VERSION)
```

### 5.11. Chạy test

1. Execute Package (F5)
2. Kiểm tra tất cả components chuyển xanh
3. Verify:

```sql
-- Tổng sellers
SELECT COUNT(*) AS total_rows FROM gold.dim_seller;
-- Expected: ~3,095

-- Tất cả đều is_current = 1 (lần đầu load)
SELECT is_current, COUNT(*) AS cnt
FROM gold.dim_seller
GROUP BY is_current;
-- Expected: is_current=1 → ~3,095

-- Kiểm tra seller_region có đúng không
SELECT seller_region, COUNT(*) AS cnt
FROM gold.dim_seller
GROUP BY seller_region
ORDER BY cnt DESC;
-- Kỳ vọng: Sudeste nhiều nhất (SP là bang lớn nhất)
```

---
---

# PHẦN E – SSIS PACKAGE 3: LOAD FACT_ORDER_LIFECYCLE (UPSERT)

---

## BƯỚC 6: Xây dựng `Load_Fact_Lifecycle.dtsx`

### 6.1. Điều kiện tiên quyết

- ✅ `staging.stg_orders` đã có dữ liệu (bạn vừa load ở Bước 4)
- ✅ `staging.stg_order_items` đã có dữ liệu (TV2 phải chạy Extract trước)
- ✅ `gold.dim_customer` đã có dữ liệu (TV1)
- ✅ `gold.dim_seller` đã có dữ liệu (bạn vừa load ở Bước 5)

Kiểm tra:
```sql
SELECT
    (SELECT COUNT(*) FROM staging.stg_orders) AS stg_orders,
    (SELECT COUNT(*) FROM staging.stg_order_items) AS stg_order_items,
    (SELECT COUNT(*) FROM gold.dim_customer WHERE is_current = 1) AS dim_customer,
    (SELECT COUNT(*) FROM gold.dim_seller WHERE is_current = 1) AS dim_seller;
```

Tất cả phải > 0.

### 6.2. Mở Package → thêm Data Flow Task

1. Double-click `Load_Fact_Lifecycle.dtsx`
2. Kéo **Data Flow Task** → đổi tên `DFT - Load fact_order_lifecycle (Upsert)`
3. Double-click vào Data Flow

### 6.3. Component 1 – OLE DB Source (đọc + tính toán sẵn bằng SQL)

**Lựa chọn đơn giản:** Tính tất cả derived columns (days_to_approve, days_to_delivery, is_delayed) **trong SQL** thay vì dùng Derived Column SSIS (dễ lỗi với DATEDIFF).

1. Kéo **OLE DB Source**
2. Double-click:
   - Connection: `OlistDW_OLEDB`
   - Data access mode: `SQL command`
   - SQL command text:

```sql
SELECT
    o.order_id,
    o.customer_id,
    -- Lấy seller_id từ order_item đầu tiên
    (SELECT TOP 1 oi.seller_id
     FROM staging.stg_order_items oi
     WHERE oi.order_id = o.order_id) AS seller_id,
    o.order_status,
    CAST(o.order_purchase_timestamp AS DATE)       AS order_date,
    CAST(o.order_approved_at AS DATE)              AS approved_date,
    CAST(o.order_delivered_customer_date AS DATE)   AS delivered_date,
    CAST(o.order_estimated_delivery_date AS DATE)   AS estimated_delivery_date,
    -- Derived measures tính sẵn trong SQL
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_approved_at)
        AS days_to_approve,
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date)
        AS days_to_delivery,
    CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1
        WHEN o.order_delivered_customer_date IS NULL THEN NULL
        ELSE 0
    END AS is_delayed
FROM staging.stg_orders o;
```

3. Click **Preview** → verify dữ liệu → Click **OK**

### 6.4. Component 2 – Lookup dim_customer (lấy customer_key)

1. Kéo **Lookup** → nối từ OLE DB Source
2. Double-click:

**Tab General:** Redirect rows to no match output

**Tab Connection:**
```sql
SELECT customer_key, customer_id
FROM gold.dim_customer
WHERE is_current = 1;
```

**Tab Columns:**
- Join: `customer_id` (input) → `customer_id` (lookup)
- Output: ✅ `customer_key` → Alias: `lkp_customer_key`

3. Click **OK**

**Xử lý No Match:** Tương tự Bước 5.5 – dùng Derived Column (set `lkp_customer_key = NULL(DT_I4)`) + Union All để merge.

### 6.5. Component 3 – Lookup dim_seller (lấy seller_key)

1. Kéo **Lookup** → nối từ Union All (sau customer lookup)
2. Double-click:

**Tab General:** Redirect rows to no match output

**Tab Connection:**
```sql
SELECT seller_key, seller_id
FROM gold.dim_seller
WHERE is_current = 1;
```

**Tab Columns:**
- Join: `seller_id` → `seller_id`
- Output: ✅ `seller_key` → Alias: `lkp_seller_key`

3. Click **OK**

**Xử lý No Match:** Tương tự – Derived Column + Union All.

### 6.6. Component 4 – Lookup fact_order_lifecycle (Upsert check)

**Mục đích:** Kiểm tra order_id đã có trong fact_order_lifecycle chưa → New (insert) hoặc Existing (có thể update).

1. Kéo **Lookup** → nối từ Union All (sau seller lookup)
2. Double-click:

**Tab General:** Redirect rows to no match output

**Tab Connection:**
```sql
SELECT
    fact_lifecycle_id,
    order_id,
    order_status  AS existing_status,
    delivered_date AS existing_delivered
FROM gold.fact_order_lifecycle;
```

**Tab Columns:**
- Join: `order_id` → `order_id`
- Output:
  - ✅ `fact_lifecycle_id` → Alias: `existing_lifecycle_id`
  - ✅ `existing_status`
  - ✅ `existing_delivered`

3. Click **OK**

### 6.7. No Match → OLE DB Destination (INSERT đơn hàng mới)

1. Kéo **OLE DB Destination** → nối từ Lookup (**No Match Output**)
2. Double-click:
   - Table: `[gold].[fact_order_lifecycle]`
3. Mappings:

| Input Column | Destination Column |
|---|---|
| order_id | order_id |
| lkp_customer_key | customer_key |
| lkp_seller_key | seller_key |
| order_date | order_date |
| approved_date | approved_date |
| delivered_date | delivered_date |
| estimated_delivery_date | estimated_delivery_date |
| days_to_approve | days_to_approve |
| days_to_delivery | days_to_delivery |
| is_delayed | is_delayed |
| order_status | order_status |

4. Click **OK**

### 6.8. Match → Conditional Split → OLE DB Command (UPDATE)

#### 6.8.1. Conditional Split

1. Kéo **Conditional Split** → nối từ Lookup (**Match Output**)
2. Double-click:

| Output Name | Condition |
|---|---|
| `Is_Updated` | `order_status != existing_status \|\| (!ISNULL(delivered_date) && ISNULL(existing_delivered))` |
| Default | `Unchanged` |

3. Click **OK**

> **Giải thích condition:** Update nếu status thay đổi HOẶC delivered_date mới xuất hiện (trước đó là NULL, giờ có giá trị).

#### 6.8.2. OLE DB Command (UPDATE)

1. Kéo **OLE DB Command** → nối từ Conditional Split output `Is_Updated`
2. Double-click:

**Tab Connection Managers:** `OlistDW_OLEDB`

**Tab Component Properties:**
- SqlCommand:
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

**Tab Column Mappings:**

| Input Column | Destination Column |
|---|---|
| order_status | Param_0 |
| approved_date | Param_1 |
| delivered_date | Param_2 |
| days_to_approve | Param_3 |
| days_to_delivery | Param_4 |
| is_delayed | Param_5 |
| existing_lifecycle_id | Param_6 |

3. Click **OK**

### 6.9. Tổng quan Data Flow

```
OLE DB Source (stg_orders + calculated fields)
        │
        ▼
Lookup dim_customer ──No Match──→ DerCol(NULL) ──→ Union All
        │ (Match)                                      ▲
        └──────────────────────────────────────────────┘
                              │
                              ▼
Lookup dim_seller ──No Match──→ DerCol(NULL) ──→ Union All
        │ (Match)                                      ▲
        └──────────────────────────────────────────────┘
                              │
                              ▼
        Lookup fact_order_lifecycle
        ┌─────────────┴─────────────┐
        │ (Match)                    │ (No Match)
        ▼                            ▼
 Conditional Split            OLE DB Destination
 ┌────────┴────────┐          (INSERT mới)
 │                 │
Is_Updated      Unchanged
 │               (bỏ qua)
 ▼
OLE DB Command
(UPDATE)
```

### 6.10. Chạy test

```sql
-- Sau lần chạy 1
SELECT COUNT(*) FROM gold.fact_order_lifecycle;
-- Expected: ~99,441

-- Kiểm tra phân bố status
SELECT order_status, COUNT(*) AS cnt
FROM gold.fact_order_lifecycle
GROUP BY order_status
ORDER BY cnt DESC;

-- Kiểm tra is_delayed
SELECT
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed,
    SUM(CASE WHEN is_delayed = 0 THEN 1 ELSE 0 END) AS on_time,
    SUM(CASE WHEN is_delayed IS NULL THEN 1 ELSE 0 END) AS unknown
FROM gold.fact_order_lifecycle;

-- Test Upsert: chạy package lần 2 → count không đổi
```

---
---

# PHẦN F – SSIS PACKAGE 4: AGGREGATED FACTS

---

## BƯỚC 7: Xây dựng `Load_Fact_Delivery_Payment.dtsx`

### 7.1. Điều kiện tiên quyết

- ✅ `gold.fact_orders` có dữ liệu (TV2 phải load xong)
- ✅ `gold.fact_order_lifecycle` có dữ liệu (bạn vừa load ở Bước 6)
- ✅ `staging.stg_order_payments` có dữ liệu (bạn load ở Bước 4)

### 7.2. Mở Package

Double-click `Load_Fact_Delivery_Payment.dtsx` → tab Control Flow.

### 7.3. Control Flow Layout

Package này có **2 nhánh song song**: Delivery và Payment.

```
┌─────────────────────────┐       ┌──────────────────────────────┐
│ EST - Truncate           │       │ EST - Truncate                │
│ fact_delivery +          │       │ fact_payment_trends +         │
│ fact_delivery_year       │       │ fact_payment_trends_year      │
└───────────┬─────────────┘       └───────────┬────────────────────┘
            │                                  │
            ▼                                  ▼
┌───────────────────────┐          ┌────────────────────────────┐
│ DFT - Load             │          │ DFT - Load                  │
│ fact_delivery (month)  │          │ fact_payment_trends (month) │
└───────────┬────────────┘          └───────────┬─────────────────┘
            │                                   │
            ▼                                   ▼
┌───────────────────────┐          ┌────────────────────────────┐
│ DFT - Load             │          │ DFT - Load                  │
│ fact_delivery_year     │          │ fact_payment_trends_year    │
└────────────────────────┘          └─────────────────────────────┘
```

Hai nhánh **KHÔNG nối với nhau** → chạy song song.

### 7.4. Nhánh 1: fact_delivery

#### 7.4.1. Execute SQL Task – Truncate

1. Kéo **Execute SQL Task** → đổi tên `EST - Truncate Delivery`
2. Connection: `OlistDW_OLEDB`
3. SQL:
```sql
TRUNCATE TABLE gold.fact_delivery;
TRUNCATE TABLE gold.fact_delivery_year;
```

#### 7.4.2. Data Flow Task – fact_delivery (monthly)

1. Kéo **Data Flow Task** → đổi tên `DFT - Load fact_delivery`
2. Nối: `EST - Truncate Delivery` → `DFT - Load fact_delivery`
3. Vào Data Flow:

**OLE DB Source:**
- Data access mode: SQL command
- SQL:
```sql
SELECT
    fo.seller_key,
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01') AS date_key,
    COUNT(DISTINCT fo.order_id) AS total_orders_delivered,
    SUM(CASE WHEN fl.is_delayed = 0 THEN 1 ELSE 0 END) AS on_time_orders,
    CAST(
        SUM(CASE WHEN fl.is_delayed = 0 THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(DISTINCT fo.order_id), 0)
    AS DECIMAL(5,4)) AS on_time_rate
FROM gold.fact_orders fo
INNER JOIN gold.dim_date dd
    ON fo.order_date_key = dd.date_key
INNER JOIN gold.fact_order_lifecycle fl
    ON fo.order_id = fl.order_id
WHERE fo.seller_key IS NOT NULL
  AND fl.order_status = 'delivered'
GROUP BY
    fo.seller_key,
    CONVERT(INT, FORMAT(dd.full_date, 'yyyyMM') + '01');
```

> **Nếu bảng `gold.fact_orders` chưa có dữ liệu (TV2 chưa chạy)**, dùng query thay thế từ staging:
```sql
SELECT
    ds.seller_key,
    CONVERT(INT, FORMAT(CAST(o.order_purchase_timestamp AS DATE), 'yyyyMM') + '01') AS date_key,
    COUNT(DISTINCT o.order_id) AS total_orders_delivered,
    SUM(CASE WHEN fl.is_delayed = 0 THEN 1 ELSE 0 END) AS on_time_orders,
    CAST(
        SUM(CASE WHEN fl.is_delayed = 0 THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(DISTINCT o.order_id), 0)
    AS DECIMAL(5,4)) AS on_time_rate
FROM staging.stg_orders o
INNER JOIN staging.stg_order_items oi ON o.order_id = oi.order_id
INNER JOIN gold.dim_seller ds ON oi.seller_id = ds.seller_id AND ds.is_current = 1
INNER JOIN gold.fact_order_lifecycle fl ON o.order_id = fl.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
  AND fl.order_status = 'delivered'
GROUP BY
    ds.seller_key,
    CONVERT(INT, FORMAT(CAST(o.order_purchase_timestamp AS DATE), 'yyyyMM') + '01');
```

**OLE DB Destination:**
- Table: `[gold].[fact_delivery]`
- Map: `seller_key`, `date_key`, `total_orders_delivered`, `on_time_orders`, `on_time_rate`

#### 7.4.3. Data Flow Task – fact_delivery_year

1. Quay lại Control Flow → kéo **Data Flow Task** mới → đổi tên `DFT - Load fact_delivery_year`
2. Nối: `DFT - Load fact_delivery` → `DFT - Load fact_delivery_year`
3. Vào Data Flow:

**OLE DB Source:**
```sql
SELECT
    fo.seller_key,
    dd.year * 10000 + 101 AS year_key,
    COUNT(DISTINCT fo.order_id) AS total_orders_delivered,
    SUM(CASE WHEN fl.is_delayed = 0 THEN 1 ELSE 0 END) AS on_time_orders,
    CAST(
        SUM(CASE WHEN fl.is_delayed = 0 THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(DISTINCT fo.order_id), 0)
    AS DECIMAL(5,4)) AS on_time_rate
FROM gold.fact_orders fo
INNER JOIN gold.dim_date dd ON fo.order_date_key = dd.date_key
INNER JOIN gold.fact_order_lifecycle fl ON fo.order_id = fl.order_id
WHERE fo.seller_key IS NOT NULL
  AND fl.order_status = 'delivered'
GROUP BY fo.seller_key, dd.year * 10000 + 101;
```

**OLE DB Destination:** `[gold].[fact_delivery_year]`

---

### 7.5. Nhánh 2: fact_payment_trends

#### 7.5.1. Execute SQL Task – Truncate

1. Kéo **Execute SQL Task** → đổi tên `EST - Truncate Payment Trends`
2. SQL:
```sql
TRUNCATE TABLE gold.fact_payment_trends;
TRUNCATE TABLE gold.fact_payment_trends_year;
```

> **Không nối** với nhánh Delivery → 2 nhánh chạy song song.

#### 7.5.2. Data Flow Task – fact_payment_trends (monthly)

1. Kéo **Data Flow Task** → đổi tên `DFT - Load fact_payment_trends`
2. Nối: `EST - Truncate Payment Trends` → `DFT - Load fact_payment_trends`
3. Vào Data Flow:

**OLE DB Source:**
```sql
SELECT
    p.payment_type,
    CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMM') + '01') AS date_key,
    SUM(p.payment_value)        AS total_payment_value,
    COUNT(*)                     AS transaction_count,
    COUNT(DISTINCT p.order_id)   AS order_count
FROM staging.stg_order_payments p
INNER JOIN staging.stg_orders o
    ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY
    p.payment_type,
    CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMM') + '01');
```

**OLE DB Destination:** `[gold].[fact_payment_trends]`
- Map: `payment_type`, `date_key`, `total_payment_value`, `transaction_count`, `order_count`

#### 7.5.3. Data Flow Task – fact_payment_trends_year

1. Kéo DFT mới → đổi tên `DFT - Load fact_payment_trends_year`
2. Nối sau DFT monthly

**OLE DB Source:**
```sql
SELECT
    p.payment_type,
    YEAR(o.order_purchase_timestamp) * 10000 + 101 AS year_key,
    SUM(p.payment_value)        AS total_payment_value,
    COUNT(*)                     AS transaction_count,
    COUNT(DISTINCT p.order_id)   AS order_count
FROM staging.stg_order_payments p
INNER JOIN staging.stg_orders o
    ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY
    p.payment_type,
    YEAR(o.order_purchase_timestamp) * 10000 + 101;
```

**OLE DB Destination:** `[gold].[fact_payment_trends_year]`

### 7.6. Chạy test

```sql
SELECT 'fact_delivery'          AS tbl, COUNT(*) AS cnt FROM gold.fact_delivery
UNION ALL SELECT 'fact_delivery_year',   COUNT(*) FROM gold.fact_delivery_year
UNION ALL SELECT 'fact_payment_trends',  COUNT(*) FROM gold.fact_payment_trends
UNION ALL SELECT 'fact_payment_trends_yr', COUNT(*) FROM gold.fact_payment_trends_year;
```

---
---

# PHẦN G – MASTER PACKAGE

---

## BƯỚC 8: Xây dựng `Master_ETL.dtsx`

### 8.1. Mở Package

Double-click `Master_ETL.dtsx` → tab Control Flow.

### 8.2. Thêm Execute Package Tasks

Mỗi child package cần 1 **Execute Package Task**. Tạo tổng cộng **10 tasks**:

| # | Tên Task | Package gọi | Thành viên |
|---|---|---|---|
| 1 | `Step1_Extract_Customer_Geo` | `Extract_Customer_Geo.dtsx` | TV1 |
| 2 | `Step1_Extract_Product_Items` | `Extract_Product_Items.dtsx` | TV2 |
| 3 | `Step1_Extract_Seller_Order_Payment` | `Extract_Seller_Order_Payment.dtsx` | TV3 |
| 4 | `Step2_Load_Dim_Date_Geo_Customer` | `Load_Dim_Date_Geo_Customer.dtsx` | TV1 |
| 5 | `Step2_Load_Dim_Product` | `Load_Dim_Product.dtsx` | TV2 |
| 6 | `Step2_Load_Dim_Seller` | `Load_Dim_Seller.dtsx` | TV3 |
| 7 | `Step3_Load_Fact_Orders` | `Load_Fact_Orders.dtsx` | TV2 |
| 8 | `Step3_Load_Fact_Lifecycle` | `Load_Fact_Lifecycle.dtsx` | TV3 |
| 9 | `Step4_Load_Fact_Customer_Orders` | `Load_Fact_Customer_Orders.dtsx` | TV1 |
| 10 | `Step4_Load_Fact_Sales` | `Load_Fact_Sales.dtsx` | TV2 |
| 11 | `Step4_Load_Fact_Delivery_Payment` | `Load_Fact_Delivery_Payment.dtsx` | TV3 |

**Cách tạo mỗi Execute Package Task:**

1. Kéo **Execute Package Task** từ Toolbox vào Control Flow
2. Đổi tên (Properties → Name)
3. Double-click → **Execute Package Task Editor**:

**Tab Package:**

| Thuộc tính | Giá trị |
|---|---|
| ReferenceType | **Project Reference** |
| PackageNameFromProjectReference | Chọn từ dropdown (vd: `Extract_Customer_Geo.dtsx`) |

4. Click **OK**

Lặp lại cho tất cả 11 tasks.

### 8.3. Nối Precedence Constraints

**STEP 1 – Extract (song song, 3 tasks):**
- `Step1_Extract_Customer_Geo`, `Step1_Extract_Product_Items`, `Step1_Extract_Seller_Order_Payment`
- KHÔNG nối với nhau → chạy song song

**STEP 1 → STEP 2:**
- Nối mũi tên xanh từ CẢ 3 task Step 1 → `Step2_Load_Dim_Date_Geo_Customer`
- Nối cả 3 → `Step2_Load_Dim_Product`
- Cách nối nhiều-1: kéo mũi tên xanh từ mỗi Step1 task vào Step2 task
- Double-click mũi tên → **Multiple Constraints:** chọn **Logical AND** (tất cả phải thành công)

**STEP 2 dependency quan trọng:**
- `Step2_Load_Dim_Seller` **phụ thuộc** `Step2_Load_Dim_Date_Geo_Customer` (cần dim_geolocation)
- Nối: `Step2_Load_Dim_Date_Geo_Customer` → `Step2_Load_Dim_Seller`
- Đồng thời nối cả 3 Step1 tasks → `Step2_Load_Dim_Seller`

**STEP 2 → STEP 3:**
- Nối `Step2_Load_Dim_Date_Geo_Customer` + `Step2_Load_Dim_Product` + `Step2_Load_Dim_Seller` → `Step3_Load_Fact_Orders`
- Nối cả 3 Step2 → `Step3_Load_Fact_Lifecycle`
- Multiple Constraints: **Logical AND**

**STEP 3 → STEP 4:**
- Nối `Step3_Load_Fact_Orders` + `Step3_Load_Fact_Lifecycle` → mỗi Step4 task
- Step4 tasks chạy song song với nhau

### 8.4. Layout tổng quan

```
       Step1_Extract     Step1_Extract     Step1_Extract
       _Customer_Geo     _Product_Items    _Seller_Order
            │                  │                 │
            └──────────────────┼─────────────────┘
                               │ (AND)
                    ┌──────────┼──────────┐
                    ▼          ▼          │
          Step2_Load_Dim   Step2_Load    │
          _Date_Geo_Cust   _Dim_Product  │
                    │          │          │
                    │          │          │
                    ▼──────────│──────────▼
              Step2_Load_Dim_Seller
                    │
            ┌───────┼───────┐
            ▼       │       ▼
    Step3_Load    Step3_Load
    _Fact_Orders  _Fact_Lifecycle
            │       │
            └───┬───┘
                │ (AND)
        ┌───────┼───────────────┐
        ▼       ▼               ▼
  Step4_Load  Step4_Load    Step4_Load
  _Fact_Cust  _Fact_Sales   _Fact_Deliv
  _Orders                   _Payment
```

### 8.5. Cấu hình Logging

1. Click vào vùng trống trong Control Flow
2. Menu **SSIS** → **Logging...**
3. Ở panel trái, chọn **Master_ETL** (root package)
4. Tab **Providers and Logs**: chọn Provider type = **SSIS log provider for SQL Server** → click **Add**
5. Chọn Configuration: `OlistDW_OLEDB`
6. Tab **Details**: tick các events:
   - ✅ `OnPreExecute`
   - ✅ `OnPostExecute`
   - ✅ `OnError`
   - ✅ `OnWarning`
   - ✅ `OnTaskFailed`
7. Click **OK**

### 8.6. Chạy test Master Package

1. Click `Master_ETL.dtsx` → F5
2. Quan sát thứ tự thực thi:
   - 3 Extract chạy song song (vàng → xanh)
   - Dim Date/Geo/Customer + Dim Product chạy song song
   - Dim Seller chờ → chạy sau
   - Fact Orders + Fact Lifecycle
   - 3 Aggregated Facts song song
3. Nếu có lỗi (đỏ): xem tab **Progress** hoặc **Output** window → tìm error message

---
---

# PHẦN H – SQL TRUY VẤN PHÂN TÍCH

---

## BƯỚC 9: Viết 3 câu truy vấn insight

### Query 1: Top 10 sellers giao hàng đúng hạn nhất (ít nhất 10 đơn)

```sql
SELECT TOP 10
    ds.seller_id,
    ds.city,
    ds.state,
    ds.seller_region,
    SUM(fd.total_orders_delivered) AS total_delivered,
    SUM(fd.on_time_orders)         AS total_on_time,
    CAST(SUM(fd.on_time_orders) * 1.0 / NULLIF(SUM(fd.total_orders_delivered), 0)
         AS DECIMAL(5,4))          AS overall_on_time_rate
FROM gold.fact_delivery fd
INNER JOIN gold.dim_seller ds
    ON fd.seller_key = ds.seller_key AND ds.is_current = 1
GROUP BY ds.seller_id, ds.city, ds.state, ds.seller_region
HAVING SUM(fd.total_orders_delivered) >= 10
ORDER BY overall_on_time_rate DESC, total_delivered DESC;
```

### Query 2: Xu hướng phương thức thanh toán theo thời gian

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
INNER JOIN gold.dim_payment_method pm
    ON fpt.payment_type = pm.payment_type
INNER JOIN gold.dim_date dd
    ON fpt.date_key = dd.date_key
ORDER BY dd.year, dd.month, fpt.total_payment_value DESC;
```

### Query 3: Phân tích vòng đời đơn hàng

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
    AS DECIMAL(5,2)) AS delay_rate_pct
FROM gold.fact_order_lifecycle fl
GROUP BY fl.order_status
ORDER BY total_orders DESC;
```

---
---

# PHẦN I – VERIFY & TEST

---

## BƯỚC 10: Kiểm tra toàn bộ

### 10.1. Row counts

```sql
SELECT 'dim_seller'               AS tbl, COUNT(*) AS cnt FROM gold.dim_seller
UNION ALL SELECT 'fact_order_lifecycle',   COUNT(*) FROM gold.fact_order_lifecycle
UNION ALL SELECT 'fact_delivery',          COUNT(*) FROM gold.fact_delivery
UNION ALL SELECT 'fact_delivery_year',     COUNT(*) FROM gold.fact_delivery_year
UNION ALL SELECT 'fact_payment_trends',    COUNT(*) FROM gold.fact_payment_trends
UNION ALL SELECT 'fact_payment_trends_yr', COUNT(*) FROM gold.fact_payment_trends_year;
```

### 10.2. Kiểm tra SCD Type 2 hoạt động

```sql
-- Tất cả sellers phải có ít nhất 1 bản ghi is_current = 1
SELECT
    (SELECT COUNT(DISTINCT seller_id) FROM gold.dim_seller) AS unique_sellers,
    (SELECT COUNT(DISTINCT seller_id) FROM gold.dim_seller WHERE is_current = 1) AS current_sellers;
-- Hai số phải bằng nhau

-- Tìm sellers có nhiều versions (nếu có)
SELECT seller_id, COUNT(*) AS versions
FROM gold.dim_seller
GROUP BY seller_id
HAVING COUNT(*) > 1;
```

### 10.3. Kiểm tra Upsert hoạt động

```sql
-- Ghi nhận count
SELECT COUNT(*) AS before_count FROM gold.fact_order_lifecycle;

-- Chạy lại package Load_Fact_Lifecycle.dtsx

SELECT COUNT(*) AS after_count FROM gold.fact_order_lifecycle;
-- before_count PHẢI = after_count (không insert trùng)
```

### 10.4. Kiểm tra data consistency

```sql
-- Tổng payment phải gần khớp staging
SELECT SUM(total_payment_value) AS fact_total FROM gold.fact_payment_trends;
SELECT SUM(payment_value)       AS stg_total  FROM staging.stg_order_payments;
-- Có thể lệch nhỏ do orders không có timestamp bị loại

-- Kiểm tra FK integrity
SELECT 'lifecycle_missing_customer' AS issue, COUNT(*)
FROM gold.fact_order_lifecycle f
LEFT JOIN gold.dim_customer d ON f.customer_key = d.customer_key
WHERE f.customer_key IS NOT NULL AND d.customer_key IS NULL
UNION ALL
SELECT 'lifecycle_missing_seller', COUNT(*)
FROM gold.fact_order_lifecycle f
LEFT JOIN gold.dim_seller d ON f.seller_key = d.seller_key
WHERE f.seller_key IS NOT NULL AND d.seller_key IS NULL;
-- Cả 2 phải = 0
```

### 10.5. Test Master Package end-to-end

1. Truncate toàn bộ bảng gold (hoặc tạo database mới)
2. Chạy `Master_ETL.dtsx`
3. Tất cả tasks phải xanh
4. Chạy lại verify queries ở trên

---
---

# CHECKLIST HOÀN THÀNH TV3

- [ ] **DDL:** 3 staging tables tạo thành công
- [ ] **DDL:** dim_seller tạo thành công (có SCD Type 2 columns)
- [ ] **DDL:** 5 fact tables tạo thành công
- [ ] **Package 1:** `Extract_Seller_Order_Payment.dtsx` – tất cả DFT xanh, row counts đúng
- [ ] **Package 2:** `Load_Dim_Seller.dtsx` – SCD Type 2 hoạt động (Match/No Match/Changed/Unchanged paths)
- [ ] **Package 3:** `Load_Fact_Lifecycle.dtsx` – Upsert hoạt động (chạy lần 2 không trùng)
- [ ] **Package 4:** `Load_Fact_Delivery_Payment.dtsx` – 4 fact tables load thành công
- [ ] **Master Package:** `Master_ETL.dtsx` – tích hợp tất cả packages, chạy end-to-end xanh
- [ ] **SCD Type 2:** dim_seller có effective_from/to, is_current đúng
- [ ] **Upsert:** fact_order_lifecycle không insert trùng
- [ ] **SQL Queries:** 3 queries chạy đúng, trả về insight hợp lý
- [ ] **Data Integrity:** Payment totals khớp, không orphan FK
- [ ] **Logging:** Master Package có SSIS Logging enabled

# 📘 GUIDE_TV2.md – Hướng dẫn chi tiết Thành viên 2

## Domain: Sản phẩm & Doanh thu

**Phụ trách:**
- Staging: `stg_products`, `stg_category_translation`, `stg_order_items`
- Dimensions: `dim_product_category` (SCD Type 1), `dim_product` (SCD Type 1), `dim_payment_method`
- Facts: `fact_orders` (Incremental Load – bảng chính của toàn bộ DW), `fact_sales` (month + year)
- SSIS Packages: 4 packages

---
---

# PHẦN A – CHUẨN BỊ

---

## BƯỚC 0: Kiểm tra môi trường

### 0.1. Xác nhận Database + Schemas đã tồn tại

Mở SSMS, kết nối tới SQL Server, chạy:

```sql
USE OlistDW;
GO
SELECT name FROM sys.schemas WHERE name IN ('staging', 'gold');
```

Phải trả về 2 dòng: `staging` và `gold`. Nếu không có → nhờ TV1 tạo trước.

### 0.2. Chuẩn bị file CSV

Đảm bảo 3 file CSV đã tải từ Kaggle và nằm trong **cùng 1 thư mục** (ví dụ `D:\OlistData\`):

| File | Tên đầy đủ |
|---|---|
| Products | `olist_products_dataset.csv` |
| Category Translation | `product_category_name_translation.csv` |
| Order Items | `olist_order_items_dataset.csv` |

### 0.3. Mở SSIS Project

1. Mở **Visual Studio** (hoặc SQL Server Data Tools – SSDT)
2. Mở Solution/Project `OlistDW_ETL` mà team đã tạo

### 0.4. Tạo 4 SSIS Packages

Trong **Solution Explorer** (panel bên phải):
1. Chuột phải vào thư mục **SSIS Packages** → **New SSIS Package**
2. Đổi tên (chuột phải → Rename):
   - `Extract_Product_Items.dtsx`
3. Lặp lại tạo thêm 3 packages:
   - `Load_Dim_Product.dtsx`
   - `Load_Fact_Orders.dtsx`
   - `Load_Fact_Sales.dtsx`

### 0.5. Kiểm tra Connection Manager

Kiểm tra ở panel **Connection Managers** (dưới cùng) đã có `OlistDW_OLEDB`. Nếu chưa:

1. Chuột phải vùng trống → **New OLE DB Connection...**
2. Click **New...**
3. Server name: `localhost` hoặc `.\SQLEXPRESS` (tùy cấu hình)
4. Database: `OlistDW`
5. Click **Test Connection** → "Test connection succeeded"
6. **OK** → **OK**
7. Đổi tên connection thành `OlistDW_OLEDB`

> **Tip:** Chuột phải connection → **Convert to Project Connection** để dùng chung cho tất cả packages.

---
---

# PHẦN B – TẠO BẢNG TRONG SQL SERVER

---

## BƯỚC 1: Tạo Staging Tables

Mở SSMS → New Query → đảm bảo kết nối đúng database `OlistDW` → chạy từng block:

### 1.1. staging.stg_products

```sql
USE OlistDW;
GO

IF OBJECT_ID('staging.stg_products', 'U') IS NOT NULL
    DROP TABLE staging.stg_products;
GO

CREATE TABLE staging.stg_products (
    product_id                  VARCHAR(50)    NOT NULL,
    product_category_name       NVARCHAR(100)  NULL,
    product_name_lenght         INT            NULL,  -- typo gốc trong dataset Olist
    product_description_lenght  INT            NULL,  -- typo gốc trong dataset Olist
    product_photos_qty          INT            NULL,
    product_weight_g            DECIMAL(10,2)  NULL,
    product_length_cm           DECIMAL(10,2)  NULL,
    product_height_cm           DECIMAL(10,2)  NULL,
    product_width_cm            DECIMAL(10,2)  NULL
);
GO
```

> **Lưu ý:** Tên cột `product_name_lenght` và `product_description_lenght` **đúng là viết sai chính tả** trong dataset gốc. Giữ nguyên để khớp với CSV.

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
    price               DECIMAL(10,5) NOT NULL,
    freight_value       DECIMAL(10,2) NOT NULL
);
GO
```

**Kiểm tra:** Mở Object Explorer → OlistDW → Tables → phải thấy 3 bảng staging mới.

---

## BƯỚC 2: Tạo Dimension Tables

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

-- Unique index để Lookup nhanh hơn
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
    product_key                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    product_id                 VARCHAR(50)        NOT NULL,
    category_key               INT                NULL,
    product_name_length        INT                NULL,
    product_description_length INT                NULL,
    product_photos_qty         INT                NULL,
    product_weight_g           DECIMAL(10,2)      NULL,
    product_length_cm          DECIMAL(10,2)      NULL,
    product_height_cm          DECIMAL(10,2)      NULL,
    product_width_cm           DECIMAL(10,2)      NULL,
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

## BƯỚC 3: Tạo Fact Tables

### 3.1. gold.fact_orders ⭐ (Bảng chính của toàn bộ Data Warehouse)

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

-- Index phục vụ Incremental Load (kiểm tra record đã tồn tại)
CREATE UNIQUE INDEX UX_fact_orders_bk ON gold.fact_orders(order_id, order_item_id);
GO

-- Indexes cho FK joins (cải thiện performance truy vấn)
CREATE INDEX IX_fo_customer ON gold.fact_orders(customer_key);
CREATE INDEX IX_fo_seller   ON gold.fact_orders(seller_key);
CREATE INDEX IX_fo_product  ON gold.fact_orders(product_key);
CREATE INDEX IX_fo_date     ON gold.fact_orders(order_date_key);
GO
```

> **Nếu lỗi FK:** Các bảng `dim_customer`, `dim_seller`, `dim_date`, `dim_order_status` do TV1 và TV3 tạo. Nếu chưa có → tạm bỏ các dòng CONSTRAINT FK, thêm sau bằng ALTER TABLE.

### 3.2. gold.fact_sales (monthly)

```sql
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales;
GO

CREATE TABLE gold.fact_sales (
    seller_key       INT            NOT NULL,
    category_key     INT            NOT NULL,
    date_key         INT            NOT NULL,
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
    year_key         INT            NOT NULL,
    total_revenue    DECIMAL(14,2)  NOT NULL DEFAULT 0,
    total_items_sold INT            NOT NULL DEFAULT 0,
    total_orders     INT            NOT NULL DEFAULT 0,
    avg_review_score DECIMAL(3,2)   NULL,
    CONSTRAINT PK_fact_sales_year PRIMARY KEY (seller_key, category_key, year_key)
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

## BƯỚC 4: Xây dựng `Extract_Product_Items.dtsx`

### 4.1. Mở Package

Double-click `Extract_Product_Items.dtsx` trong Solution Explorer → mở tab **Control Flow**.

### 4.2. Thêm Execute SQL Task – Truncate Staging

1. Trong **SSIS Toolbox** (panel trái), kéo **Execute SQL Task** vào Control Flow
2. Click vào task → panel Properties bên phải → đổi **Name**: `EST - Truncate Staging Tables`
3. Double-click task → mở editor:

| Thuộc tính | Giá trị |
|---|---|
| Connection | Chọn `OlistDW_OLEDB` |
| SQLSourceType | `Direct input` |
| SQLStatement | Click `...` → paste SQL dưới đây |

```sql
TRUNCATE TABLE staging.stg_products;
TRUNCATE TABLE staging.stg_category_translation;
TRUNCATE TABLE staging.stg_order_items;
```

4. Click **OK**

---

### 4.3. Data Flow Task 1 – Load Products (phức tạp nhất do xử lý NULL)

#### 4.3.1. Thêm Data Flow Task

1. Kéo **Data Flow Task** từ Toolbox vào Control Flow
2. Đổi tên: `DFT - Load Products`
3. **Nối Precedence Constraint:** Click vào `EST - Truncate Staging Tables` → thấy mũi tên xanh nhỏ ở cạnh dưới → kéo xuống `DFT - Load Products` → thả → xuất hiện đường mũi tên xanh
4. Double-click `DFT - Load Products` → chuyển sang tab **Data Flow**

#### 4.3.2. Flat File Source

1. Từ Toolbox, kéo **Flat File Source** vào canvas Data Flow
2. Double-click → click **New...** để tạo Connection Manager

**Flat File Connection Manager Editor – Tab General:**

| Thuộc tính | Giá trị |
|---|---|
| Connection manager name | `FF_Products` |
| File name | Click Browse → `D:\OlistData\olist_products_dataset.csv` |
| Locale | `English (United States)` |
| Code page | `65001 (UTF-8)` |
| Format | `Delimited` |
| Text qualifier | `"` |
| Header row delimiter | `{CR}{LF}` |
| ✅ Column names in the first data row | Check |

**Tab Columns:**
- Kiểm tra 9 cột hiển thị đúng: `product_id`, `product_category_name`, `product_name_lenght`, `product_description_lenght`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`
- Column delimiter: `Comma {,}`
- Row delimiter: `{CR}{LF}`

**Tab Advanced – ĐẶC BIỆT QUAN TRỌNG:**

File products có nhiều giá trị **rỗng (empty string)** ở các cột numeric (weight, dimensions, photos_qty, name_length, description_length). Nếu set DataType là numeric/integer, SSIS sẽ **lỗi ngay** khi gặp giá trị rỗng.

**Giải pháp:** Set TẤT CẢ cột thành **string**, rồi convert an toàn ở Derived Column.

Click vào từng cột ở panel trái → sửa Properties ở panel phải:

| Column Name | DataType | OutputColumnWidth |
|---|---|---|
| product_id | string [DT_STR] | 50 |
| product_category_name | Unicode string [DT_WSTR] | 100 |
| product_name_lenght | **string [DT_STR]** | **20** |
| product_description_lenght | **string [DT_STR]** | **20** |
| product_photos_qty | **string [DT_STR]** | **20** |
| product_weight_g | **string [DT_STR]** | **20** |
| product_length_cm | **string [DT_STR]** | **20** |
| product_height_cm | **string [DT_STR]** | **20** |
| product_width_cm | **string [DT_STR]** | **20** |

3. Click **OK** → Click **OK**

#### 4.3.3. Derived Column – Convert và xử lý NULL

**Mục đích:** Chuyển string → đúng kiểu dữ liệu, giá trị rỗng → 0 hoặc NULL.

1. Kéo **Derived Column** từ Toolbox → nối mũi tên xanh từ Flat File Source
2. Double-click → thêm 9 cột mới:

Cho mỗi cột, chọn `<add as new column>` ở dropdown "Derived Column":

**Cột 1 – product_id (giữ nguyên string, chỉ trim):**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_product_id` |
| Expression | `(DT_STR, 50, 1252)TRIM(product_id)` |

**Cột 2 – category_name (giữ nguyên):**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_category_name` |
| Expression | `TRIM(product_category_name)` |

**Cột 3 – name_length:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_name_len` |
| Expression | `LEN(TRIM(product_name_lenght)) == 0 ? NULL(DT_I4) : (DT_I4)product_name_lenght` |

**Cột 4 – description_length:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_desc_len` |
| Expression | `LEN(TRIM(product_description_lenght)) == 0 ? NULL(DT_I4) : (DT_I4)product_description_lenght` |

**Cột 5 – photos_qty:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_photos` |
| Expression | `LEN(TRIM(product_photos_qty)) == 0 ? NULL(DT_I4) : (DT_I4)product_photos_qty` |

**Cột 6 – weight:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_weight` |
| Expression | `LEN(TRIM(product_weight_g)) == 0 ? NULL(DT_NUMERIC,10,2) : (DT_NUMERIC,10,2)product_weight_g` |

**Cột 7 – length:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_length` |
| Expression | `LEN(TRIM(product_length_cm)) == 0 ? NULL(DT_NUMERIC,10,2) : (DT_NUMERIC,10,2)product_length_cm` |

**Cột 8 – height:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_height` |
| Expression | `LEN(TRIM(product_height_cm)) == 0 ? NULL(DT_NUMERIC,10,2) : (DT_NUMERIC,10,2)product_height_cm` |

**Cột 9 – width:**

| Thuộc tính | Giá trị |
|---|---|
| Derived Column Name | `clean_width` |
| Expression | `LEN(TRIM(product_width_cm)) == 0 ? NULL(DT_NUMERIC,10,2) : (DT_NUMERIC,10,2)product_width_cm` |

> **Giải thích logic:** `LEN(TRIM(...)) == 0` kiểm tra chuỗi rỗng → trả NULL. Ngược lại → cast sang đúng kiểu.

3. Click **OK**

#### 4.3.4. OLE DB Destination

1. Kéo **OLE DB Destination** → nối từ Derived Column
2. Double-click:

**Tab Connection Manager:**

| Thuộc tính | Giá trị |
|---|---|
| OLE DB connection manager | `OlistDW_OLEDB` |
| Data access mode | `Table or view - fast load` |
| Name of the table or view | `[staging].[stg_products]` |
| Table lock | ✅ Checked |

**Tab Mappings – map thủ công:**

| Input Column | Destination Column |
|---|---|
| clean_product_id | product_id |
| clean_category_name | product_category_name |
| clean_name_len | product_name_lenght |
| clean_desc_len | product_description_lenght |
| clean_photos | product_photos_qty |
| clean_weight | product_weight_g |
| clean_length | product_length_cm |
| clean_height | product_height_cm |
| clean_width | product_width_cm |

> **Quan trọng:** Chỉ map các cột `clean_*`, KHÔNG map các cột string gốc. Nếu thấy cột gốc tự động map → click vào đường nối → xóa → chọn đúng cột clean.

3. Click **OK**

---

### 4.4. Data Flow Task 2 – Load Category Translation

#### 4.4.1. Quay lại Control Flow

Click tab **Control Flow** ở trên.

#### 4.4.2. Thêm Data Flow Task

1. Kéo **Data Flow Task** → đổi tên `DFT - Load Category Translation`
2. Nối Precedence Constraint từ `EST - Truncate Staging Tables` → task này (chạy song song với Load Products)
3. Double-click → vào Data Flow

#### 4.4.3. Flat File Source

1. Kéo **Flat File Source** → double-click → New:

| Thuộc tính | Giá trị |
|---|---|
| Connection manager name | `FF_CategoryTranslation` |
| File name | `D:\OlistData\product_category_name_translation.csv` |
| ✅ Column names in the first data row | Check |

**Tab Advanced:**

| Column | DataType | OutputColumnWidth |
|---|---|---|
| product_category_name | Unicode string [DT_WSTR] | 100 |
| product_category_name_english | Unicode string [DT_WSTR] | 100 |

2. **OK** → **OK**

#### 4.4.4. OLE DB Destination

1. Kéo **OLE DB Destination** → nối từ Flat File Source
2. Connection: `OlistDW_OLEDB`
3. Table: `[staging].[stg_category_translation]`
4. Mappings: auto-map (tên cột giống nhau)
5. **OK**

> **Không cần Data Conversion/Derived Column** vì file này chỉ có 2 cột string, không có giá trị NULL.

---

### 4.5. Data Flow Task 3 – Load Order Items

#### 4.5.1. Quay lại Control Flow → kéo Data Flow Task → đổi tên `DFT - Load Order Items`

#### 4.5.2. Nối Precedence Constraint từ Truncate (chạy song song với 2 DFT kia)

#### 4.5.3. Vào Data Flow

**Flat File Source:**
1. Tạo Connection Manager mới: `FF_OrderItems` → file `olist_order_items_dataset.csv`
2. Tab Advanced – set DataType:

| Column | DataType | OutputColumnWidth |
|---|---|---|
| order_id | string [DT_STR] | 50 |
| order_item_id | string [DT_STR] | 10 |
| product_id | string [DT_STR] | 50 |
| seller_id | string [DT_STR] | 50 |
| shipping_limit_date | string [DT_STR] | 30 |
| price | string [DT_STR] | 20 |
| freight_value | string [DT_STR] | 20 |

**Data Conversion:**
1. Kéo **Data Conversion** → nối từ Flat File Source
2. Double-click → tick và cấu hình:

| ✅ Input Column | Output Alias | Data Type | Precision | Scale |
|---|---|---|---|---|
| order_id | cv_order_id | string [DT_STR], 50 | — | — |
| order_item_id | cv_item_id | four-byte signed integer [DT_I4] | — | — |
| product_id | cv_product_id | string [DT_STR], 50 | — | — |
| seller_id | cv_seller_id | string [DT_STR], 50 | — | — |
| shipping_limit_date | cv_ship_date | database timestamp [DT_DBTIMESTAMP] | — | — |
| price | cv_price | numeric [DT_NUMERIC] | 10 | 2 |
| freight_value | cv_freight | numeric [DT_NUMERIC] | 10 | 2 |

> **Cách thao tác:** Tick checkbox cột ở panel trên → dòng xuất hiện ở panel dưới → sửa Output Alias và Data Type.

3. Click **OK**

**OLE DB Destination:**
1. Kéo → nối từ Data Conversion
2. Table: `[staging].[stg_order_items]`
3. Mappings:

| Input Column | Destination Column |
|---|---|
| cv_order_id | order_id |
| cv_item_id | order_item_id |
| cv_product_id | product_id |
| cv_seller_id | seller_id |
| cv_ship_date | shipping_limit_date |
| cv_price | price |
| cv_freight | freight_value |

4. **OK**

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
         │Products  │ │Category  │ │Order     │
         │          │ │Translat. │ │Items     │
         └──────────┘ └──────────┘ └──────────┘
```

### 4.7. Chạy test Package

1. Chuột phải `Extract_Product_Items.dtsx` → **Execute Package** (hoặc F5)
2. Quan sát: Truncate → vàng → xanh; 3 DFTs → vàng (song song) → xanh
3. Nếu đỏ → tab **Progress** hiển thị lỗi chi tiết
4. Click **Stop Debugging**

5. Verify trong SSMS:

```sql
SELECT 'stg_products'           AS tbl, COUNT(*) AS rows_loaded FROM staging.stg_products
UNION ALL
SELECT 'stg_category_translation',      COUNT(*) FROM staging.stg_category_translation
UNION ALL
SELECT 'stg_order_items',               COUNT(*) FROM staging.stg_order_items;
```

Kỳ vọng:

| tbl | rows_loaded |
|---|---|
| stg_products | ~32,951 |
| stg_category_translation | ~71 |
| stg_order_items | ~112,650 |

> **Lỗi thường gặp ở Load Products:** Nếu Derived Column lỗi "Cannot convert..." → kiểm tra lại expression, đảm bảo dùng `LEN(TRIM(...)) == 0` thay vì `ISNULL(...)` (vì chuỗi rỗng KHÔNG phải NULL trong CSV).

---
---

# PHẦN D – SSIS PACKAGE 2: LOAD DIMENSIONS (SCD TYPE 1)

---

## BƯỚC 5: Xây dựng `Load_Dim_Product.dtsx`

### 5.1. Mở Package

Double-click `Load_Dim_Product.dtsx` → tab Control Flow.

### 5.2. Control Flow Layout

```
┌──────────────────────────────────┐
│ EST - Populate dim_payment_method│
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│ DFT - Load dim_product_category  │
│ (SCD Type 1)                     │
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│ DFT - Load dim_product           │
│ (SCD Type 1)                     │
└──────────────────────────────────┘
```

> Thứ tự tuần tự vì `dim_product` cần FK `category_key` từ `dim_product_category`.

### 5.3. Task 1 – Execute SQL Task: Populate dim_payment_method

1. Kéo **Execute SQL Task** → đổi tên `EST - Populate dim_payment_method`
2. Double-click:
   - Connection: `OlistDW_OLEDB`
   - SQLStatement:

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

3. Click **OK**

---

### 5.4. Task 2 – Data Flow: Load dim_product_category (SCD Type 1)

#### 5.4.1. Thêm Data Flow Task → đổi tên `DFT - Load dim_product_category`

Nối: `EST - Populate dim_payment_method` → `DFT - Load dim_product_category`

#### 5.4.2. Vào Data Flow – tổng quan layout:

```
OLE DB Source (stg_category_translation)
        │
        ▼
Lookup dim_product_category (match tên PT)
   ┌────┴────┐
   │         │
Match    No Match ──→ OLE DB Destination (INSERT mới)
   │
   ▼
Conditional Split (tên EN thay đổi?)
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
SELECT
    product_category_name         AS category_name_portuguese,
    product_category_name_english AS category_name_english
FROM staging.stg_category_translation;
```

2. Click **Preview** → verify 2 cột, ~71 dòng → **OK**

#### 5.4.4. Component 2 – Lookup dim_product_category

1. Kéo **Lookup** → nối từ OLE DB Source
2. Double-click:

**Tab General:**

| Thuộc tính | Giá trị |
|---|---|
| Cache mode | Full cache |
| Connection type | OLE DB connection manager |
| Specify how to handle rows with no matching entries | **Redirect rows to no match output** |

**Tab Connection:**
- OLE DB connection manager: `OlistDW_OLEDB`
- Use a table or view: `gold.dim_product_category`

**Tab Columns:**
- Panel trái: kéo `category_name_portuguese`
- Panel phải: thả lên `category_name_portuguese` (tạo đường nối join)
- Tick output:
  - ✅ `category_key` → Output Alias: `existing_cat_key`
  - ✅ `category_name_english` → Output Alias: `existing_eng_name`

3. Click **OK**

#### 5.4.5. No Match → OLE DB Destination (INSERT category mới)

1. Kéo **OLE DB Destination**
2. Nối: Click vào Lookup → kéo mũi tên xanh → thả lên OLE DB Dest → dialog hỏi Output → chọn **Lookup No Match Output**
3. Double-click:
   - Table: `[gold].[dim_product_category]`
   - Mappings:

| Input Column | Destination Column |
|---|---|
| category_name_portuguese | category_name_portuguese |
| category_name_english | category_name_english |

> `category_key` không map vì là IDENTITY auto-increment.

4. **OK**

#### 5.4.6. Match → Conditional Split

1. Kéo **Conditional Split** → nối từ Lookup (**Match Output** – mũi tên xanh còn lại)
2. Double-click:

| Output Name | Condition |
|---|---|
| `Is_Changed` | `category_name_english != existing_eng_name` |
| Default output name | `Unchanged` |

3. Click **OK**

#### 5.4.7. Changed → OLE DB Command (UPDATE)

1. Kéo **OLE DB Command** từ Toolbox (tìm trong section **Other Transforms**)
2. Nối từ Conditional Split output `Is_Changed`
3. Double-click:

**Tab Connection Managers:**
- Connection Manager: `OlistDW_OLEDB`

**Tab Component Properties:**
- Tìm property **SqlCommand** → click ô giá trị → paste:

```sql
UPDATE gold.dim_product_category
SET category_name_english = ?
WHERE category_key = ?;
```

**Tab Column Mappings:**

| Input Column | Destination Column |
|---|---|
| category_name_english | Param_0 |
| existing_cat_key | Param_1 |

> `?` thứ nhất = Param_0, `?` thứ hai = Param_1. Thứ tự quan trọng!

4. Click **OK**

---

### 5.5. Task 3 – Data Flow: Load dim_product (SCD Type 1)

#### 5.5.1. Quay lại Control Flow → kéo Data Flow Task → đổi tên `DFT - Load dim_product`

Nối: `DFT - Load dim_product_category` → `DFT - Load dim_product`

#### 5.5.2. Vào Data Flow – layout:

```
OLE DB Source (stg_products – clean NULL bằng SQL)
        │
        ▼
Lookup dim_product_category (lấy category_key)
   ┌────┴────┐
   │         │
Match    No Match ──→ Derived Column (category_key=NULL)
   │                        │
   └───────► Union All ◄────┘
                │
                ▼
Lookup dim_product (match product_id)
   ┌────┴────┐
   │         │
Match    No Match ──→ OLE DB Destination (INSERT mới)
   │
   ▼
Conditional Split (weight/dims thay đổi?)
   ┌────┴────┐
   │         │
Changed   Unchanged (bỏ qua)
   │
   ▼
OLE DB Command (UPDATE)
```

#### 5.5.3. Component 1 – OLE DB Source

```sql
SELECT
    p.product_id,
    p.product_category_name,
    p.product_name_lenght          AS product_name_length,
    p.product_description_lenght   AS product_description_length,
    p.product_photos_qty,
    ISNULL(p.product_weight_g, 0)  AS product_weight_g,
    ISNULL(p.product_length_cm, 0) AS product_length_cm,
    ISNULL(p.product_height_cm, 0) AS product_height_cm,
    ISNULL(p.product_width_cm, 0)  AS product_width_cm
FROM staging.stg_products p;
```

> **Lưu ý:** `ISNULL(..., 0)` xử lý NULL ngay trong SQL – đơn giản hơn Derived Column. Tên cột output sửa typo (`lenght` → `length`).

#### 5.5.4. Component 2 – Lookup dim_product_category

1. Kéo **Lookup** → nối từ OLE DB Source
2. General: **Redirect rows to no match output**
3. Connection – SQL command:

```sql
SELECT category_key, category_name_portuguese
FROM gold.dim_product_category;
```

4. Columns:
   - Join: `product_category_name` (input) → `category_name_portuguese` (lookup)
   - Output: ✅ `category_key` → Alias: `lkp_category_key`
5. **OK**

#### 5.5.5. Xử lý No Match (product không có category)

Sản phẩm có `product_category_name = NULL` → Lookup không match → cần set `lkp_category_key = NULL` rồi merge lại.

1. Kéo **Derived Column** → nối từ Lookup **No Match Output**:
   - Click Lookup → kéo mũi tên → thả lên Derived Column → dialog → chọn **Lookup No Match Output**
2. Thêm cột: `lkp_category_key` | Expression: `NULL(DT_I4)` | `<add as new column>`
3. **OK**

4. Kéo **Union All** → nối:
   - Lookup **Match Output** (mũi tên xanh còn lại) → Union All
   - Derived Column (No Match) → Union All
5. Double-click Union All → map cột:

| Union All Output | Match Input | No Match Input |
|---|---|---|
| product_id | product_id | product_id |
| product_category_name | product_category_name | product_category_name |
| product_name_length | product_name_length | product_name_length |
| product_description_length | product_description_length | product_description_length |
| product_photos_qty | product_photos_qty | product_photos_qty |
| product_weight_g | product_weight_g | product_weight_g |
| product_length_cm | product_length_cm | product_length_cm |
| product_height_cm | product_height_cm | product_height_cm |
| product_width_cm | product_width_cm | product_width_cm |
| lkp_category_key | lkp_category_key | lkp_category_key |

#### 5.5.6. Component 3 – Lookup dim_product

1. Kéo **Lookup** → nối từ Union All
2. General: **Redirect rows to no match output**
3. Connection – Table: `gold.dim_product`
4. Columns:
   - Join: `product_id` → `product_id`
   - Output:
     - ✅ `product_key` → Alias: `existing_prod_key`
     - ✅ `product_weight_g` → Alias: `existing_weight`
     - ✅ `product_length_cm` → Alias: `existing_length`
5. **OK**

#### 5.5.7. No Match → OLE DB Destination (INSERT product mới)

1. Kéo **OLE DB Destination** → nối từ Lookup **No Match Output**
2. Table: `[gold].[dim_product]`
3. Mappings:

| Input Column | Destination Column |
|---|---|
| product_id | product_id |
| lkp_category_key | category_key |
| product_name_length | product_name_length |
| product_description_length | product_description_length |
| product_photos_qty | product_photos_qty |
| product_weight_g | product_weight_g |
| product_length_cm | product_length_cm |
| product_height_cm | product_height_cm |
| product_width_cm | product_width_cm |

4. **OK**

#### 5.5.8. Match → Conditional Split

1. Kéo **Conditional Split** → nối từ Lookup **Match Output**
2. Condition:

| Output Name | Condition |
|---|---|
| `Is_Changed` | `product_weight_g != existing_weight \|\| product_length_cm != existing_length` |
| Default | `Unchanged` |

3. **OK**

#### 5.5.9. Changed → OLE DB Command (UPDATE)

1. Kéo **OLE DB Command** → nối từ output `Is_Changed`
2. Connection: `OlistDW_OLEDB`
3. SqlCommand:

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

4. Column Mappings:

| Input Column | Destination Column |
|---|---|
| lkp_category_key | Param_0 |
| product_name_length | Param_1 |
| product_description_length | Param_2 |
| product_photos_qty | Param_3 |
| product_weight_g | Param_4 |
| product_length_cm | Param_5 |
| product_height_cm | Param_6 |
| product_width_cm | Param_7 |
| existing_prod_key | Param_8 |

5. **OK**

### 5.6. Chạy test

```sql
SELECT 'dim_product_category' AS tbl, COUNT(*) AS cnt FROM gold.dim_product_category
UNION ALL SELECT 'dim_product',        COUNT(*) FROM gold.dim_product
UNION ALL SELECT 'dim_payment_method', COUNT(*) FROM gold.dim_payment_method;
```

Kỳ vọng:

| tbl | cnt |
|---|---|
| dim_product_category | ~71 |
| dim_product | ~32,951 |
| dim_payment_method | 5 |

---
---

# PHẦN E – SSIS PACKAGE 3: LOAD FACT_ORDERS (INCREMENTAL LOAD) ⭐

**Đây là bảng fact quan trọng nhất – tất cả bảng aggregated (fact_sales, fact_customer_orders, fact_delivery) đều phụ thuộc vào nó.**

---

## BƯỚC 6: Xây dựng `Load_Fact_Orders.dtsx`

### 6.1. Điều kiện tiên quyết

⚠️ **TẤT CẢ dimensions phải có dữ liệu trước khi chạy package này:**

```sql
SELECT
    (SELECT COUNT(*) FROM gold.dim_date)              AS dim_date,
    (SELECT COUNT(*) FROM gold.dim_customer WHERE is_current = 1) AS dim_customer,
    (SELECT COUNT(*) FROM gold.dim_seller WHERE is_current = 1)   AS dim_seller,
    (SELECT COUNT(*) FROM gold.dim_product)            AS dim_product,
    (SELECT COUNT(*) FROM gold.dim_order_status)       AS dim_order_status;
```

Tất cả phải > 0. Nếu = 0 → nhờ TV1/TV3 chạy packages Load Dimension trước.

Ngoài ra cần staging data:
```sql
SELECT
    (SELECT COUNT(*) FROM staging.stg_order_items)   AS stg_order_items,
    (SELECT COUNT(*) FROM staging.stg_orders)        AS stg_orders,
    (SELECT COUNT(*) FROM staging.stg_order_reviews) AS stg_reviews;
```

> `stg_orders` do TV3 load, `stg_order_reviews` do TV1 load.

### 6.2. Mở Package → Control Flow

Double-click `Load_Fact_Orders.dtsx` → kéo **Data Flow Task** → đổi tên `DFT - Load fact_orders (Incremental)`.

### 6.3. Data Flow – tổng quan

```
OLE DB Source (JOIN staging – chỉ lấy records mới)
        │
        ▼
Lookup dim_customer (lấy customer_key)
   ├── No Match → DerCol(NULL) → Union All ←─── Match
        │
        ▼
Lookup dim_seller (lấy seller_key)
   ├── No Match → DerCol(NULL) → Union All ←─── Match
        │
        ▼
Lookup dim_product (lấy product_key)
   ├── No Match → DerCol(NULL) → Union All ←─── Match
        │
        ▼
Derived Column (tính 4 date_keys từ timestamps)
        │
        ▼
OLE DB Destination (gold.fact_orders)
```

### 6.4. Component 1 – OLE DB Source (Incremental Load)

1. Kéo **OLE DB Source** → double-click:
   - Connection: `OlistDW_OLEDB`
   - Data access mode: `SQL command`
   - SQL:

```sql
WITH LatestReviews AS (
    SELECT 
        order_id,
        review_score,
        -- Sắp xếp để lấy review mới nhất lên đầu (rn = 1)
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY review_creation_date DESC, review_answer_timestamp DESC
        ) AS rn
    FROM staging.stg_order_reviews
)
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
LEFT JOIN LatestReviews r
    ON o.order_id = r.order_id
    AND r.rn = 1 -- CHỈ LẤY REVIEW MỚI NHẤT
WHERE NOT EXISTS (
    SELECT 1 FROM gold.fact_orders f
    WHERE f.order_id = oi.order_id
      AND f.order_item_id = oi.order_item_id
);
```

2. Click **Preview** → lần đầu sẽ thấy toàn bộ ~112,650 dòng (vì fact_orders trống)
3. Click **OK**

> **Giải thích Incremental Load:** `NOT EXISTS` kiểm tra mỗi record `(order_id, order_item_id)` đã có trong `gold.fact_orders` chưa. Nếu chưa → đưa vào pipeline. Nếu rồi → bỏ qua. Lần chạy đầu → load tất cả. Lần chạy 2+ → chỉ load records mới (nếu có).

### 6.5. Component 2 – Lookup dim_customer

1. Kéo **Lookup** → nối từ OLE DB Source
2. **Tab General:** Redirect rows to no match output
3. **Tab Connection** – SQL command:

```sql
SELECT customer_key, customer_id
FROM gold.dim_customer
WHERE is_current = 1;
```

4. **Tab Columns:**
   - Join: `customer_id` → `customer_id`
   - Output: ✅ `customer_key` → Alias: `lkp_customer_key`
5. **OK**

**Xử lý No Match:**
1. Kéo **Derived Column** → nối từ Lookup **No Match Output**
2. Thêm cột: `lkp_customer_key` | Expression: `NULL(DT_I4)`
3. Kéo **Union All** → nối Match + No Match vào
4. Map `lkp_customer_key` từ cả 2 inputs

### 6.6. Component 3 – Lookup dim_seller

1. Kéo **Lookup** → nối từ Union All
2. **Tab General:** Redirect rows to no match output
3. **Tab Connection** – SQL command:

```sql
SELECT seller_key, seller_id
FROM gold.dim_seller
WHERE is_current = 1;
```

4. **Tab Columns:**
   - Join: `seller_id` → `seller_id`
   - Output: ✅ `seller_key` → Alias: `lkp_seller_key`
5. **OK**

**Xử lý No Match:** Tương tự – Derived Column (`NULL(DT_I4)`) + Union All.

### 6.7. Component 4 – Lookup dim_product

1. Kéo **Lookup** → nối từ Union All
2. **Tab General:** Redirect rows to no match output
3. **Tab Connection** – Table: `gold.dim_product`
4. **Tab Columns:**
   - Join: `product_id` → `product_id`
   - Output: ✅ `product_key` → Alias: `lkp_product_key`
5. **OK**

**Xử lý No Match:** Tương tự – Derived Column + Union All.

### 6.8. Component 5 – Derived Column (tính date_keys)

**Mục đích:** Chuyển timestamp sang format `YYYYMMDD` (integer) để match với `dim_date.date_key`.

1. Kéo **Derived Column** → nối từ Union All cuối cùng
2. Double-click → thêm 4 cột mới:

**Cột 1 – order_date_key:**

| Thuộc tính | Giá trị |
|---|---|
| Name | `calc_order_date_key` |
| Expression | `ISNULL(order_purchase_timestamp) ? (DT_I4)19000101 : (DT_I4)(YEAR(order_purchase_timestamp) * 10000 + MONTH(order_purchase_timestamp) * 100 + DAY(order_purchase_timestamp))` |

**Cột 2 – approved_date_key:**

| Thuộc tính | Giá trị |
|---|---|
| Name | `calc_approved_date_key` |
| Expression | `ISNULL(order_approved_at) ? NULL(DT_I4) : (DT_I4)(YEAR(order_approved_at) * 10000 + MONTH(order_approved_at) * 100 + DAY(order_approved_at))` |

**Cột 3 – delivered_date_key:**

| Thuộc tính | Giá trị |
|---|---|
| Name | `calc_delivered_date_key` |
| Expression | `ISNULL(order_delivered_customer_date) ? NULL(DT_I4) : (DT_I4)(YEAR(order_delivered_customer_date) * 10000 + MONTH(order_delivered_customer_date) * 100 + DAY(order_delivered_customer_date))` |

**Cột 4 – estimated_delivery_date_key:**

| Thuộc tính | Giá trị |
|---|---|
| Name | `calc_estimated_date_key` |
| Expression | `ISNULL(order_estimated_delivery_date) ? NULL(DT_I4) : (DT_I4)(YEAR(order_estimated_delivery_date) * 10000 + MONTH(order_estimated_delivery_date) * 100 + DAY(order_estimated_delivery_date))` |

> **Giải thích:** `YEAR * 10000 + MONTH * 100 + DAY` → ví dụ 2017-10-15 → 20171015. NULL timestamp → NULL date_key (trừ order_date dùng 19000101 làm default).

3. Click **OK**

### 6.9. Component 6 – OLE DB Destination (fact_orders)

1. Kéo **OLE DB Destination** → nối từ Derived Column
2. Double-click:
   - Connection: `OlistDW_OLEDB`
   - Table: `[gold].[fact_orders]`
3. **Tab Mappings:**

| Input Column | Destination Column |
|---|---|
| order_id | order_id |
| order_item_id | order_item_id |
| lkp_customer_key | customer_key |
| lkp_seller_key | seller_key |
| lkp_product_key | product_key |
| calc_order_date_key | order_date_key |
| calc_approved_date_key | approved_date_key |
| calc_delivered_date_key | delivered_date_key |
| calc_estimated_date_key | estimated_delivery_date_key |
| order_status | order_status |
| price | price |
| freight_value | freight_value |
| quantity | quantity |
| review_score | review_score |

> **Không map `fact_order_item_id`** vì là IDENTITY.

4. Click **OK**

### 6.10. Chạy test

1. Execute Package (F5)
2. Verify:

```sql
SELECT COUNT(*) AS total_rows FROM gold.fact_orders;
-- Expected: ~112,650
```

### 6.11. Test Incremental Load

**Chạy package LẦN 2:**
1. F5 lại
2. Quan sát Data Flow → OLE DB Source hiển thị **0 rows**
3. Verify: `SELECT COUNT(*) FROM gold.fact_orders;` → vẫn = ~112,650

**Nếu vẫn insert trùng:** Kiểm tra `UX_fact_orders_bk` index → nếu thiếu thì `NOT EXISTS` vẫn hoạt động nhưng chậm. Nếu bỏ WHERE NOT EXISTS → bị duplicate.

---
---

# PHẦN F – SSIS PACKAGE 4: AGGREGATED FACT_SALES

---

## BƯỚC 7: Xây dựng `Load_Fact_Sales.dtsx`

### 7.1. Điều kiện tiên quyết

- ✅ `gold.fact_orders` phải có dữ liệu (bạn vừa load ở Bước 6)
- ✅ `gold.dim_product` phải có dữ liệu (bạn load ở Bước 5)
- ✅ `gold.dim_date` phải có dữ liệu (TV1)

### 7.2. Mở Package → Control Flow Layout

```
┌──────────────────────────────────┐
│ EST - Truncate fact_sales        │
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│ DFT - Load fact_sales (daily)  │
└──────────────────────────────────┘

```

### 7.3. Task 1 – Execute SQL: Truncate fact_sales

1. Kéo **Execute SQL Task** → đổi tên `EST - Truncate fact_sales`
2. Connection: `OlistDW_OLEDB`
3. SQL:

```sql
TRUNCATE TABLE gold.fact_sales;
```

### 7.4. Task 2 – Data Flow: Load fact_sales (monthly)

1. Kéo **Data Flow Task** → đổi tên `DFT - Load fact_sales`
2. Nối: `EST - Truncate fact_sales` → `DFT - Load fact_sales`
3. Vào Data Flow:

**OLE DB Source:**
- Connection: `OlistDW_OLEDB`
- SQL command:

```sql
SELECT
    fo.seller_key,
    dp.category_key,
    dd.date_key, 
    SUM(fo.price + fo.freight_value)                AS total_revenue,
    COUNT(*)                                        AS total_items_sold,
    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    AVG(CAST(fo.review_score AS DECIMAL(3,2)))      AS avg_review_score
FROM gold.fact_orders fo
INNER JOIN gold.dim_product dp
    ON fo.product_key = dp.product_key
INNER JOIN gold.dim_date dd
    ON fo.order_date_key = dd.date_key
WHERE fo.seller_key IS NOT NULL
  AND dp.category_key IS NOT NULL
GROUP BY
    fo.seller_key,
    dp.category_key,
    dd.date_key; 
```

> **Nếu lỗi do fact_orders phụ thuộc bảng khác:** Query này join `gold.fact_orders` (bạn) + `gold.dim_product` (bạn) + `gold.dim_date` (TV1). Tất cả phải có dữ liệu.

**OLE DB Destination:**
1. Kéo → nối từ OLE DB Source
2. Table: `[gold].[fact_sales]`
3. Mappings:

| Input Column | Destination Column |
|---|---|
| seller_key | seller_key |
| category_key | category_key |
| date_key | date_key |
| total_revenue | total_revenue |
| total_items_sold | total_items_sold |
| total_orders | total_orders |
| avg_review_score | avg_review_score |

4. **OK**

### 7.4. Chạy test

```sql
SELECT 'fact_sales'      AS tbl, COUNT(*) AS cnt FROM gold.fact_sales
UNION ALL
SELECT 'fact_sales_year',        COUNT(*) FROM gold.fact_sales_year;
```

---
---

# PHẦN G – SQL TRUY VẤN PHÂN TÍCH

---

## BƯỚC 8: Viết 3 câu truy vấn insight

### Query 1: Top 10 danh mục sản phẩm doanh thu cao nhất

```sql
SELECT TOP 10
    pc.category_name_english,
    SUM(fs.total_revenue) AS revenue,
    SUM(fs.total_items_sold) AS items_sold,
    SUM(fs.total_orders) AS orders,
    AVG(fs.avg_review_score) AS avg_review
FROM gold.fact_sales fs
INNER JOIN gold.dim_product_category pc
    ON fs.category_key = pc.category_key
GROUP BY pc.category_name_english
ORDER BY revenue DESC;
```

### Query 2: Xu hướng doanh thu theo tháng (time series)

```sql
SELECT
    dd.year,git config core.autocrlf false
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

### Query 3: So sánh doanh thu theo kích thước sản phẩm (nhẹ vs nặng)

```sql
SELECT
    CASE
        WHEN dp.product_weight_g < 500 THEN 'Light (<500g)'
        WHEN dp.product_weight_g < 2000 THEN 'Medium (500g-2kg)'
        WHEN dp.product_weight_g < 10000 THEN 'Heavy (2-10kg)'
        ELSE 'Very Heavy (>10kg)'
    END AS weight_segment,
    COUNT(*) AS total_items,
    SUM(fo.price) AS total_revenue,
    AVG(fo.price) AS avg_price,
    AVG(fo.freight_value) AS avg_freight,
    AVG(CAST(fo.review_score AS DECIMAL(3,2))) AS avg_review
FROM gold.fact_orders fo
INNER JOIN gold.dim_product dp ON fo.product_key = dp.product_key
WHERE dp.product_weight_g > 0
GROUP BY
    CASE
        WHEN dp.product_weight_g < 500 THEN 'Light (<500g)'
        WHEN dp.product_weight_g < 2000 THEN 'Medium (500g-2kg)'
        WHEN dp.product_weight_g < 10000 THEN 'Heavy (2-10kg)'
        ELSE 'Very Heavy (>10kg)'
    END
ORDER BY total_revenue DESC;
```

---
---

# PHẦN H – VERIFY & TEST

---

## BƯỚC 9: Kiểm tra toàn bộ

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
-- Ghi nhận count trước
SELECT COUNT(*) AS before_count FROM gold.fact_orders;

-- Chạy lại Load_Fact_Orders.dtsx

SELECT COUNT(*) AS after_count FROM gold.fact_orders;
-- before_count PHẢI = after_count (0 rows mới)
```

### 9.3. Kiểm tra SCD Type 1

```sql
-- Chạy lại Load_Dim_Product.dtsx
-- Nếu staging không thay đổi → 0 rows updated, 0 rows inserted
-- Kiểm tra: dim_product count không đổi
```

### 9.4. Kiểm tra data integrity

```sql
-- Tổng revenue phải khớp giữa fact_orders và staging
SELECT SUM(price + freight_value) AS fact_total
FROM gold.fact_orders;

SELECT SUM(CAST(price AS DECIMAL(14,2)) + CAST(freight_value AS DECIMAL(14,2))) AS staging_total
FROM staging.stg_order_items;
-- Hai giá trị phải bằng nhau

-- Kiểm tra FK integrity
SELECT 'orphan_customer' AS issue, COUNT(*)
FROM gold.fact_orders f
LEFT JOIN gold.dim_customer d ON f.customer_key = d.customer_key
WHERE f.customer_key IS NOT NULL AND d.customer_key IS NULL
UNION ALL
SELECT 'orphan_product', COUNT(*)
FROM gold.fact_orders f
LEFT JOIN gold.dim_product d ON f.product_key = d.product_key
WHERE f.product_key IS NOT NULL AND d.product_key IS NULL
UNION ALL
SELECT 'orphan_seller', COUNT(*)
FROM gold.fact_orders f
LEFT JOIN gold.dim_seller d ON f.seller_key = d.seller_key
WHERE f.seller_key IS NOT NULL AND d.seller_key IS NULL;
-- Tất cả phải = 0
```

### 9.5. Kiểm tra fact_sales consistency

```sql
-- Tổng revenue trong fact_sales phải khớp với fact_orders
-- (chỉ tính records có seller_key và category_key NOT NULL)
SELECT SUM(total_revenue) AS sales_total FROM gold.fact_sales;

SELECT SUM(fo.price + fo.freight_value) AS orders_total
FROM gold.fact_orders fo
INNER JOIN gold.dim_product dp ON fo.product_key = dp.product_key
WHERE fo.seller_key IS NOT NULL AND dp.category_key IS NOT NULL;
-- Hai giá trị phải bằng nhau
```

---
---

# CHECKLIST HOÀN THÀNH TV2

- [ ] **DDL:** 3 staging tables tạo thành công
- [ ] **DDL:** 3 dimension tables tạo thành công
- [ ] **DDL:** 3 fact tables tạo thành công (bao gồm indexes)
- [ ] **Package 1:** `Extract_Product_Items.dtsx` – tất cả DFT xanh, row counts đúng
- [ ] **Package 2:** `Load_Dim_Product.dtsx` – dim_payment_method populated (5 rows), SCD Type 1 hoạt động cho dim_product_category và dim_product
- [ ] **Package 3:** `Load_Fact_Orders.dtsx` – Incremental Load hoạt động (chạy lần 2 = 0 new rows)
- [ ] **Package 4:** `Load_Fact_Sales.dtsx` – fact_sales + fact_sales_year load thành công
- [ ] **SCD Type 1:** dim_product_category UPDATE khi tên English thay đổi
- [ ] **SCD Type 1:** dim_product UPDATE khi weight/dimensions thay đổi
- [ ] **Incremental Load:** fact_orders không insert trùng khi chạy lại
- [ ] **SQL Queries:** 3 queries chạy đúng, trả về insight hợp lý
- [ ] **Data Integrity:** Revenue totals khớp giữa staging ↔ fact_orders ↔ fact_sales
- [ ] **FK Integrity:** Không có orphan foreign keys

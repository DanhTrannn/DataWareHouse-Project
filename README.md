# 🏗️ Data Warehouse Design – Gold Layer

### Phân tích Doanh thu theo Sản phẩm & Hiệu suất Bán hàng

**Nguồn dữ liệu:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## 1. Tổng quan kiến trúc

```
Nguồn thô (Kaggle CSV)
        │
        ▼
┌─────────────────┐
│  Staging Area    │  SSIS import CSV → SQL Server staging tables
│  schema: staging │  Raw data, truncate-reload mỗi lần chạy
└───────┬─────────┘
        │
        ▼
┌─────────────────┐
│  Bronze Layer    │  Raw ingestion – giữ nguyên schema gốc + metadata
│  schema: bronze  │  Thêm cột: _loaded_at, _source_file, _batch_id
└───────┬─────────┘
        │
        ▼
┌─────────────────┐
│  Silver Layer    │  Cleansing, deduplication, type casting, join đơn giản
│  schema: silver  │  Derived columns: delay_days, lead_time, category_group...
└───────┬─────────┘
        │
        ▼
┌─────────────────┐
│  Gold Layer      │  Star Schema – tối ưu cho analytical queries
│  schema: gold    │  Fact + Dimension tables → phục vụ BI, dashboard, báo cáo
└─────────────────┘
```

**Công nghệ:** SQL Server + SSIS (SQL Server Integration Services)
**Tần suất refresh:** Daily batch (dữ liệu Olist là historical, không cần streaming)

---

## 2. Nguồn dữ liệu gốc

Dataset Olist gồm **9 bảng CSV** liên kết qua các khóa tự nhiên:

| # | File CSV gốc | Mô tả | Cột chính |
|---|---|---|---|
| 1 | `olist_orders_dataset.csv` | Thông tin đơn hàng, timestamps, trạng thái | `order_id`, `customer_id`, `order_status`, 5 timestamps |
| 2 | `olist_order_items_dataset.csv` | Chi tiết từng item trong đơn | `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight_value` |
| 3 | `olist_order_payments_dataset.csv` | Thanh toán (nhiều method/order) | `order_id`, `payment_type`, `payment_value`, `payment_installments` |
| 4 | `olist_order_reviews_dataset.csv` | Đánh giá khách hàng per order | `order_id`, `review_score`, `review_comment_title` |
| 5 | `olist_customers_dataset.csv` | Thông tin khách hàng | `customer_id`, `customer_unique_id`, `zip_code_prefix`, `city`, `state` |
| 6 | `olist_sellers_dataset.csv` | Thông tin người bán | `seller_id`, `zip_code_prefix`, `city`, `state` |
| 7 | `olist_products_dataset.csv` | Thuộc tính sản phẩm | `product_id`, `category_name`, dimensions, weight |
| 8 | `product_category_name_translation.csv` | Dịch category tiếng Anh | `category_name`, `category_name_english` |
| 9 | `olist_geolocation_dataset.csv` | Tọa độ GPS theo zip code | `zip_code_prefix`, `lat`, `lng`, `city`, `state` |

### Quan hệ dữ liệu gốc

```
olist_orders (1) ──< (N) olist_order_items       -- 1 order có nhiều items
olist_orders (1) ──< (N) olist_order_payments    -- 1 order có thể nhiều payment method
olist_orders (1) ──< (1) olist_order_reviews     -- 1 order có 0 hoặc 1 review
olist_customers (1) ──< (N) olist_orders         -- 1 customer có nhiều orders (qua customer_unique_id)
olist_sellers (1) ──< (N) olist_order_items      -- 1 seller bán nhiều items
olist_products (1) ──< (N) olist_order_items     -- 1 product xuất hiện trong nhiều items
```

---

## 3. Kiến trúc ETL (SSIS)

### 3.1. Tổng quan Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                    SSIS Master Package                       │
│                    (Control Flow)                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │ Step 1   │───>│ Step 2   │───>│ Step 3   │              │
│  │ Extract  │    │ Transform│    │ Load     │              │
│  │ to       │    │ Silver   │    │ Gold     │              │
│  │ Staging  │    │ Layer    │    │ Layer    │              │
│  └──────────┘    └──────────┘    └──────────┘              │
│       │               │               │                     │
│  Flat File Src   Data Conversion  Lookup Transform          │
│  → OLE DB Dest   Derived Column   Conditional Split         │
│                  Data Cleaning    OLE DB Destination         │
│                  Deduplication    Surrogate Key Gen          │
└─────────────────────────────────────────────────────────────┘
```

### 3.2. Staging Area

Mỗi bảng CSV được import vào staging table tương ứng trong SQL Server. Staging sử dụng **truncate-reload** mỗi batch:

| Staging Table | Source CSV | Ghi chú |
|---|---|---|
| `staging.stg_orders` | `olist_orders_dataset.csv` | Truncate trước khi load |
| `staging.stg_order_items` | `olist_order_items_dataset.csv` | Truncate trước khi load |
| `staging.stg_order_payments` | `olist_order_payments_dataset.csv` | Truncate trước khi load |
| `staging.stg_order_reviews` | `olist_order_reviews_dataset.csv` | Truncate trước khi load |
| `staging.stg_customers` | `olist_customers_dataset.csv` | Truncate trước khi load |
| `staging.stg_sellers` | `olist_sellers_dataset.csv` | Truncate trước khi load |
| `staging.stg_products` | `olist_products_dataset.csv` | Truncate trước khi load |
| `staging.stg_category_translation` | `product_category_name_translation.csv` | Truncate trước khi load |
| `staging.stg_geolocation` | `olist_geolocation_dataset.csv` | Truncate trước khi load |

### 3.3. Control Flow vs Data Flow

**Control Flow** (thứ tự thực thi):
1. Execute SQL Task: Truncate staging tables
2. Data Flow Task: Load CSV → Staging (9 luồng song song)
3. Execute SQL Task: Run cleansing scripts (Bronze → Silver)
4. Data Flow Task: Load Dimensions (Lookup + SCD)
5. Data Flow Task: Load Fact tables (Lookup foreign keys)
6. Execute SQL Task: Load Aggregated Facts (SQL-based)

**Data Flow** (luồng dữ liệu trong mỗi task):
- Flat File Source → Data Conversion → Derived Column → OLE DB Destination
- OLE DB Source → Lookup → Conditional Split → OLE DB Destination (cho SCD)
- OLE DB Source → Aggregate → OLE DB Destination (cho periodic snapshots)

### 3.4. Incremental Load & Change Detection

| Đối tượng | Chiến lược | Cơ chế detect change |
|---|---|---|
| **Staging tables** | Truncate-Reload | Không cần CDC, luôn reload toàn bộ CSV |
| **Dimension tables** | SCD (xem mục 5) | Lookup trên business key, so sánh cột thay đổi |
| **fact_orders** | Incremental Insert | So sánh `order_id + order_item_id` với bảng đích; chỉ insert dòng mới bằng `LEFT JOIN ... WHERE target.PK IS NULL` |
| **fact_order_lifecycle** | Upsert (Insert/Update) | Lookup `order_id`; nếu tồn tại → update timestamps mới; nếu chưa → insert |
| **Periodic Snapshots** | Rebuild theo tháng/năm | Xóa partition tháng hiện tại, tính lại từ fact_orders |

**SSIS Components cho Incremental Load:**
- **Lookup Transform**: Kiểm tra record đã tồn tại trong đích chưa
- **Conditional Split**: Phân luồng New / Changed / Unchanged
- **OLE DB Command**: Thực hiện UPDATE cho record thay đổi
- **OLE DB Destination**: INSERT cho record mới

---

## 4. Bảng Dimension

### 4.1. Danh sách Dimension

| Dimension | Mô tả | PK | Các cột chính |
|-----------|-------|----|----------------|
| `dim_date` | Lịch ngày, hỗ trợ phân tích thời gian | `date_key` | `full_date`, `year`, `quarter`, `month`, `month_name`, `day_of_week`, `is_weekend`, `is_holiday_brazil`, `season_brazil` |
| `dim_geolocation` | Địa lý theo mã zip | `geo_key` | `zip_code_prefix`, `city`, `state`, `region`, `latitude`, `longitude` |
| `dim_customer` | Khách hàng | `customer_key` | `customer_id`, `customer_unique_id`, `city`, `state`, `geo_key` (FK) |
| `dim_seller` | Người bán | `seller_key` | `seller_id`, `city`, `state`, `geo_key` (FK), `seller_region` |
| `dim_product_category` | Danh mục sản phẩm | `category_key` | `category_name_english`, `category_name_portuguese` |
| `dim_product` | Sản phẩm | `product_key` | `product_id`, `category_key` (FK), `product_name_length`, `product_description_length`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` |
| `dim_order_status` | Trạng thái đơn hàng | `order_status` | `description` |
| `dim_payment_method` | Phương thức thanh toán | `payment_type` | `description` |

### 4.2. Slowly Changing Dimension (SCD)

| Dimension | Loại SCD | Lý do | Cột áp dụng | Cách xử lý trong SSIS |
|-----------|----------|-------|-------------|----------------------|
| `dim_date` | **Không SCD** | Bảng tĩnh, sinh sẵn 1 lần | Toàn bộ | Pre-populate bằng script SQL |
| `dim_geolocation` | **Type 1** (Overwrite) | Tọa độ GPS có thể cập nhật chính xác hơn, không cần lưu lịch sử | `latitude`, `longitude`, `city` | SSIS Lookup → nếu khác → OLE DB Command UPDATE |
| `dim_customer` | **Type 2** (History) | Khách hàng có thể chuyển thành phố/state, cần phân tích theo vị trí tại thời điểm mua | `city`, `state`, `geo_key` | Thêm cột: `effective_from`, `effective_to`, `is_current`. SSIS SCD Transform hoặc Lookup + Conditional Split |
| `dim_seller` | **Type 2** (History) | Seller có thể thay đổi vị trí kinh doanh | `city`, `state`, `geo_key` | Tương tự dim_customer |
| `dim_product` | **Type 1** (Overwrite) | Thuộc tính vật lý (weight, dimensions) là bản chất cố định, nếu sai thì sửa | `product_weight_g`, dimensions | SSIS Lookup → UPDATE nếu khác |
| `dim_product_category` | **Type 1** (Overwrite) | Tên danh mục ít thay đổi, nếu sửa thì cập nhật | `category_name_english` | SSIS Lookup → UPDATE nếu khác |
| `dim_order_status` | **Không SCD** | Giá trị cố định (delivered, shipped, canceled...) | Toàn bộ | Pre-populate |
| `dim_payment_method` | **Không SCD** | Giá trị cố định (credit_card, boleto, voucher...) | Toàn bộ | Pre-populate |

**Cột bổ sung cho SCD Type 2:**

```sql
-- dim_customer (SCD Type 2)
ALTER TABLE gold.dim_customer ADD
    effective_from DATE NOT NULL DEFAULT '1900-01-01',
    effective_to   DATE NOT NULL DEFAULT '9999-12-31',
    is_current     BIT  NOT NULL DEFAULT 1;

-- dim_seller (SCD Type 2)
ALTER TABLE gold.dim_seller ADD
    effective_from DATE NOT NULL DEFAULT '1900-01-01',
    effective_to   DATE NOT NULL DEFAULT '9999-12-31',
    is_current     BIT  NOT NULL DEFAULT 1;
```

---

## 5. Hệ thống phân cấp (Hierarchy)

### 5.1. Geography Hierarchy

```
Region (Vùng: Sudeste, Sul, Nordeste, Norte, Centro-Oeste)
  └── State (Bang: SP, RJ, MG, ...)
        └── City (Thành phố: São Paulo, Rio de Janeiro, ...)
              └── Zip Code Prefix (Mã bưu điện: 01000, 02000, ...)
```

Áp dụng cho cả `dim_customer` và `dim_seller` thông qua FK `geo_key` → `dim_geolocation`.

Cột `region` trong `dim_geolocation` được derive từ `state`:
- **Sudeste**: SP, RJ, MG, ES
- **Sul**: PR, SC, RS
- **Nordeste**: BA, PE, CE, MA, PB, RN, AL, PI, SE
- **Norte**: AM, PA, RO, TO, AC, AP, RR
- **Centro-Oeste**: GO, MT, MS, DF

### 5.2. Product Hierarchy

```
Product Category (dim_product_category)
  └── Product (dim_product)
```

### 5.3. Time Hierarchy

```
Year
  └── Quarter (Q1, Q2, Q3, Q4)
        └── Month (01–12)
              └── Day (full_date)
```

Tất cả level nằm trong `dim_date`, hỗ trợ drill-down/drill-up trong BI tools.

### 5.4. Order Lifecycle Hierarchy

```
Order Status (dim_order_status)
  └── Milestone: Created → Approved → Shipped → Delivered
```

Phục vụ phân tích funnel và bottleneck trong `fact_order_lifecycle`.

---

## 6. Bảng Fact

### 6.1. Transaction Fact – `fact_orders`

- **Grain**: Mỗi dòng = một sản phẩm trong một đơn hàng (`order_id` + `order_item_id`)
- **Các cột**:

| Cột | Kiểu | Vai trò |
|-----|------|---------|
| `fact_order_item_id` | INT IDENTITY | PK (surrogate) |
| `order_id` | VARCHAR | Degenerate dimension |
| `order_item_id` | INT | Degenerate dimension |
| `customer_key` | INT | FK → `dim_customer` |
| `seller_key` | INT | FK → `dim_seller` |
| `product_key` | INT | FK → `dim_product` |
| `order_date_key` | INT | FK → `dim_date` |
| `approved_date_key` | INT | FK → `dim_date` |
| `delivered_date_key` | INT | FK → `dim_date` |
| `estimated_delivery_date_key` | INT | FK → `dim_date` |
| `order_status` | VARCHAR | FK → `dim_order_status` |
| `price` | DECIMAL(10,2) | Measure – giá sản phẩm |
| `freight_value` | DECIMAL(10,2) | Measure – phí vận chuyển |
| `quantity` | INT | Measure – số lượng (luôn = 1 trong dataset Olist) |
| `review_score` | INT | Measure – điểm đánh giá (1–5) |

### 6.2. Periodic Snapshot – Month

| Fact Table | Grain | FK Dimensions | Measures |
|---|---|---|---|
| `fact_sales` | seller + category + tháng | `seller_key`, `category_key`, `date_key` | `total_revenue`, `total_items_sold`, `total_orders`, `avg_review_score` |
| `fact_delivery` | seller + tháng | `seller_key`, `date_key` | `total_orders_delivered`, `on_time_orders`, `on_time_rate` |
| `fact_payment_trends` | payment_type + tháng | `payment_type`, `date_key` | `total_payment_value`, `transaction_count`, `order_count` |
| `fact_customer_orders` | customer + order_status + tháng | `customer_key`, `order_status`, `date_key` | `total_orders`, `total_items`, `total_spent`, `avg_review_score` |

### 6.3. Periodic Snapshot – Year

Tương tự các bảng tháng nhưng grain theo năm, tên có hậu tố `_year`:
`fact_sales_year`, `fact_delivery_year`, `fact_payment_trends_year`, `fact_customer_orders_year`

### 6.4. Accumulating Snapshot – `fact_order_lifecycle`

- **Grain**: Mỗi dòng = một đơn hàng (`order_id`)
- **Các cột**:

| Cột | Kiểu | Vai trò |
|-----|------|---------|
| `fact_lifecycle_id` | INT IDENTITY | PK |
| `order_id` | VARCHAR | Degenerate dimension |
| `customer_key` | INT | FK → `dim_customer` |
| `seller_key` | INT | FK → `dim_seller` |
| `order_date` | DATE | Milestone timestamp |
| `approved_date` | DATE | Milestone timestamp |
| `delivered_date` | DATE | Milestone timestamp |
| `estimated_delivery_date` | DATE | Milestone timestamp |
| `days_to_approve` | INT | Derived measure |
| `days_to_delivery` | INT | Derived measure |
| `is_delayed` | BIT | Flag: delivered_date > estimated_delivery_date |
| `order_status` | VARCHAR | FK → `dim_order_status` |

---

## 7. Liên kết giữa các bảng (Star Schema Diagram)

```
                        ┌──────────────┐
                        │  dim_date    │
                        │  (date_key)  │
                        └──────┬───────┘
                               │
     ┌──────────────┐    ┌─────┴──────┐    ┌───────────────────┐
     │dim_customer  │───>│            │<───│ dim_product        │
     │(customer_key)│    │fact_orders │    │ (product_key)      │
     └──────┬───────┘    │            │    └────────┬───────────┘
            │            └─────┬──────┘             │
     ┌──────┴───────┐         │            ┌────────┴───────────┐
     │dim_geolocation│   ┌────┴─────┐     │dim_product_category│
     │(geo_key)      │   │dim_seller│     │(category_key)      │
     └───────────────┘   │(seller_key)    └────────────────────┘
                         └──────┬───┘
                                │
                         ┌──────┴───────┐
                         │dim_geolocation│
                         └───────────────┘
```

### Foreign Key Summary

**fact_orders:**
- `customer_key` → `dim_customer.customer_key`
- `seller_key` → `dim_seller.seller_key`
- `product_key` → `dim_product.product_key`
- `order_date_key`, `approved_date_key`, `delivered_date_key`, `estimated_delivery_date_key` → `dim_date.date_key`
- `order_status` → `dim_order_status.order_status`

**fact_sales:** `seller_key` → dim_seller, `category_key` → dim_product_category, `date_key` → dim_date

**fact_delivery:** `seller_key` → dim_seller, `date_key` → dim_date

**fact_payment_trends:** `payment_type` → dim_payment_method, `date_key` → dim_date

**fact_customer_orders:** `customer_key` → dim_customer, `order_status` → dim_order_status, `date_key` → dim_date

**fact_order_lifecycle:** `customer_key` → dim_customer, `seller_key` → dim_seller

**Snowflake extensions:** `dim_customer.geo_key` → dim_geolocation, `dim_seller.geo_key` → dim_geolocation, `dim_product.category_key` → dim_product_category

---

## 8. Tối ưu hiệu năng

| Kỹ thuật | Áp dụng |
|---|---|
| **Staging tables** | Tách biệt vùng đệm, tránh lock bảng chính khi import |
| **Batch Processing** | SSIS Data Flow dùng buffer size phù hợp, DefaultBufferMaxRows = 10000 |
| **Lookup Cache** | Full Cache cho dim nhỏ (dim_date, dim_order_status, dim_payment_method); Partial Cache cho dim lớn (dim_customer) |
| **Index** | Clustered index trên PK; Non-clustered index trên FK columns trong fact tables |
| **Parallel Execution** | Các Data Flow Task load dimension chạy song song (MaxConcurrentExecutables) |
| **Partition** | Fact tables partition theo `order_date_key` (tháng) cho periodic snapshot rebuild |

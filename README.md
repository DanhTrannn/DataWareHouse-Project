# 🏗️ Data Warehouse Design – Gold Layer
### Phân tích Doanh thu theo Sản phẩm & Hiệu suất Bán hàng
**Nguồn dữ liệu:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Nguồn dữ liệu gốc](#2-nguồn-dữ-liệu-gốc)
3. [Gold Layer – Star Schema](#3-gold-layer--star-schema)
4. [Fact Tables](#4-fact-tables)
   - [fact_orders](#41-factorders)
   - [fact_payments](#42-factpayments)
5. [Dimension Tables](#5-dimension-tables)
   - [dim_date](#51-dimdate)
   - [dim_customer](#52-dimcustomer)
   - [dim_seller](#53-dimseller)
   - [dim_product](#54-dimproduct)
   - [dim_order](#55-dimorder)
   - [dim_geography](#56-dimgeography)
   - [dim_payment_method](#57-dimpaymentmethod)
6. [Luồng dữ liệu (Data Lineage)](#6-luồng-dữ-liệu-data-lineage)
7. [KPIs & Metrics mục tiêu](#7-kpis--metrics-mục-tiêu)
8. [Mẫu câu truy vấn phân tích](#8-mẫu-câu-truy-vấn-phân-tích)
9. [Quyết định thiết kế & Trade-offs](#9-quyết-định-thiết-kế--trade-offs)
10. [Checklist triển khai](#10-checklist-triển-khai)

---

## 1. Tổng quan kiến trúc

```
Nguồn thô (Kaggle CSV)
        │
        ▼
┌───────────────┐
│  Bronze Layer │  Raw ingestion – không transform, giữ nguyên schema gốc
│  schema: raw  │  Bảng: olist_orders, olist_order_items, olist_products, ...
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Silver Layer │  Cleansing, deduplication, type casting, join đơn giản
│ schema: silver│  Tính derived columns: delay_days, lead_time, category_group, ...
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Gold Layer   │  Star Schema – tối ưu cho analytical queries
│ schema: gold  │  Fact + Dimension tables → phục vụ BI, dashboard, báo cáo
└───────────────┘
```

**Công nghệ tham khảo:** dbt + BigQuery / Snowflake / PostgreSQL  
**Tần suất refresh:** Daily batch (dữ liệu Olist là historical, không cần streaming)

---

## 2. Nguồn dữ liệu gốc

Dataset Olist gồm **9 bảng CSV** liên kết qua các khóa tự nhiên:

| File CSV gốc | Mô tả | Số cột chính |
|---|---|---|
| `olist_orders_dataset.csv` | Thông tin đơn hàng, timestamps, trạng thái | `order_id`, `customer_id`, `order_status`, 5 timestamp |
| `olist_order_items_dataset.csv` | Chi tiết từng item trong đơn | `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight_value` |
| `olist_order_payments_dataset.csv` | Thanh toán (có thể nhiều method/order) | `order_id`, `payment_type`, `payment_value`, `payment_installments` |
| `olist_order_reviews_dataset.csv` | Đánh giá khách hàng per order | `order_id`, `review_score`, `review_comment_title` |
| `olist_customers_dataset.csv` | Thông tin khách hàng | `customer_id`, `customer_unique_id`, `zip_code_prefix`, `city`, `state` |
| `olist_sellers_dataset.csv` | Thông tin người bán | `seller_id`, `zip_code_prefix`, `city`, `state` |
| `olist_products_dataset.csv` | Thuộc tính sản phẩm | `product_id`, `category_name`, dimensions, weight |
| `product_category_name_translation.csv` | Dịch category tiếng Anh | `category_name`, `category_name_english` |
| `olist_geolocation_dataset.csv` | Tọa độ GPS theo zip code | `zip_code_prefix`, `lat`, `lng`, `city`, `state` |

### Lưu ý quan trọng về quan hệ dữ liệu

```
olist_orders (1) ──< (N) olist_order_items       -- 1 order có nhiều items
olist_orders (1) ──< (N) olist_order_payments    -- 1 order có thể có nhiều payment method
olist_orders (1) ──< (1) olist_order_reviews     -- 1 order có 0 hoặc 1 review
olist_customers (1) ──< (N) olist_orders         -- 1 customer có nhiều orders (qua customer_unique_id)
olist_sellers (1) ──< (N) olist_order_items      -- 1 seller bán nhiều items
olist_products (1) ──< (N) olist_order_items     -- 1 product xuất hiện trong nhiều items
```

---

## 3. Gold Layer – Star Schema

### Sơ đồ tổng thể

```
                        ┌─────────────────┐
                        │   dim_date      │
                        │  (date_key PK)  │
                        └────────┬────────┘
                                 │ order_date_key
                                 │ approved_date_key
             ┌───────────────────┼──────────────────────┐
             │                   │                       │
   ┌─────────┴───────┐  ┌────────▼──────────┐  ┌───────┴───────────┐
   │  dim_customer   │  │   fact_orders     │  │   dim_seller      │
   │ (customer_key)  ├──┤  (GRAIN: 1 row    ├──┤  (seller_key PK)  │
   └─────────────────┘  │  = 1 order_item)  │  └───────────────────┘
                        │                   │
   ┌─────────────────┐  │  order_key (FK)   │  ┌───────────────────┐
   │  dim_product    ├──┤  product_key (FK) ├──┤   dim_order       │
   │ (product_key PK)│  │  seller_key (FK)  │  │  (order_key PK)   │
   └─────────────────┘  │  customer_key(FK) │  └───────────────────┘
                        │  date_keys (FK×4) │
                        └────────┬──────────┘
                                 │ order_id (shared key)
                        ┌────────▼──────────┐
                        │  fact_payments    │
                        │ (GRAIN: 1 row     │
                        │  = 1 payment rec) │
                        └────────┬──────────┘
                                 │
                        ┌────────▼──────────┐
                        │ dim_payment_method│
                        │(payment_method_key│
                        └───────────────────┘

   dim_geography ◄── (dim_customer.geo_key, dim_seller.geo_key)
```

### Lý do chọn Star Schema

- **Query đơn giản:** Analyst không cần hiểu mô hình phức tạp – chỉ cần JOIN fact với dim.
- **Hiệu năng cao:** Ít JOIN hơn Snowflake schema; optimizer của các OLAP engine (BigQuery, Snowflake, Redshift) tối ưu tốt cho star schema.
- **Phù hợp BI tools:** Tableau, Power BI, Looker đều ưu tiên star schema.
- **Denormalized dims** là chấp nhận được ở Gold layer vì ưu tiên tốc độ đọc.

---

## 4. Fact Tables

### 4.1 `fact_orders`

**Grain:** Một dòng = một order item (`order_item_id`)  
**Mục đích:** Phân tích doanh thu theo sản phẩm, seller, thời gian; đo hiệu suất giao hàng

```sql
CREATE TABLE gold.fact_orders (
    -- Surrogate key
    fact_order_key          BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural keys (giữ lại để trace về Silver/Bronze)
    order_id                VARCHAR(50)     NOT NULL,
    order_item_id           INT             NOT NULL,           -- thứ tự item trong đơn (1, 2, 3...)

    -- Foreign keys → Dimension tables
    order_key               INT             NOT NULL,           -- FK → dim_order
    customer_key            INT             NOT NULL,           -- FK → dim_customer
    seller_key              INT             NOT NULL,           -- FK → dim_seller
    product_key             INT             NOT NULL,           -- FK → dim_product
    order_date_key          INT             NOT NULL,           -- FK → dim_date
    approved_date_key       INT,                                -- FK → dim_date (NULL nếu chưa approved)
    delivered_date_key      INT,                                -- FK → dim_date (NULL nếu chưa giao)
    estimated_delivery_key  INT,                                -- FK → dim_date

    -- Measures – Revenue
    price                   NUMERIC(12, 2)  NOT NULL,           -- giá sản phẩm
    freight_value           NUMERIC(12, 2)  NOT NULL DEFAULT 0, -- phí vận chuyển
    total_item_revenue      NUMERIC(12, 2)  NOT NULL            -- = price + freight_value
        GENERATED ALWAYS AS (price + freight_value) STORED,

    -- Measures – Delivery performance
    approval_lead_time_days INT,            -- approved_date - order_date (ngày)
    shipping_lead_time_days INT,            -- delivered_date - approved_date (ngày)
    delivery_delay_days     INT,            -- delivered_date - estimated_delivery_date
                                            -- âm = giao sớm, dương = giao trễ
    estimated_shipping_days INT,            -- estimated_delivery_date - order_date

    -- Flags
    is_late_delivery        BOOLEAN         NOT NULL DEFAULT FALSE,
                                            -- TRUE nếu delivery_delay_days > 0
    is_freight_free         BOOLEAN         NOT NULL DEFAULT FALSE,
                                            -- TRUE nếu freight_value = 0

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

> **Lưu ý thiết kế:**
> - `payment_value` và `review_score` **không** có trong bảng này.
>   - `payment_value` → `fact_payments` (vì một order có nhiều payment records)
>   - `review_score` → `dim_order` (vì review là per order, không per item)
> - Các NULL dates (chưa approved, chưa giao) phải trỏ về surrogate key `-1` trong `dim_date` với giá trị "N/A" – không để NULL FK để tránh lỗi aggregation.
> - `delivery_delay_days` âm = giao hàng sớm hơn dự kiến (tốt); dương = trễ (cần theo dõi).

**Indexes:**
```sql
CREATE INDEX idx_fact_orders_order_date    ON gold.fact_orders (order_date_key);
CREATE INDEX idx_fact_orders_product       ON gold.fact_orders (product_key);
CREATE INDEX idx_fact_orders_seller        ON gold.fact_orders (seller_key);
CREATE INDEX idx_fact_orders_customer      ON gold.fact_orders (customer_key);
CREATE INDEX idx_fact_orders_order_id      ON gold.fact_orders (order_id);
```

---

### 4.2 `fact_payments`

**Grain:** Một dòng = một payment record  
**Lý do tách riêng:** Olist cho phép nhiều phương thức thanh toán trên cùng một order (VD: vừa dùng credit card vừa dùng voucher). Nếu đặt `payment_value` vào `fact_orders` theo item grain sẽ gây **double-counting** khi SUM.

```sql
CREATE TABLE gold.fact_payments (
    -- Surrogate key
    fact_payment_key        BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural key
    order_id                VARCHAR(50)     NOT NULL,           -- join về fact_orders qua order_id
    payment_sequential      INT             NOT NULL,           -- thứ tự thanh toán trong đơn

    -- Foreign keys
    payment_method_key      INT             NOT NULL,           -- FK → dim_payment_method
    order_date_key          INT             NOT NULL,           -- FK → dim_date (để phân tích time-series)

    -- Measures
    payment_value           NUMERIC(12, 2)  NOT NULL,
    payment_installments    INT             NOT NULL DEFAULT 1, -- số kỳ trả góp (1 = trả một lần)

    -- Derived
    avg_installment_value   NUMERIC(12, 2)                      -- = payment_value / payment_installments
        GENERATED ALWAYS AS (
            CASE WHEN payment_installments > 0
                 THEN payment_value / payment_installments
                 ELSE payment_value
            END
        ) STORED,
    is_installment          BOOLEAN         NOT NULL DEFAULT FALSE,
                                            -- TRUE nếu payment_installments > 1

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

> **Cách JOIN khi phân tích:**
> ```sql
> -- Doanh thu theo phương thức thanh toán + category sản phẩm
> SELECT
>     pm.payment_type,
>     p.category_group,
>     SUM(fp.payment_value)  AS total_payment_value,
>     COUNT(DISTINCT fp.order_id) AS order_count
> FROM gold.fact_payments fp
> JOIN gold.dim_payment_method pm ON fp.payment_method_key = pm.payment_method_key
> JOIN gold.fact_orders fo        ON fp.order_id = fo.order_id
> JOIN gold.dim_product p         ON fo.product_key = p.product_key
> GROUP BY pm.payment_type, p.category_group;
> ```

**Indexes:**
```sql
CREATE INDEX idx_fact_payments_order_id    ON gold.fact_payments (order_id);
CREATE INDEX idx_fact_payments_method      ON gold.fact_payments (payment_method_key);
CREATE INDEX idx_fact_payments_date        ON gold.fact_payments (order_date_key);
```

---

## 5. Dimension Tables

### 5.1 `dim_date`

**Mục đích:** Trục thời gian cho tất cả phân tích doanh thu theo ngày/tuần/tháng/quý/năm

```sql
CREATE TABLE gold.dim_date (
    date_key                INT             PRIMARY KEY,        -- format YYYYMMDD, VD: 20180315
    full_date               DATE            NOT NULL,

    -- Năm/Quý/Tháng/Tuần
    year                    SMALLINT        NOT NULL,
    quarter                 SMALLINT        NOT NULL,           -- 1–4
    quarter_name            VARCHAR(6)      NOT NULL,           -- 'Q1 18', 'Q2 18', ...
    month                   SMALLINT        NOT NULL,           -- 1–12
    month_name              VARCHAR(20)     NOT NULL,           -- 'January', 'Fevereiro', ...
    month_name_short        CHAR(3)         NOT NULL,           -- 'Jan', 'Feb', ...
    week_of_year            SMALLINT        NOT NULL,           -- ISO week 1–53
    day_of_year             SMALLINT        NOT NULL,           -- 1–366
    day_of_month            SMALLINT        NOT NULL,           -- 1–31
    day_of_week             SMALLINT        NOT NULL,           -- 1 (Mon) – 7 (Sun)
    day_name                VARCHAR(10)     NOT NULL,           -- 'Monday', ...
    day_name_short          CHAR(3)         NOT NULL,           -- 'Mon', ...

    -- Flags hữu ích
    is_weekend              BOOLEAN         NOT NULL,
    is_weekday              BOOLEAN         NOT NULL,
    is_month_start          BOOLEAN         NOT NULL,
    is_month_end            BOOLEAN         NOT NULL,
    is_quarter_start        BOOLEAN         NOT NULL,
    is_quarter_end          BOOLEAN         NOT NULL,

    -- Đặc thù Brazil (quan trọng cho seasonal analysis)
    is_holiday_brazil       BOOLEAN         NOT NULL DEFAULT FALSE,
    holiday_name_brazil     VARCHAR(100),                       -- NULL nếu không phải holiday
    -- Mùa theo Nam bán cầu (Brazil):
    -- Summer: Dec–Feb | Autumn: Mar–May | Winter: Jun–Aug | Spring: Sep–Nov
    season_brazil           VARCHAR(10)     NOT NULL,           -- 'Summer', 'Autumn', 'Winter', 'Spring'

    -- Fiscal (nếu cần – tham khảo năm tài chính Brazil: Jan–Dec)
    fiscal_year             SMALLINT        NOT NULL,
    fiscal_quarter          SMALLINT        NOT NULL,

    -- Relative (tính từ ngày hiện tại, refresh daily)
    is_current_day          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_current_month        BOOLEAN         NOT NULL DEFAULT FALSE,
    is_current_year         BOOLEAN         NOT NULL DEFAULT FALSE,
    days_ago                INT,                                -- số ngày tính từ hôm nay

    -- Special row
    -- date_key = -1: "Unknown / N/A" – dùng cho FK của NULL dates trong fact_orders
    CONSTRAINT chk_dim_date_quarter    CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT chk_dim_date_month      CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_dim_date_dow        CHECK (day_of_week BETWEEN 1 AND 7)
);
```

> **Phạm vi:** Tạo từ **2015-01-01 đến 2020-12-31** (bao phủ dữ liệu Olist 2016-2018 + buffer).  
> **Dòng đặc biệt:** Chèn thêm dòng `date_key = -1` với `full_date = '1900-01-01'`, `month_name = 'N/A'` để FK không bao giờ NULL.

---

### 5.2 `dim_customer`

**Mục đích:** Phân tích doanh thu theo địa lý khách hàng, phân khúc khách hàng lặp lại

```sql
CREATE TABLE gold.dim_customer (
    customer_key            INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural keys
    customer_id             VARCHAR(50)     NOT NULL UNIQUE,    -- ID per order (Olist tạo mới mỗi order)
    customer_unique_id      VARCHAR(50)     NOT NULL,           -- ID thực của người mua (dùng để đếm khách quay lại)

    -- Location
    zip_code_prefix         VARCHAR(10),
    city                    VARCHAR(100),
    state                   CHAR(2),                            -- VD: 'SP', 'RJ', 'MG'
    geo_key                 INT,                                -- FK → dim_geography (tọa độ GPS)

    -- Derived – Region Brazil
    -- Theo phân vùng IBGE:
    --   Đông Nam: SP, RJ, MG, ES
    --   Nam: RS, SC, PR
    --   Đông Bắc: BA, PE, CE, MA, PB, RN, AL, SE, PI
    --   Trung Tây: GO, MT, MS, DF
    --   Bắc: AM, PA, RO, AC, AP, RR, TO
    customer_region         VARCHAR(20),                        -- 'Southeast', 'South', 'Northeast', ...

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

> **Quan trọng:** Trong Olist, `customer_id` được tạo mới cho mỗi order (để ẩn danh). Dùng `customer_unique_id` để phân tích hành vi khách hàng lặp lại (repeat purchase rate).

---

### 5.3 `dim_seller`

**Mục đích:** Phân tích hiệu suất seller, doanh thu theo vùng địa lý seller

```sql
CREATE TABLE gold.dim_seller (
    seller_key              INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural key
    seller_id               VARCHAR(50)     NOT NULL UNIQUE,

    -- Location
    zip_code_prefix         VARCHAR(10),
    city                    VARCHAR(100),
    state                   CHAR(2),
    geo_key                 INT,                                -- FK → dim_geography
    seller_region           VARCHAR(20),                        -- 'Southeast', 'South', ...

    -- Derived – Performance tiers (tính từ Silver, cập nhật định kỳ)
    -- Có thể NULL nếu seller quá mới (< 30 ngày dữ liệu)
    avg_review_score        NUMERIC(3, 2),                      -- trung bình review score của seller
    total_orders_count      INT             DEFAULT 0,
    seller_tier             VARCHAR(10),                        -- 'Gold', 'Silver', 'Bronze'
                                                                -- Dựa trên avg_review + total_orders

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

---

### 5.4 `dim_product`

**Mục đích:** Phân tích doanh thu theo category, nhóm sản phẩm, đặc điểm vật lý (liên quan phí ship)

```sql
CREATE TABLE gold.dim_product (
    product_key             INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural key
    product_id              VARCHAR(50)     NOT NULL UNIQUE,

    -- Category
    category_name_pt        VARCHAR(100),                       -- tên gốc tiếng Bồ Đào Nha
    category_name_en        VARCHAR(100),                       -- tên tiếng Anh (sau khi join translation)

    -- Category Group – do Data team định nghĩa, nhóm 73 categories thành ~8 nhóm
    -- phục vụ phân tích cấp cao (CEO dashboard, strategic report)
    category_group          VARCHAR(50),
    -- Mapping đề xuất:
    --   'Electronics'       ← computers_accessories, electronics, telephony, ...
    --   'Home & Living'     ← furniture_decor, bed_bath_table, home_appliances, ...
    --   'Health & Beauty'   ← health_beauty, perfumery
    --   'Fashion'           ← fashion_*, luggage_accessories, watches_gifts
    --   'Sports & Outdoors' ← sports_leisure, garden_tools
    --   'Toys & Games'      ← toys, baby, games
    --   'Food & Beverages'  ← food, drinks, food_security
    --   'Books & Media'     ← books_general_interest, books_technical, music, dvds_blu_ray
    --   'Other'             ← phần còn lại

    -- Physical attributes (ảnh hưởng freight_value)
    weight_g                INT,                                -- trọng lượng gram
    length_cm               INT,
    height_cm               INT,
    width_cm                INT,
    volume_cm3              INT                                 -- = length × height × width
        GENERATED ALWAYS AS (
            COALESCE(length_cm, 0) * COALESCE(height_cm, 0) * COALESCE(width_cm, 0)
        ) STORED,

    -- Size tier – phân loại kích thước để phân tích mối quan hệ với freight
    size_tier               VARCHAR(5),
    -- Logic phân loại:
    --   'XS': volume_cm3 < 500
    --   'S':  500 ≤ volume < 5000
    --   'M':  5000 ≤ volume < 25000
    --   'L':  25000 ≤ volume < 100000
    --   'XL': volume ≥ 100000

    -- Content (tùy chọn – dữ liệu Olist thường không đầy đủ)
    product_name_length     INT,                                -- số ký tự tên sản phẩm
    product_description_length INT,
    product_photos_qty      INT,

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

---

### 5.5 `dim_order`

**Mục đích:** Chứa các thuộc tính cấp order (không phải cấp item); bao gồm trạng thái đơn hàng và review

```sql
CREATE TABLE gold.dim_order (
    order_key               INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural key
    order_id                VARCHAR(50)     NOT NULL UNIQUE,

    -- Order status
    order_status            VARCHAR(20)     NOT NULL,
    -- Các giá trị Olist: created, approved, invoiced, processing,
    --                    shipped, delivered, unavailable, canceled

    -- Review (per order – không phải per item)
    review_score            SMALLINT,                           -- 1–5, NULL nếu không có review
    review_has_comment      BOOLEAN         NOT NULL DEFAULT FALSE,
    review_comment_title    VARCHAR(255),
    review_creation_date    DATE,                               -- ngày khách tạo review
    review_answer_date      DATE,                               -- ngày seller trả lời (nếu có)

    -- Order-level flags
    is_canceled             BOOLEAN         NOT NULL DEFAULT FALSE,
    is_delivered            BOOLEAN         NOT NULL DEFAULT FALSE,
    is_reviewed             BOOLEAN         NOT NULL DEFAULT FALSE,
    is_late_delivery        BOOLEAN         NOT NULL DEFAULT FALSE, -- tổng hợp từ fact_orders

    -- Order size
    order_items_count       INT             NOT NULL DEFAULT 1,     -- số lượng items trong đơn
    order_sellers_count     INT             NOT NULL DEFAULT 1,     -- số sellers trong đơn
    order_total_price       NUMERIC(12, 2),                         -- SUM(price) của các items
    order_total_freight     NUMERIC(12, 2),                         -- SUM(freight_value)
    order_total_payment     NUMERIC(12, 2),                         -- SUM(payment_value) từ fact_payments

    -- SCD Type – dùng Type 1 (overwrite) vì chỉ cần trạng thái cuối cùng
    -- Nếu cần lịch sử trạng thái → chuyển sang SCD Type 2 với valid_from/valid_to

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

> **Tại sao `review_score` ở đây, không ở `fact_orders`?**  
> Dataset Olist: 1 order → 0 hoặc 1 review. Fact_orders grain là per item. Nếu 1 order có 3 items thì `review_score` xuất hiện 3 lần → `AVG(review_score)` bị sai. Đặt ở `dim_order` là chuẩn xác.

---

### 5.6 `dim_geography`

**Mục đích:** Dùng chung cho `dim_customer` và `dim_seller`; chứa tọa độ GPS để phân tích địa lý, tính khoảng cách

```sql
CREATE TABLE gold.dim_geography (
    geo_key                 INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural key
    zip_code_prefix         VARCHAR(10)     NOT NULL UNIQUE,

    -- Location
    city                    VARCHAR(100),
    state                   CHAR(2)         NOT NULL,
    state_name              VARCHAR(50),                        -- 'São Paulo', 'Rio de Janeiro', ...
    state_abbr              CHAR(2),                            -- = state

    -- Coordinates (trung bình của các điểm GPS trong Olist geolocation)
    latitude                NUMERIC(10, 6),
    longitude               NUMERIC(10, 6),

    -- Region phân loại theo IBGE
    region                  VARCHAR(20),                        -- 'Southeast', 'South', 'Northeast', 'Midwest', 'North'

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

> **Cách dùng:**
> ```sql
> -- Khoảng cách giữa customer và seller (Haversine) có thể tính trực tiếp
> -- qua geo_key của dim_customer và dim_seller
> SELECT
>     c.geo_key AS customer_geo,
>     s.geo_key AS seller_geo,
>     -- Sử dụng lat/lng từ dim_geography để tính distance
>     earth_distance(
>         ll_to_earth(cg.latitude, cg.longitude),
>         ll_to_earth(sg.latitude, sg.longitude)
>     ) / 1000 AS distance_km
> FROM gold.dim_customer c
> JOIN gold.dim_geography cg ON c.geo_key = cg.geo_key
> JOIN gold.dim_seller s     ON ...
> JOIN gold.dim_geography sg ON s.geo_key = sg.geo_key;
> ```

---

### 5.7 `dim_payment_method`

**Mục đích:** Phân tích doanh thu theo phương thức thanh toán  
**Liên kết với:** `fact_payments` (không phải `fact_orders`)

```sql
CREATE TABLE gold.dim_payment_method (
    payment_method_key      INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- Natural key
    payment_type            VARCHAR(30)     NOT NULL UNIQUE,
    -- Các giá trị Olist: credit_card, boleto, voucher, debit_card, not_defined

    -- Attributes
    payment_type_label      VARCHAR(50)     NOT NULL,           -- nhãn hiển thị đẹp hơn
    -- VD: 'Credit Card', 'Boleto Bancário', 'Voucher', 'Debit Card'

    is_digital              BOOLEAN         NOT NULL DEFAULT TRUE,
    -- FALSE chỉ với boleto (thanh toán tại ngân hàng/cửa hàng)

    supports_installment    BOOLEAN         NOT NULL DEFAULT FALSE,
    -- TRUE với credit_card (Olist data có installments up to 24 kỳ)

    payment_category        VARCHAR(20),
    -- 'Card', 'Bank Transfer', 'Voucher', 'Other'

    -- Metadata
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Seed data
INSERT INTO gold.dim_payment_method
    (payment_type, payment_type_label, is_digital, supports_installment, payment_category)
VALUES
    ('credit_card',  'Credit Card',      TRUE,  TRUE,  'Card'),
    ('boleto',       'Boleto Bancário',  FALSE, FALSE, 'Bank Transfer'),
    ('voucher',      'Voucher',          TRUE,  FALSE, 'Voucher'),
    ('debit_card',   'Debit Card',       TRUE,  FALSE, 'Card'),
    ('not_defined',  'Not Defined',      FALSE, FALSE, 'Other');
```

---

## 6. Luồng dữ liệu (Data Lineage)

### Từ Silver → Gold: `fact_orders`

```sql
INSERT INTO gold.fact_orders (...)
SELECT
    oi.order_id,
    oi.order_item_id,

    -- FK lookups
    do_.order_key,
    dc.customer_key,
    ds.seller_key,
    dp.product_key,
    dd_order.date_key        AS order_date_key,
    COALESCE(dd_appr.date_key,  -1) AS approved_date_key,
    COALESCE(dd_deliv.date_key, -1) AS delivered_date_key,
    COALESCE(dd_est.date_key,   -1) AS estimated_delivery_key,

    -- Measures
    oi.price,
    oi.freight_value,

    -- Derived delivery metrics
    EXTRACT(DAY FROM (o.order_approved_at - o.order_purchase_timestamp))::INT
        AS approval_lead_time_days,

    EXTRACT(DAY FROM (o.order_delivered_customer_date - o.order_approved_at))::INT
        AS shipping_lead_time_days,

    EXTRACT(DAY FROM (
        o.order_delivered_customer_date - o.order_estimated_delivery_date
    ))::INT AS delivery_delay_days,

    EXTRACT(DAY FROM (
        o.order_estimated_delivery_date - o.order_purchase_timestamp
    ))::INT AS estimated_shipping_days,

    -- Flags
    CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
         THEN TRUE ELSE FALSE END AS is_late_delivery,

    CASE WHEN oi.freight_value = 0
         THEN TRUE ELSE FALSE END AS is_freight_free

FROM silver.order_items oi
JOIN silver.orders o          ON oi.order_id = o.order_id
JOIN gold.dim_order do_       ON oi.order_id = do_.order_id
JOIN gold.dim_customer dc     ON o.customer_id = dc.customer_id
JOIN gold.dim_seller ds       ON oi.seller_id = ds.seller_id
JOIN gold.dim_product dp      ON oi.product_id = dp.product_id
JOIN gold.dim_date dd_order   ON o.order_purchase_timestamp::DATE = dd_order.full_date
LEFT JOIN gold.dim_date dd_appr  ON o.order_approved_at::DATE = dd_appr.full_date
LEFT JOIN gold.dim_date dd_deliv ON o.order_delivered_customer_date::DATE = dd_deliv.full_date
LEFT JOIN gold.dim_date dd_est   ON o.order_estimated_delivery_date::DATE = dd_est.full_date;
```

### Từ Silver → Gold: `fact_payments`

```sql
INSERT INTO gold.fact_payments (...)
SELECT
    p.order_id,
    p.payment_sequential,
    pm.payment_method_key,
    dd.date_key                 AS order_date_key,
    p.payment_value,
    p.payment_installments,
    CASE WHEN p.payment_installments > 1 THEN TRUE ELSE FALSE END AS is_installment
FROM silver.order_payments p
JOIN gold.dim_payment_method pm ON p.payment_type = pm.payment_type
JOIN silver.orders o            ON p.order_id = o.order_id
JOIN gold.dim_date dd           ON o.order_purchase_timestamp::DATE = dd.full_date;
```

---

## 7. KPIs & Metrics mục tiêu

### Doanh thu (Revenue)

| KPI | Công thức | Bảng nguồn |
|---|---|---|
| **GMV (Gross Merchandise Value)** | `SUM(price)` | `fact_orders` |
| **Total Revenue** | `SUM(price + freight_value)` | `fact_orders` |
| **Total Payment Received** | `SUM(payment_value)` | `fact_payments` |
| **AOV (Average Order Value)** | `SUM(price) / COUNT(DISTINCT order_id)` | `fact_orders` |
| **Revenue by Category Group** | GROUP BY `dim_product.category_group` | `fact_orders` + `dim_product` |
| **Revenue by State/Region** | GROUP BY `dim_customer.state` | `fact_orders` + `dim_customer` |
| **Revenue by Month** | GROUP BY `dim_date.year, dim_date.month` | `fact_orders` + `dim_date` |
| **Revenue by Payment Method** | GROUP BY `dim_payment_method.payment_type` | `fact_payments` + `dim_payment_method` |
| **Freight Revenue %** | `SUM(freight_value) / SUM(price+freight_value)` | `fact_orders` |

### Hiệu suất Bán hàng (Sales Performance)

| KPI | Công thức | Bảng nguồn |
|---|---|---|
| **Order Count** | `COUNT(DISTINCT order_id)` | `fact_orders` |
| **Item Count** | `COUNT(*)` | `fact_orders` |
| **Units per Order** | `COUNT(*) / COUNT(DISTINCT order_id)` | `fact_orders` |
| **Seller GMV** | `SUM(price)` GROUP BY `seller_key` | `fact_orders` + `dim_seller` |
| **Top Sellers by Revenue** | RANK() OVER `SUM(price)` | `fact_orders` + `dim_seller` |
| **Category Penetration** | `COUNT(DISTINCT order_id)` per `category_group` | `fact_orders` + `dim_product` |
| **Repeat Purchase Rate** | `COUNT` customers với >1 order / total customers | `fact_orders` + `dim_customer` |
| **Cancellation Rate** | `COUNT(canceled orders) / COUNT(all orders)` | `dim_order` |

### Hiệu suất Giao hàng (Delivery Performance)

| KPI | Công thức | Bảng nguồn |
|---|---|---|
| **On-Time Delivery Rate** | `COUNT(is_late=FALSE) / COUNT(*)` | `fact_orders` |
| **Average Delivery Delay** | `AVG(delivery_delay_days)` WHERE delivered | `fact_orders` |
| **Late Delivery Rate by Seller** | GROUP BY `seller_key`, `is_late_delivery` | `fact_orders` + `dim_seller` |
| **Late Delivery Rate by Region** | GROUP BY `dim_seller.seller_region` | `fact_orders` + `dim_seller` |
| **Avg Approval Lead Time** | `AVG(approval_lead_time_days)` | `fact_orders` |
| **Avg Shipping Lead Time** | `AVG(shipping_lead_time_days)` | `fact_orders` |

### Customer Satisfaction (Review)

| KPI | Công thức | Bảng nguồn |
|---|---|---|
| **Average Review Score** | `AVG(review_score)` | `dim_order` |
| **Review Rate** | `COUNT(is_reviewed=TRUE) / COUNT(*)` | `dim_order` |
| **Score Distribution** | GROUP BY `review_score` | `dim_order` |
| **Review Score vs Delivery** | JOIN `dim_order` + `fact_orders` | Cả hai |

---

## 8. Mẫu câu truy vấn phân tích

### Q1 – Doanh thu theo Category Group và Tháng (2018)

```sql
SELECT
    d.year,
    d.month,
    d.month_name_short,
    p.category_group,
    SUM(fo.price)                           AS gmv,
    SUM(fo.price + fo.freight_value)        AS total_revenue,
    COUNT(DISTINCT fo.order_id)             AS order_count,
    ROUND(AVG(fo.price), 2)                AS avg_item_price
FROM gold.fact_orders fo
JOIN gold.dim_date    d  ON fo.order_date_key = d.date_key
JOIN gold.dim_product p  ON fo.product_key    = p.product_key
WHERE d.year = 2018
  AND p.category_group IS NOT NULL
GROUP BY d.year, d.month, d.month_name_short, p.category_group
ORDER BY d.month, total_revenue DESC;
```

---

### Q2 – Top 10 Seller theo GMV với Delivery Score

```sql
SELECT
    s.seller_id,
    s.state,
    s.seller_region,
    SUM(fo.price)                               AS gmv,
    COUNT(DISTINCT fo.order_id)                 AS orders,
    ROUND(AVG(CASE WHEN fo.is_late_delivery THEN 0 ELSE 1 END) * 100, 1)
                                                AS on_time_pct,
    ROUND(AVG(do_.review_score), 2)             AS avg_review,
    ROUND(AVG(fo.delivery_delay_days), 1)       AS avg_delay_days,
    RANK() OVER (ORDER BY SUM(fo.price) DESC)   AS revenue_rank
FROM gold.fact_orders fo
JOIN gold.dim_seller s   ON fo.seller_key  = s.seller_key
JOIN gold.dim_order  do_ ON fo.order_id    = do_.order_id
WHERE fo.delivered_date_key != -1          -- chỉ các đơn đã giao
GROUP BY s.seller_id, s.state, s.seller_region
ORDER BY gmv DESC
LIMIT 10;
```

---

### Q3 – Phân tích Phương thức Thanh toán theo Region Khách hàng

```sql
SELECT
    c.customer_region,
    pm.payment_type_label,
    COUNT(DISTINCT fp.order_id)             AS order_count,
    SUM(fp.payment_value)                   AS total_payment,
    ROUND(AVG(fp.payment_installments), 1) AS avg_installments,
    ROUND(
        100.0 * SUM(fp.payment_value) /
        SUM(SUM(fp.payment_value)) OVER (PARTITION BY c.customer_region),
        1
    )                                       AS payment_share_pct
FROM gold.fact_payments fp
JOIN gold.dim_payment_method pm ON fp.payment_method_key = pm.payment_method_key
JOIN gold.fact_orders fo        ON fp.order_id           = fo.order_id
JOIN gold.dim_customer c        ON fo.customer_key       = c.customer_key
GROUP BY c.customer_region, pm.payment_type_label
ORDER BY c.customer_region, total_payment DESC;
```

---

### Q4 – On-Time Delivery Rate theo Tháng và Seller Region

```sql
SELECT
    d.year,
    d.month,
    d.month_name_short,
    s.seller_region,
    COUNT(*)                                                    AS total_deliveries,
    SUM(CASE WHEN fo.is_late_delivery THEN 1 ELSE 0 END)       AS late_count,
    ROUND(
        100.0 * SUM(CASE WHEN fo.is_late_delivery THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                                           AS late_rate_pct,
    ROUND(AVG(fo.delivery_delay_days), 1)                      AS avg_delay_days,
    ROUND(AVG(fo.shipping_lead_time_days), 1)                  AS avg_ship_days
FROM gold.fact_orders fo
JOIN gold.dim_date   d ON fo.order_date_key  = d.date_key
JOIN gold.dim_seller s ON fo.seller_key      = s.seller_key
WHERE fo.delivered_date_key != -1
  AND d.year IN (2017, 2018)
GROUP BY d.year, d.month, d.month_name_short, s.seller_region
ORDER BY d.year, d.month, late_rate_pct DESC;
```

---

### Q5 – Revenue heatmap: Category × State

```sql
SELECT
    p.category_group,
    c.state,
    SUM(fo.price)               AS gmv,
    COUNT(DISTINCT fo.order_id) AS orders,
    RANK() OVER (
        PARTITION BY c.state
        ORDER BY SUM(fo.price) DESC
    )                           AS category_rank_in_state
FROM gold.fact_orders fo
JOIN gold.dim_product  p ON fo.product_key  = p.product_key
JOIN gold.dim_customer c ON fo.customer_key = c.customer_key
WHERE p.category_group IS NOT NULL
GROUP BY p.category_group, c.state
ORDER BY c.state, gmv DESC;
```

---

## 9. Quyết định thiết kế & Trade-offs

### 9.1 Tại sao tách `fact_payments` thay vì gộp vào `fact_orders`?

Olist cho phép **nhiều payment methods trên cùng một order**. Ví dụ, một đơn hàng có thể dùng 50% credit card + 50% voucher → 2 rows trong `olist_order_payments`. Nếu giữ `payment_value` trong `fact_orders` (grain = item):

- Order có 2 items × 2 payment records = 4 rows → `SUM(payment_value)` bị gấp đôi thực tế.
- Không thể phân tích payment method nếu một order dùng nhiều method.

**Giải pháp:** `fact_payments` với grain riêng, JOIN qua `order_id` tự nhiên.

### 9.2 SCD Type 1 cho hầu hết dimensions

Dữ liệu Olist là **historical snapshot** (không có streaming). SCD Type 1 (overwrite) phù hợp vì:
- Không cần lịch sử thay đổi của customer address hay product category.
- Giảm độ phức tạp vận hành.

**Ngoại lệ:** Nếu sau này cần track `order_status` changes → `dim_order` chuyển sang SCD Type 2.

### 9.3 `dim_geography` dùng chung vs inline trong dim_customer/dim_seller

**Trade-off:** Dimension dùng chung (shared) tốt về DRY nhưng thêm 1 JOIN khi query.

**Quyết định:** Dùng chung `dim_geography` vì:
- Tọa độ GPS không thay đổi theo customer/seller.
- Hỗ trợ spatial analysis (khoảng cách, clustering).
- Tránh duplicate ~100k rows GPS giữa customer và seller tables.

### 9.4 Surrogate keys thay vì Natural keys

- Natural keys Olist là UUID string (~50 chars) → index size lớn, JOIN chậm hơn.
- Surrogate INT key giảm storage, tăng tốc JOIN đáng kể trên large datasets.
- Giữ natural key (customer_id, order_id...) như cột thường để trace về nguồn.

### 9.5 Computed/Derived columns ở đâu?

| Metric | Tính ở đâu | Lý do |
|---|---|---|
| `delivery_delay_days` | Silver → Gold | Cần trong nhiều queries |
| `is_late_delivery` | Silver → Gold | Thường xuyên dùng làm filter |
| `category_group` | Silver (mapping table) | Business rule, cần versioning |
| `seller_tier` | Gold (dim_seller) | Phụ thuộc aggregate từ fact |
| `volume_cm3` | Gold (generated column) | Deterministic, không cần Silver |
| `total_item_revenue` | Gold (generated column) | Deterministic |

---

## 10. Checklist triển khai

### Phase 1 – Foundation

- [ ] Tạo schema `gold` trong database
- [ ] Tạo `dim_date` và populate 2015–2020
- [ ] Chèn row "Unknown" (`date_key = -1`) vào `dim_date`
- [ ] Tạo `dim_geography` và load từ `olist_geolocation_dataset.csv`
  - Deduplicate: nhiều coordinates cho cùng 1 zip_code → dùng trung bình

### Phase 2 – Dimensions

- [ ] Tạo và populate `dim_payment_method` (seed data 5 rows)
- [ ] Tạo và populate `dim_product` (bao gồm `category_name_en`, `category_group`, `size_tier`)
- [ ] Tạo và populate `dim_customer` (bao gồm `customer_region`, `geo_key`)
- [ ] Tạo và populate `dim_seller` (bao gồm `seller_region`, `geo_key`)
- [ ] Tạo và populate `dim_order` (bao gồm `review_score` từ join với olist_order_reviews)
- [ ] Validate referential integrity: mọi FK đều có giá trị tương ứng

### Phase 3 – Fact Tables

- [ ] Tạo `fact_orders` và load data
- [ ] Verify: `SUM(price)` khớp với Silver layer
- [ ] Verify: không có NULL FK (trừ các date FK đã dùng `-1`)
- [ ] Tạo `fact_payments` và load data
- [ ] Verify: `SUM(payment_value)` per order_id ≈ `SUM(price + freight)` trong fact_orders
  (thường gần bằng nhưng không chính xác 100% do voucher/discount)
- [ ] Tạo tất cả indexes

### Phase 4 – Validation

- [ ] Chạy Q1–Q5 mẫu, kiểm tra kết quả có hợp lý
- [ ] Kiểm tra `COUNT(DISTINCT order_id)` trong `fact_orders` = tổng số orders
- [ ] Kiểm tra `is_late_delivery` trong `fact_orders` = `is_late_delivery` trong `dim_order`
- [ ] Verify `avg_review_score` per seller tính từ `dim_order` JOIN `fact_orders`
- [ ] Document anomalies: `~97k` orders, `~32k` sellers, `~32k` products (theo dataset Olist 2018)

### Phase 5 – Documentation & Governance

- [ ] Cập nhật data catalog với mô tả từng cột
- [ ] Định nghĩa `category_group` mapping và lưu vào Silver layer config
- [ ] Thiết lập data quality rules (dbt tests hoặc Great Expectations)
- [ ] Cấu hình monitoring: daily row count, null rate, duplicate check

---

*Tài liệu này được tạo cho mục đích phân tích doanh thu theo sản phẩm và hiệu suất bán hàng trên dataset Olist Brazilian E-Commerce.*  
*Phiên bản: 1.0 – Gold Layer Star Schema Design*

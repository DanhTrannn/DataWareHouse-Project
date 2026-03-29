# 🏗️ Data Warehouse Design – Gold Layer
### Phân tích Doanh thu theo Sản phẩm & Hiệu suất Bán hàng
**Nguồn dữ liệu:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---


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


## 3. Bảng Dimension

| Dimension | Mô tả | Các cột chính |
|-----------|-------|----------------|
| `dim_date` | Ngày tháng, hỗ trợ phân tích thời gian | `date_key` (PK), `full_date`, `year`, `quarter`, `month`, `month_name`, `is_weekend`, `is_holiday_brazil`, `season_brazil` |
| `dim_geolocation` | Địa lý theo mã zip | `geo_key` (PK), `zip_code_prefix`, `city`, `state`, `region`, `latitude`, `longitude` |
| `dim_customer` | Khách hàng | `customer_key` (PK), `customer_id`, `customer_unique_id`, `city`, `state`, `geo_key` (FK → `dim_geolocation`) |
| `dim_seller` | Người bán | `seller_key` (PK), `seller_id`, `city`, `state`, `geo_key` (FK → `dim_geolocation`), `seller_region` |
| `dim_product_category` | Danh mục sản phẩm | `category_key` (PK), `category_name_english`, `category_name_portuguese` |
| `dim_product` | Sản phẩm | `product_key` (PK), `product_id`, `category_key` (FK → `dim_product_category`), `product_name_length`, `product_description_length`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm` |
| `dim_order_status` | Trạng thái đơn hàng | `order_status` (PK), `description` |
| `dim_payment_method` | Phương thức thanh toán | `payment_type` (PK), `description` |

## 4. Bảng Fact

### 4.1. Transaction Fact – `fact_orders`

- **Grain**: Mỗi dòng = một sản phẩm trong một đơn hàng (`order_id` + `order_item_id`)
- **Các cột**:
  - `fact_order_item_id` (PK)
  - `order_id`, `order_item_id`
  - `customer_key` (FK → `dim_customer`)
  - `seller_key` (FK → `dim_seller`)
  - `product_key` (FK → `dim_product`)
  - `order_date_key`, `approved_date_key`, `delivered_date_key`, `estimated_delivery_date_key` (FK → `dim_date`)
  - `price`, `freight_value`, `quantity`, `review_score`, `order_status`
- **Mục đích**: Lưu chi tiết giao dịch, phục vụ truy vấn ad‑hoc, phân tích phân phối, và là nguồn cho các bảng tổng hợp.

### 4.2. Periodic Snapshot – Month

| Fact | Grain | Measures | Mục đích |
|------|-------|----------|----------|
| `fact_sales` | seller + category + tháng | `total_revenue`, `total_items_sold`, `total_orders`, `avg_review_score` | Doanh thu, số lượng bán, đánh giá theo seller, danh mục, tháng |
| `fact_delivery` | seller + tháng | `total_orders_delivered`, `on_time_orders`, `on_time_rate` | Tỷ lệ giao hàng đúng hạn, hiệu suất seller |
| `fact_payment_trends` | payment_type + tháng | `total_payment_value`, `transaction_count`, `order_count` | Xu hướng sử dụng phương thức thanh toán |
| `fact_customer_orders` | customer + order_status + tháng | `total_orders`, `total_items`, `total_spent`, `avg_review_score` | Hành vi khách hàng (chi tiêu, số đơn, đánh giá) theo trạng thái |

### 4.3. Periodic Snapshot – Year

Các bảng tương tự như trên nhưng grain theo năm, giúp truy vấn nhanh phân tích dài hạn:

- `fact_sales_year`
- `fact_delivery_year`
- `fact_payment_trends_year`
- `fact_customer_orders_year`

### 4.4. Accumulating Snapshot – `fact_order_lifecycle`

- **Grain**: Mỗi dòng = một đơn hàng (`order_id`)
- **Các cột**:
  - `fact_lifecycle_id` (PK)
  - `order_id`
  - `customer_key` (FK → `dim_customer`)
  - `seller_key` (FK → `dim_seller`)
  - `order_date`, `approved_date`, `delivered_date`, `estimated_delivery_date`
  - `days_to_approve`, `days_to_delivery`, `is_delayed`, `order_status`
- **Mục đích**: Theo dõi vòng đời đơn hàng, tính thời gian xử lý, tỷ lệ giao muộn, phân tích bottleneck.

## 5. Liên kết giữa các bảng

Dưới đây là các quan hệ khóa ngoại chính:

- `dim_customer.geo_key` → `dim_geolocation.geo_key`
- `dim_seller.geo_key` → `dim_geolocation.geo_key`
- `dim_product.category_key` → `dim_product_category.category_key`

- `fact_orders.customer_key` → `dim_customer.customer_key`
- `fact_orders.seller_key` → `dim_seller.seller_key`
- `fact_orders.product_key` → `dim_product.product_key`
- `fact_orders.order_date_key` (và các date_key khác) → `dim_date.date_key`

- `fact_sales.seller_key` → `dim_seller.seller_key`
- `fact_sales.category_key` → `dim_product_category.category_key`
- `fact_sales.date_key` → `dim_date.date_key`

- `fact_delivery.seller_key` → `dim_seller.seller_key`
- `fact_delivery.date_key` → `dim_date.date_key`

- `fact_payment_trends.payment_type` → `dim_payment_method.payment_type`
- `fact_payment_trends.date_key` → `dim_date.date_key`

- `fact_customer_orders.customer_key` → `dim_customer.customer_key`
- `fact_customer_orders.order_status` → `dim_order_status.order_status`
- `fact_customer_orders.date_key` → `dim_date.date_key`

- `fact_order_lifecycle.customer_key` → `dim_customer.customer_key`
- `fact_order_lifecycle.seller_key` → `dim_seller.seller_key`

## 6. Câu hỏi kinh doanh và fact tương ứng

| Câu hỏi | Fact sử dụng |
|---------|--------------|
| Doanh thu theo tháng, theo danh mục sản phẩm? | `fact_sales` |
| Seller nào có doanh thu cao nhất trong tháng 1/2018? | `fact_sales` |
| Tỷ lệ giao hàng đúng hạn của từng seller? | `fact_delivery` |
| Phương thức thanh toán phổ biến nhất theo từng tháng? | `fact_payment_trends` |
| Khách hàng ở bang nào chi tiêu nhiều nhất? | `fact_customer_orders` + `dim_customer` |
| Đơn hàng bị hủy thường có đặc điểm gì? | `fact_customer_orders` (lọc `order_status='canceled'`) |
| Thời gian trung bình từ đặt hàng đến giao thành công? | `fact_order_lifecycle` |
| Sản phẩm nào có điểm đánh giá thấp nhất? | `fact_orders` (group by product) |
| Phân phối giá bán (histogram)? | `fact_orders` |
| So sánh doanh thu 2017 và 2018? | `fact_sales_year` |

## 7. Sơ đồ quan hệ

```mermaid
erDiagram
    %% Dimension tables
    dim_date {
        INT date_key PK
        DATE full_date
        SMALLINT year
        SMALLINT quarter
        SMALLINT month
        VARCHAR month_name
        BOOLEAN is_weekend
        BOOLEAN is_holiday_brazil
        VARCHAR season_brazil
    }

    dim_geolocation {
        INT geo_key PK
        VARCHAR zip_code_prefix
        VARCHAR city
        VARCHAR state
        VARCHAR region
        DECIMAL latitude
        DECIMAL longitude
    }

    dim_customer {
        INT customer_key PK
        VARCHAR customer_id UK
        VARCHAR customer_unique_id
        VARCHAR city
        CHAR state
        INT geo_key FK
    }

    dim_seller {
        INT seller_key PK
        VARCHAR seller_id UK
        VARCHAR city
        CHAR state
        INT geo_key FK
        VARCHAR seller_region
    }

    dim_product_category {
        INT category_key PK
        VARCHAR category_name_english
        VARCHAR category_name_portuguese
    }

    dim_product {
        INT product_key PK
        VARCHAR product_id UK
        INT category_key FK
        INT product_name_length
        INT product_description_length
        INT product_photos_qty
        INT product_weight_g
        INT product_length_cm
        INT product_height_cm
        INT product_width_cm
    }

    dim_order_status {
        VARCHAR order_status PK
        VARCHAR description
    }

    dim_payment_method {
        VARCHAR payment_type PK
        VARCHAR description
    }

    %% Fact tables
    fact_orders {
        BIGINT fact_order_item_id PK
        VARCHAR order_id
        INT order_item_id
        INT customer_key FK
        INT seller_key FK
        INT product_key FK
        INT order_date_key FK
        INT approved_date_key FK
        INT delivered_date_key FK
        INT estimated_delivery_date_key FK
        DECIMAL price
        DECIMAL freight_value
        INT quantity
        TINYINT review_score
        VARCHAR order_status
    }

    fact_sales {
        BIGINT fact_sales_id PK
        INT seller_key FK
        INT category_key FK
        INT date_key FK
        DECIMAL total_revenue
        INT total_items_sold
        INT total_orders
        DECIMAL avg_review_score
    }

    fact_delivery {
        BIGINT fact_delivery_id PK
        INT seller_key FK
        INT date_key FK
        INT total_orders_delivered
        INT on_time_orders
        DECIMAL on_time_rate
    }

    fact_payment_trends {
        BIGINT fact_payment_id PK
        VARCHAR payment_type FK
        INT date_key FK
        DECIMAL total_payment_value
        INT transaction_count
        INT order_count
    }

    fact_customer_orders {
        BIGINT fact_customer_id PK
        INT customer_key FK
        VARCHAR order_status FK
        INT date_key FK
        INT total_orders
        INT total_items
        DECIMAL total_spent
        DECIMAL avg_review_score
    }

    fact_sales_year {
        BIGINT fact_sales_year_id PK
        INT seller_key FK
        INT category_key FK
        INT year
        DECIMAL total_revenue
        INT total_items_sold
        INT total_orders
        DECIMAL avg_review_score
    }

    fact_delivery_year {
        BIGINT fact_delivery_year_id PK
        INT seller_key FK
        INT year
        INT total_orders_delivered
        INT on_time_orders
        DECIMAL on_time_rate
    }

    fact_payment_trends_year {
        BIGINT fact_payment_year_id PK
        VARCHAR payment_type FK
        INT year
        DECIMAL total_payment_value
        INT transaction_count
        INT order_count
    }

    fact_customer_orders_year {
        BIGINT fact_customer_year_id PK
        INT customer_key FK
        VARCHAR order_status FK
        INT year
        INT total_orders
        INT total_items
        DECIMAL total_spent
        DECIMAL avg_review_score
    }

    fact_order_lifecycle {
        BIGINT fact_lifecycle_id PK
        VARCHAR order_id
        INT customer_key FK
        INT seller_key FK
        DATE order_date
        DATE approved_date
        DATE delivered_date
        DATE estimated_delivery_date
        INT days_to_approve
        INT days_to_delivery
        BOOLEAN is_delayed
        VARCHAR order_status
    }

    %% Relationships (solid lines, no labels)
    dim_customer ||--|| dim_geolocation : geo_key
    dim_seller ||--|| dim_geolocation : geo_key
    dim_product ||--|| dim_product_category : category_key

    fact_orders ||--|| dim_customer : customer_key
    fact_orders ||--|| dim_seller : seller_key
    fact_orders ||--|| dim_product : product_key
    fact_orders ||--|| dim_date : order_date_key
    fact_orders ||--|| dim_date : approved_date_key
    fact_orders ||--|| dim_date : delivered_date_key
    fact_orders ||--|| dim_date : estimated_delivery_date_key

    fact_sales ||--|| dim_seller : seller_key
    fact_sales ||--|| dim_product_category : category_key
    fact_sales ||--|| dim_date : date_key

    fact_delivery ||--|| dim_seller : seller_key
    fact_delivery ||--|| dim_date : date_key

    fact_payment_trends ||--|| dim_payment_method : payment_type
    fact_payment_trends ||--|| dim_date : date_key

    fact_customer_orders ||--|| dim_customer : customer_key
    fact_customer_orders ||--|| dim_order_status : order_status
    fact_customer_orders ||--|| dim_date : date_key

    fact_sales_year ||--|| dim_seller : seller_key
    fact_sales_year ||--|| dim_product_category : category_key

    fact_delivery_year ||--|| dim_seller : seller_key

    fact_payment_trends_year ||--|| dim_payment_method : payment_type

    fact_customer_orders_year ||--|| dim_customer : customer_key
    fact_customer_orders_year ||--|| dim_order_status : order_status

    fact_order_lifecycle ||--|| dim_customer : customer_key
    fact_order_lifecycle ||--|| dim_seller : seller_key

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

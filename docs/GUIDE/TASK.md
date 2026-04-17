# 📋 TASK.md – Phân công nhiệm vụ SSIS ETL Pipeline

**Dự án:** Data Warehouse – Brazilian E-Commerce (Olist)
**Số thành viên:** 3
**Công cụ:** SQL Server + SSIS (SQL Server Integration Services)

---

## Nguyên tắc phân công

- Mỗi thành viên đều tham gia đầy đủ 3 giai đoạn: **Extract → Transform → Load**
- Phân chia theo **domain dữ liệu** để có thể làm đồng thời, không phụ thuộc lẫn nhau
- Mỗi người chịu trách nhiệm cả Dimension lẫn Fact trong domain của mình
- Sau khi hoàn thành phần riêng, merge tất cả packages vào **Master Package** chung

---

## Tổng quan phân công

| Thành viên | Domain | Staging Tables | Dimensions | Fact Tables |
|---|---|---|---|---|
| **TV1** | Khách hàng & Địa lý & Thời gian | stg_customers, stg_geolocation, stg_order_reviews | dim_date, dim_geolocation, dim_customer, dim_order_status | fact_customer_orders (month + year) |
| **TV2** | Sản phẩm & Doanh thu | stg_products, stg_category_translation, stg_order_items | dim_product_category, dim_product, dim_payment_method | fact_orders, fact_sales (month + year) |
| **TV3** | Người bán & Vận chuyển & Thanh toán | stg_sellers, stg_orders, stg_order_payments | dim_seller | fact_delivery (month + year), fact_payment_trends (month + year), fact_order_lifecycle |

---

## Chi tiết nhiệm vụ

---

### 👤 Thành viên 1 – Domain: Khách hàng & Địa lý & Thời gian

#### Phase 1: Extract (Staging)

| # | Nhiệm vụ | Chi tiết SSIS | Output |
|---|---|---|---|
| 1.1 | Tạo staging tables | Viết DDL cho `staging.stg_customers`, `staging.stg_geolocation`, `staging.stg_order_reviews` | 3 bảng staging trong SQL Server |
| 1.2 | SSIS Package: `Extract_Customer_Geo.dtsx` | **Control Flow:** Execute SQL Task (truncate 3 bảng) → 3 Data Flow Tasks song song | Package hoàn chỉnh |
| 1.3 | Data Flow – stg_customers | Flat File Source (`olist_customers_dataset.csv`) → Data Conversion (VARCHAR types) → OLE DB Destination (`stg_customers`) | Load thành công |
| 1.4 | Data Flow – stg_geolocation | Flat File Source (`olist_geolocation_dataset.csv`) → Data Conversion → Aggregate (dedup theo `zip_code_prefix`, lấy AVG lat/lng) → OLE DB Destination | Load + dedup |
| 1.5 | Data Flow – stg_order_reviews | Flat File Source (`olist_order_reviews_dataset.csv`) → Data Conversion → OLE DB Destination | Load thành công |

#### Phase 2: Transform & Load Dimensions

| # | Nhiệm vụ | Chi tiết SSIS | Kỹ thuật đặc biệt |
|---|---|---|---|
| 2.1 | dim_date | Execute SQL Task: chạy script SQL sinh calendar table (2016–2019) với year, quarter, month, day_of_week, is_weekend, is_holiday_brazil, season_brazil | **Pre-populate**, không cần SCD |
| 2.2 | dim_order_status | Execute SQL Task: INSERT các giá trị cố định (delivered, shipped, canceled, ...) với description | **Pre-populate**, không cần SCD |
| 2.3 | dim_geolocation | **Data Flow Task:** OLE DB Source (stg_geolocation) → Derived Column (thêm `region` dựa trên state) → Lookup (kiểm tra geo_key đã tồn tại) → Conditional Split (New / Changed) → OLE DB Destination (insert mới) + OLE DB Command (update thay đổi) | **SCD Type 1** – overwrite lat/lng/city |
| 2.4 | dim_customer | **Data Flow Task:** OLE DB Source (stg_customers) → Lookup dim_geolocation (lấy geo_key) → Lookup dim_customer (kiểm tra customer_unique_id) → Conditional Split (New / Changed / Unchanged) → Insert mới / Expire + Insert mới (cho thay đổi city/state) | **SCD Type 2** – thêm effective_from, effective_to, is_current |

#### Phase 3: Load Fact Table

| # | Nhiệm vụ | Chi tiết SSIS | Kỹ thuật đặc biệt |
|---|---|---|---|
| 3.1 | fact_customer_orders (month) | **Data Flow Task:** OLE DB Source (query join fact_orders với dim) → Aggregate (GROUP BY customer_key, order_status, month) → tính total_orders, total_items, total_spent, avg_review_score → OLE DB Destination | **Rebuild monthly** – truncate tháng hiện tại trước khi load |
| 3.2 | fact_customer_orders_year | Tương tự 3.1 nhưng grain theo year | Rebuild yearly |

#### Deliverables

- [ ] DDL scripts: 3 staging tables + dim_date + dim_geolocation + dim_customer + dim_order_status + fact_customer_orders + fact_customer_orders_year
- [ ] SSIS Package: `Extract_Customer_Geo.dtsx`
- [ ] SSIS Package: `Load_Dim_Date_Geo_Customer.dtsx`
- [ ] SSIS Package: `Load_Fact_Customer_Orders.dtsx`
- [ ] Script SQL sinh dim_date (calendar table)
- [ ] Tài liệu: mapping SCD Type 2 cho dim_customer

---

### 👤 Thành viên 2 – Domain: Sản phẩm & Doanh thu

#### Phase 1: Extract (Staging)

| # | Nhiệm vụ | Chi tiết SSIS | Output |
|---|---|---|---|
| 1.1 | Tạo staging tables | Viết DDL cho `staging.stg_products`, `staging.stg_category_translation`, `staging.stg_order_items` | 3 bảng staging |
| 1.2 | SSIS Package: `Extract_Product_Items.dtsx` | **Control Flow:** Execute SQL Task (truncate) → 3 Data Flow Tasks song song | Package hoàn chỉnh |
| 1.3 | Data Flow – stg_products | Flat File Source → Data Conversion (weight → DECIMAL, dimensions → DECIMAL) → Derived Column (xử lý NULL: ISNULL weight thì gán 0) → OLE DB Destination | Load + clean NULL |
| 1.4 | Data Flow – stg_category_translation | Flat File Source → Data Conversion → OLE DB Destination | Load thành công |
| 1.5 | Data Flow – stg_order_items | Flat File Source → Data Conversion (price, freight_value → DECIMAL) → OLE DB Destination | Load thành công |

#### Phase 2: Transform & Load Dimensions

| # | Nhiệm vụ | Chi tiết SSIS | Kỹ thuật đặc biệt |
|---|---|---|---|
| 2.1 | dim_payment_method | Execute SQL Task: INSERT các giá trị cố định (credit_card, boleto, voucher, debit_card, not_defined) | **Pre-populate** |
| 2.2 | dim_product_category | **Data Flow Task:** OLE DB Source (stg_category_translation) → Lookup dim_product_category (kiểm tra tồn tại) → Conditional Split (New / Changed) → Insert hoặc Update | **SCD Type 1** – overwrite tên category |
| 2.3 | dim_product | **Data Flow Task:** OLE DB Source (stg_products) → Lookup dim_product_category (lấy category_key qua JOIN stg_category_translation) → Lookup dim_product (kiểm tra product_id) → Conditional Split → Insert mới / Update thay đổi | **SCD Type 1** – overwrite weight/dimensions |

#### Phase 3: Load Fact Tables

| # | Nhiệm vụ | Chi tiết SSIS | Kỹ thuật đặc biệt |
|---|---|---|---|
| 3.1 | fact_orders (transaction fact) | **Data Flow Task:** OLE DB Source (JOIN stg_order_items + stg_orders + stg_order_reviews) → Lookup dim_customer (lấy customer_key) → Lookup dim_seller (lấy seller_key) → Lookup dim_product (lấy product_key) → Lookup dim_date ×4 (order/approved/delivered/estimated) → OLE DB Destination | **Incremental Load** – LEFT JOIN kiểm tra order_id + order_item_id chưa tồn tại |
| 3.2 | fact_sales (month) | **Data Flow Task:** OLE DB Source (query aggregate từ fact_orders GROUP BY seller_key, category_key, month) → OLE DB Destination | **Rebuild monthly** |
| 3.3 | fact_sales_year | Tương tự 3.2 nhưng GROUP BY year | Rebuild yearly |

#### Deliverables

- [ ] DDL scripts: 3 staging tables + dim_product_category + dim_product + dim_payment_method + fact_orders + fact_sales + fact_sales_year
- [ ] SSIS Package: `Extract_Product_Items.dtsx`
- [ ] SSIS Package: `Load_Dim_Product.dtsx`
- [ ] SSIS Package: `Load_Fact_Orders.dtsx` (bao gồm Incremental Load logic)
- [ ] SSIS Package: `Load_Fact_Sales.dtsx`
- [ ] Tài liệu: mapping Incremental Load cho fact_orders

---

### 👤 Thành viên 3 – Domain: Người bán & Vận chuyển & Thanh toán

#### Phase 1: Extract (Staging)

| # | Nhiệm vụ | Chi tiết SSIS | Output |
|---|---|---|---|
| 1.1 | Tạo staging tables | Viết DDL cho `staging.stg_sellers`, `staging.stg_orders`, `staging.stg_order_payments` | 3 bảng staging |
| 1.2 | SSIS Package: `Extract_Seller_Order_Payment.dtsx` | **Control Flow:** Execute SQL Task (truncate) → 3 Data Flow Tasks song song | Package hoàn chỉnh |
| 1.3 | Data Flow – stg_sellers | Flat File Source → Data Conversion → OLE DB Destination | Load thành công |
| 1.4 | Data Flow – stg_orders | Flat File Source → Data Conversion (5 timestamp columns → DATETIME) → Derived Column (xử lý NULL timestamps) → OLE DB Destination | Load + type cast |
| 1.5 | Data Flow – stg_order_payments | Flat File Source → Data Conversion (payment_value → DECIMAL) → OLE DB Destination | Load thành công |

#### Phase 2: Transform & Load Dimensions

| # | Nhiệm vụ | Chi tiết SSIS | Kỹ thuật đặc biệt |
|---|---|---|---|
| 2.1 | dim_seller | **Data Flow Task:** OLE DB Source (stg_sellers) → Lookup dim_geolocation (lấy geo_key từ zip_code_prefix) → Derived Column (thêm seller_region dựa trên state) → Lookup dim_seller (kiểm tra seller_id) → Conditional Split (New / Changed / Unchanged) → Insert / Expire + Insert | **SCD Type 2** – lưu lịch sử thay đổi city/state |

#### Phase 3: Load Fact Tables

| # | Nhiệm vụ | Chi tiết SSIS | Kỹ thuật đặc biệt |
|---|---|---|---|
| 3.1 | fact_order_lifecycle | **Data Flow Task:** OLE DB Source (stg_orders) → Lookup dim_customer → Lookup dim_seller → Derived Column (tính days_to_approve, days_to_delivery, is_delayed) → Lookup fact_order_lifecycle (kiểm tra order_id) → Conditional Split (New / Updated) → Insert / Update | **Upsert** – Accumulating Snapshot, update timestamps khi order tiến triển |
| 3.2 | fact_delivery (month) | **Data Flow Task:** OLE DB Source (query từ fact_orders + fact_order_lifecycle: tính on_time_orders, on_time_rate GROUP BY seller_key, month) → OLE DB Destination | **Rebuild monthly** |
| 3.3 | fact_delivery_year | Tương tự 3.2 nhưng GROUP BY year | Rebuild yearly |
| 3.4 | fact_payment_trends (month) | **Data Flow Task:** OLE DB Source (stg_order_payments JOIN stg_orders → GROUP BY payment_type, month) → tính total_payment_value, transaction_count, order_count → OLE DB Destination | **Rebuild monthly** |
| 3.5 | fact_payment_trends_year | Tương tự 3.4 nhưng GROUP BY year | Rebuild yearly |

#### Deliverables

- [ ] DDL scripts: 3 staging tables + dim_seller + fact_order_lifecycle + fact_delivery + fact_delivery_year + fact_payment_trends + fact_payment_trends_year
- [ ] SSIS Package: `Extract_Seller_Order_Payment.dtsx`
- [ ] SSIS Package: `Load_Dim_Seller.dtsx`
- [ ] SSIS Package: `Load_Fact_Lifecycle.dtsx` (bao gồm Upsert logic)
- [ ] SSIS Package: `Load_Fact_Delivery.dtsx`
- [ ] SSIS Package: `Load_Fact_Payment_Trends.dtsx`
- [ ] Tài liệu: mapping Upsert cho fact_order_lifecycle, SCD Type 2 cho dim_seller

---

## Công việc chung (cả 3 cùng làm)

| # | Nhiệm vụ | Người chịu trách nhiệm chính | Hỗ trợ |
|---|---|---|---|
| C1 | Tạo database + schemas (staging, bronze, silver, gold) | TV1 | TV2, TV3 review |
| C2 | Master Package (`Master_ETL.dtsx`) – Control Flow gọi tất cả child packages theo thứ tự | TV3 | TV1, TV2 cung cấp packages |
| C3 | Viết SQL truy vấn phân tích (5–10 câu insight) | Mỗi người 2–3 câu theo domain | Cùng review |
| C4 | Tài liệu kiến trúc tổng thể (README.md) | TV2 | TV1, TV3 bổ sung |
| C5 | Test tích hợp end-to-end | Cả 3 | Chạy Master Package, verify data |

---

## Thứ tự thực thi trong Master Package

```
Master_ETL.dtsx (Control Flow)
│
├── Step 1: [Song song] Extract to Staging
│   ├── Extract_Customer_Geo.dtsx          (TV1)
│   ├── Extract_Product_Items.dtsx         (TV2)
│   └── Extract_Seller_Order_Payment.dtsx  (TV3)
│
├── Step 2: [Song song] Load Dimensions
│   ├── Load_Dim_Date_Geo_Customer.dtsx    (TV1)  ← dim_date, dim_geolocation, dim_customer, dim_order_status
│   ├── Load_Dim_Product.dtsx              (TV2)  ← dim_product_category, dim_product, dim_payment_method
│   └── Load_Dim_Seller.dtsx               (TV3)  ← dim_seller (phụ thuộc dim_geolocation từ TV1)
│
├── Step 3: Load Transaction Facts
│   ├── Load_Fact_Orders.dtsx              (TV2)  ← cần tất cả dim đã load xong
│   └── Load_Fact_Lifecycle.dtsx           (TV3)  ← cần dim_customer, dim_seller
│
├── Step 4: [Song song] Load Aggregated Facts
│   ├── Load_Fact_Customer_Orders.dtsx     (TV1)  ← từ fact_orders
│   ├── Load_Fact_Sales.dtsx               (TV2)  ← từ fact_orders
│   ├── Load_Fact_Delivery.dtsx            (TV3)  ← từ fact_orders + lifecycle
│   └── Load_Fact_Payment_Trends.dtsx      (TV3)  ← từ staging payments
│
└── Step 5: Yearly Aggregations (tất cả song song)
```

**Lưu ý dependency:**
- Step 2: `Load_Dim_Seller.dtsx` (TV3) phải chờ `dim_geolocation` từ TV1 hoàn thành → dùng **Precedence Constraint** trong Master Package
- Step 3: Chỉ chạy sau khi tất cả Dimensions ở Step 2 xong
- Step 4: Chỉ chạy sau khi `fact_orders` và `fact_order_lifecycle` ở Step 3 xong

---

## Tổng kết khối lượng

| Hạng mục | TV1 | TV2 | TV3 |
|---|---|---|---|
| Staging tables | 3 | 3 | 3 |
| Dimension tables | 4 (dim_date, dim_geolocation, dim_customer, dim_order_status) | 3 (dim_product_category, dim_product, dim_payment_method) | 1 (dim_seller) |
| Fact tables | 2 (fact_customer_orders month+year) | 3 (fact_orders, fact_sales month+year) | 5 (fact_lifecycle, fact_delivery m+y, fact_payment_trends m+y) |
| SSIS Packages | 3 | 4 | 4 |
| SCD xử lý | Type 2 (dim_customer), Type 1 (dim_geolocation) | Type 1 (dim_product, dim_product_category) | Type 2 (dim_seller) |
| Incremental Load | Rebuild snapshot | Incremental insert (fact_orders) | Upsert (fact_lifecycle), Rebuild snapshot |
| SQL truy vấn | 2–3 câu (customer behavior) | 2–3 câu (revenue, product) | 2–3 câu (delivery, payment) |

-- TV1
-- Tạo database chính cho Data Warehouse
CREATE DATABASE OlistDW;
GO

-- Chuyển sang database vừa tạo
USE OlistDW;
GO

-- Schema cho vùng đệm staging (truncate-reload)
CREATE SCHEMA staging;
GO

-- Schema cho Gold layer (star schema)
CREATE SCHEMA gold;
GO

SELECT name FROM sys.schemas WHERE name IN ('staging', 'gold');

-- BƯỚC 1: Tạo Staging Tables
-- 1.1. staging.stg_customers
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

-- 1.2. staging.stg_geolocation
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

-- 1.3. staging.stg_order_reviews
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

-- BƯỚC 2: Tạo Dimension Tables
-- 2.1. gold.dim_date
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

-- 2.2. gold.dim_geolocation
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

-- 2.3. gold.dim_customer (SCD Type 2)
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

-- 2.4. gold.dim_order_status
IF OBJECT_ID('gold.dim_order_status', 'U') IS NOT NULL
    DROP TABLE gold.dim_order_status;
GO

CREATE TABLE gold.dim_order_status (
    order_status  VARCHAR(30)   NOT NULL PRIMARY KEY,
    description   NVARCHAR(100) NULL
);
GO

-- BƯỚC 3: Tạo Fact Tables
-- 3.1. gold.fact_customer_orders (monthly)
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

-- 3.2. gold.fact_customer_orders_year
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

-- Check
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('staging', 'gold')
ORDER BY TABLE_SCHEMA, TABLE_NAME;


SELECT 'dim_date'         AS tbl, COUNT(*) AS cnt FROM gold.dim_date
UNION ALL SELECT 'dim_order_status',  COUNT(*) FROM gold.dim_order_status
UNION ALL SELECT 'dim_geolocation',   COUNT(*) FROM gold.dim_geolocation
UNION ALL SELECT 'dim_customer',      COUNT(*) FROM gold.dim_customer;

-- Verify SCD Type 2: lần đầu tất cả is_current = 1
SELECT is_current, COUNT(*) FROM gold.dim_customer GROUP BY is_current;
-- Expected: is_current=1 → ~99,441



-- TV3
-- staging.stg_sellers
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

-- staging.stg_orders
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

-- staging.stg_order_payments
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

--  gold.dim_seller (SCD Type 2)
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

-- gold.fact_order_lifecycle (Accumulating Snapshot)
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

-- gold.fact_order_lifecycle (Accumulating Snapshot)
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

-- gold.fact_delivery_year
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
-- gold.fact_payment_trends_year
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
SELECT COUNT(*) AS total_rows FROM gold.dim_seller;
SELECT is_current, COUNT(*) AS cnt
FROM gold.dim_seller
GROUP BY is_current;

SELECT seller_region, COUNT(*) AS cnt
FROM gold.dim_seller
GROUP BY seller_region
ORDER BY cnt DESC;

SELECT COUNT(*) FROM gold.fact_order_lifecycle;

SELECT order_status, COUNT(*) AS cnt
FROM gold.fact_order_lifecycle
GROUP BY order_status
ORDER BY cnt DESC;

SELECT
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed,
    SUM(CASE WHEN is_delayed = 0 THEN 1 ELSE 0 END) AS on_time,
    SUM(CASE WHEN is_delayed IS NULL THEN 1 ELSE 0 END) AS unknown
FROM gold.fact_order_lifecycle;
--TV2
--  staging.stg_products
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

-- staging.stg_category_translation
IF OBJECT_ID('staging.stg_category_translation', 'U') IS NOT NULL
    DROP TABLE staging.stg_category_translation;
GO

CREATE TABLE staging.stg_category_translation (
    product_category_name         NVARCHAR(100) NOT NULL,
    product_category_name_english NVARCHAR(100) NOT NULL
);
GO
--  staging.stg_order_items
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

-- gold.dim_product_category
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

--  gold.dim_product
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
-- gold.dim_payment_method
IF OBJECT_ID('gold.dim_payment_method', 'U') IS NOT NULL
    DROP TABLE gold.dim_payment_method;
GO

CREATE TABLE gold.dim_payment_method (
    payment_type  VARCHAR(30)   NOT NULL PRIMARY KEY,
    description   NVARCHAR(100) NULL
);
GO

-- gold.fact_orders
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

-- gold.fact_sales (monthly)
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

-- gold.fact_sales_year
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
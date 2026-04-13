CREATE SCHEMA staging;
GO
CREATE SCHEMA gold;
GO

-- B\u01af\u1edaC 1: T\u1ea1o Staging Tables (DDL)
-- 1.1. staging.stg_customers
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

-- B\u01af\u1edaC 2: T\u1ea1o Dimension Tables (DDL)
-- 2.1. gold.dim_date
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

-- 2.4. gold.dim_order_status
IF OBJECT_ID('gold.dim_order_status', 'U') IS NOT NULL
    DROP TABLE gold.dim_order_status;
GO

CREATE TABLE gold.dim_order_status (
    order_status  VARCHAR(30)  NOT NULL PRIMARY KEY,
    description   NVARCHAR(100) NULL
);
GO

-- B\u01af\u1edaC 3: T\u1ea1o Fact Tables (DDL)
-- 3.1. gold.fact_customer_orders
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

-- 3.2. gold.fact_customer_orders_year
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


-- 4.5. Test Package
SELECT COUNT(*) AS cnt FROM staging.stg_customers;       -- Expected: ~99,441
SELECT COUNT(*) AS cnt FROM staging.stg_geolocation;     -- Expected: ~19,015 (sau dedup)
SELECT COUNT(*) AS cnt FROM staging.stg_order_reviews;   -- Expected: ~99,224
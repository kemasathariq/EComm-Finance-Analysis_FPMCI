CREATE DATABASE IF NOT EXISTS dustinia;

-- raw data

CREATE TABLE IF NOT EXISTS dustinia.raw_orders (
    order_id                      String,
    customer_id                   String,
    order_status                  String,
    order_purchase_timestamp      DateTime,
    order_approved_at             Nullable(DateTime),
    order_delivered_carrier_date  Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS dustinia.raw_order_items (
    order_id            String,
    order_item_id       UInt8,
    product_id          String,
    seller_id           String,
    shipping_limit_date DateTime,
    price               Float64,
    freight_value       Float64
) ENGINE = MergeTree()
ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS dustinia.raw_order_payments (
    order_id             String,
    payment_sequential   UInt8,
    payment_type         String,
    payment_installments UInt8,
    payment_value        Float64
) ENGINE = MergeTree()
ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS dustinia.raw_customers (
    customer_id              String,
    customer_unique_id       String,
    customer_zip_code_prefix String,
    customer_city            String,
    customer_state           String
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dustinia.raw_products (
    product_id                 String,
    product_category_name      Nullable(String),
    product_name_length        Nullable(UInt16),
    product_description_length Nullable(UInt32),
    product_photos_qty         Nullable(UInt8),
    product_weight_g           Nullable(Float64),
    product_length_cm          Nullable(Float64),
    product_height_cm          Nullable(Float64),
    product_width_cm           Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS dustinia.raw_product_category_translation (
    product_category_name         String,
    product_category_name_english String
) ENGINE = MergeTree()
ORDER BY product_category_name;

CREATE TABLE IF NOT EXISTS dustinia.raw_sellers (
    seller_id              String,
    seller_zip_code_prefix String,
    seller_city            String,
    seller_state           String
) ENGINE = MergeTree()
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS dustinia.raw_geolocation (
    geolocation_zip_code_prefix String,
    geolocation_lat             Float64,
    geolocation_lng             Float64,
    geolocation_city            String,
    geolocation_state           String
) ENGINE = MergeTree()
ORDER BY geolocation_zip_code_prefix;

CREATE TABLE IF NOT EXISTS dustinia.raw_order_reviews (
    review_id               String,
    order_id                String,
    review_score            UInt8,
    review_comment_title    Nullable(String),
    review_comment_message  Nullable(String),
    review_creation_date    DateTime,
    review_answer_timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (order_id, review_id);

CREATE TABLE IF NOT EXISTS dustinia.raw_mql (
    mql_id             String,
    first_contact_date Date,
    landing_page_id    String,
    origin             String
) ENGINE = MergeTree()
ORDER BY mql_id;

CREATE TABLE IF NOT EXISTS dustinia.raw_closed_deals (
    mql_id                        String,
    seller_id                     String,
    sdr_id                        String,
    sr_id                         String,
    won_date                      Date,
    business_segment              Nullable(String),
    lead_type                     Nullable(String),
    lead_behaviour_profile        Nullable(String),
    has_company                   Nullable(String),
    has_gtin                      Nullable(String),
    average_stock                 Nullable(String),
    business_type                 Nullable(String),
    declared_product_catalog_size Nullable(Float64),
    declared_monthly_revenue      Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY mql_id;


-- mart layer
CREATE TABLE IF NOT EXISTS dustinia.mart_customer_payment_profile (
    customer_unique_id       String,
    customer_state           String,
    customer_city            String,
    total_orders             UInt32,
    total_spend              Float64,
    avg_order_value          Float64,
    is_high_value            UInt8,
    preferred_payment_type   String,
    avg_installments         Float64,
    credit_card_orders       UInt32,
    boleto_orders            UInt32,
    voucher_orders           UInt32,
    debit_card_orders        UInt32,
    max_installments_used    UInt8,
    uses_installments        UInt8,
    avg_review_score         Float64,
    installment_bucket       String,
    updated_at               DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_unique_id;

-- One row per order, clean join of orders, payments, items
CREATE TABLE IF NOT EXISTS dustinia.mart_order_payment_detail (
    order_id                 String,
    customer_unique_id       String,
    customer_state           String,
    order_status             String,
    order_purchase_timestamp DateTime,
    total_order_value        Float64,
    total_payment_value      Float64,
    payment_type             String,
    payment_installments     UInt8,
    num_items                UInt8,
    product_categories       String,
    updated_at               DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;

-- One row per state, payment mix and revenue summary
-- high_value_revenue_share = % of state revenue from high-value customers
CREATE TABLE IF NOT EXISTS dustinia.mart_geo_payment_summary (
    customer_state           String,
    total_customers          UInt32,
    high_value_customers     UInt32,
    total_revenue            Float64,
    avg_order_value          Float64,
    credit_card_pct          Float64,
    boleto_pct               Float64,
    installment_avg          Float64,
    high_value_revenue_share Float64,
    updated_at               DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_state;

-- Revenue leakage: state × payment_type combos below national avg
-- leakage_score = (national_avg - local_avg) / national_avg * order_count
-- logicnya adalah if the rev is lower than the nat average that means the performance could be improved by switching to high performing payment types
CREATE TABLE IF NOT EXISTS dustinia.mart_payment_leakage (
    analysis_date  Date,
    state          String,
    payment_type   String,
    order_count    UInt32,
    avg_value      Float64,
    leakage_score  Float64,
    updated_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (analysis_date, state, payment_type);

-- Monthly revenue trend broken down by payment type
CREATE TABLE IF NOT EXISTS dustinia.mart_monthly_revenue (
    year_month      String,
    payment_type    String,
    order_count     UInt32,
    total_revenue   Float64,
    avg_order_value Float64,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (year_month, payment_type);

-- Category preference for HVC only
-- yang sebenarnya dibeli HVC itu apa?
CREATE TABLE IF NOT EXISTS dustinia.mart_hv_category_preference (
    customer_unique_id  String,
    product_category_en String,
    order_count         UInt32,
    total_spend         Float64,
    updated_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (customer_unique_id, product_category_en);

-- Seller acquisition funnel: MQL → closed deal
-- business_segment, lead_type, lead_behaviour_profile from closed_deals
CREATE TABLE IF NOT EXISTS dustinia.mart_seller_acquisition (
    mql_id                   String,
    seller_id                String,
    business_segment         String,
    lead_type                String,
    lead_behaviour_profile   String,
    business_type            String,
    first_contact_date       Date,
    won_date                 Date,
    days_to_close            Int32,
    declared_monthly_revenue Float64,
    updated_at               DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY mql_id;

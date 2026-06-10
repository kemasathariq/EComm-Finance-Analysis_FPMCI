from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import logging

default_args = {
    'owner': 'thoriq',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dustinia_finance_analyst_pipeline',
    default_args=default_args,
    description='Finance Analyst pipeline: customer payment profiles, '
                'geo analysis, leakage, monthly trends, seller acquisition',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'dustinia', 'final-project'],
)


# Task 0: Schema Setup
def setup_schema(**context):
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    # Evolve existing mart tables - safe to run repeatedly
    migrations = [
        "ALTER TABLE dustinia.mart_customer_payment_profile "
        "ADD COLUMN IF NOT EXISTS avg_review_score Float64 DEFAULT 0",

        "ALTER TABLE dustinia.mart_customer_payment_profile "
        "ADD COLUMN IF NOT EXISTS installment_bucket String DEFAULT ''",

        "ALTER TABLE dustinia.mart_geo_payment_summary "
        "ADD COLUMN IF NOT EXISTS high_value_revenue_share Float64 DEFAULT 0",
    ]
    for sql in migrations:
        client.command(sql)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_monthly_revenue (
            year_month      String,
            payment_type    String,
            order_count     UInt32,
            total_revenue   Float64,
            avg_order_value Float64,
            updated_at      DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (year_month, payment_type)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_hv_category_preference (
            customer_unique_id  String,
            product_category_en String,
            order_count         UInt32,
            total_spend         Float64,
            updated_at          DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (customer_unique_id, product_category_en)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_mql_funnel (
            origin              String,
            total_leads         UInt32,
            converted_leads     UInt32,
            conversion_rate     Float64,
            avg_days_to_close   Float64,
            top_business_segment String,
            updated_at          DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY origin
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_mql_monthly (
            year_month          String,
            origin              String,
            total_leads         UInt32,
            converted_leads     UInt32,
            conversion_rate     Float64,
            updated_at          DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (year_month, origin)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_lead_behaviour (
            lead_behaviour_profile String,
            total_deals            UInt32,
            avg_days_to_close      Float64,
            avg_declared_revenue   Float64,
            top_business_segment   String,
            updated_at             DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY lead_behaviour_profile
    """)

    client.command("""
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
        ORDER BY mql_id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_seller_performance (
            seller_id         String,
            seller_state      String,
            seller_city       String,
            total_orders      UInt32,
            total_revenue     Float64,
            avg_order_value   Float64,
            avg_review_score  Float64,
            unique_categories UInt32,
            total_items_sold  UInt32,
            updated_at        DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY seller_id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_repeat_customer (
            customer_unique_id String,
            customer_state     String,
            total_orders       UInt32,
            is_repeat_customer UInt8,
            first_order_date   Date,
            last_order_date    Date,
            days_active        Int32,
            is_high_value      UInt8,
            updated_at         DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY customer_unique_id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_product_price_range (
            year_month          String,
            product_category_en String,
            price_bucket        String,
            order_count         UInt32,
            total_revenue       Float64,
            avg_price           Float64,
            updated_at          DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (year_month, product_category_en, price_bucket)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS dustinia.mart_review_by_segment (
            is_high_value      UInt8,
            payment_type       String,
            customer_state     String,
            avg_review_score   Float64,
            total_reviews      UInt32,
            pct_5star          Float64,
            pct_1star          Float64,
            avg_delivery_days  Float64,
            late_delivery_rate Float64,
            updated_at         DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (is_high_value, payment_type, customer_state)
    """)

    client.close()
    logging.info("Schema setup complete.")


# Task 1: Extract
def extract_raw_data(**context):
    required_files = [
        'orders.csv', 'order_items.csv', 'order_payments.csv',
        'customers.csv', 'products.csv', 'category_translation.csv',
        'sellers.csv', 'geolocation.csv', 'order_reviews.csv',
        'mql.csv', 'closed_deals.csv',
    ]

    data_dir = '/opt/airflow/data/raw'
    missing = []

    for f in required_files:
        path = os.path.join(data_dir, f)
        if not os.path.exists(path):
            missing.append(f)
        else:
            size_mb = os.path.getsize(path) / (1024 * 1024)
            logging.info(f"Found: {f} ({size_mb:.2f} MB)")

    if missing:
        raise FileNotFoundError(
            f"Missing dataset files in {data_dir}: {missing}\n"
            "Download from: https://its.id/m/Dataset_FP_MCI and unzip into data/raw/"
        )

    logging.info("All required files present.")


# Task 2: Validate
def validate_data(**context):
    import pandas as pd

    data_dir = '/opt/airflow/data/raw'

    orders = pd.read_csv(f'{data_dir}/orders.csv')
    assert len(orders) > 0, "Orders file is empty"
    assert orders['order_id'].notna().all(), "order_id has nulls"
    assert orders['customer_id'].notna().all(), "customer_id has nulls"
    logging.info(f"Orders: {len(orders):,} rows - OK")

    payments = pd.read_csv(f'{data_dir}/order_payments.csv')
    assert len(payments) > 0, "Payments file is empty"
    assert (payments['payment_value'] >= 0).all(), "Negative payment values found"
    logging.info(f"Payments: {len(payments):,} rows - OK")

    customers = pd.read_csv(f'{data_dir}/customers.csv')
    assert len(customers) > 0, "Customers file is empty"
    logging.info(f"Customers: {len(customers):,} rows - OK")

    context['ti'].xcom_push(key='orders_count', value=len(orders))
    context['ti'].xcom_push(key='payments_count', value=len(payments))


# Task 3: Load Raw to ClickHouse
def load_raw_to_clickhouse(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    data_dir = '/opt/airflow/data/raw'

    # Bug in source data: ZIP code columns are 5-digit strings (e.g. "01310")
    # but pandas infers them as int64, dropping leading zeros and breaking
    # ClickHouse String column inserts.
    zip_dtypes = {
        'customers.csv':   {'customer_zip_code_prefix': str},
        'sellers.csv':     {'seller_zip_code_prefix': str},
        'geolocation.csv': {'geolocation_zip_code_prefix': str},
    }

    # Bug in source data: products.csv ships with two typos -
    # "product_name_lenght" and "product_description_lenght" (missing the h).
    col_renames = {
        'products.csv': {
            'product_name_lenght': 'product_name_length',
            'product_description_lenght': 'product_description_length',
        },
    }

    def load_csv(filename, table, parse_dates=None):
        logging.info(f"Loading {filename} -> dustinia.{table}")
        df = pd.read_csv(
            f'{data_dir}/{filename}',
            parse_dates=parse_dates or [],
            dtype=zip_dtypes.get(filename),
            low_memory=False,
        )
        if filename in col_renames:
            df = df.rename(columns=col_renames[filename])
        # Object columns can contain float NaN (from missing values) or
        # Python booleans (e.g. has_company, has_gtin in closed_deals).
        # Both crash ClickHouse String inserts. fillna first, then astype(str)
        # converts True/False → 'True'/'False' and leaves real strings intact.
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].fillna('').astype(str)
        client.command(f'TRUNCATE TABLE dustinia.{table}')
        client.insert_df(f'dustinia.{table}', df)
        logging.info(f"Loaded {len(df):,} rows into {table}")

    date_cols_orders = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date',
    ]

    load_csv('orders.csv',             'raw_orders',          parse_dates=date_cols_orders)
    load_csv('order_items.csv',        'raw_order_items',     parse_dates=['shipping_limit_date'])
    load_csv('order_payments.csv',     'raw_order_payments')
    load_csv('customers.csv',          'raw_customers')
    load_csv('products.csv',           'raw_products')
    load_csv('category_translation.csv', 'raw_product_category_translation')
    load_csv('sellers.csv',            'raw_sellers')
    load_csv('geolocation.csv',        'raw_geolocation')
    load_csv('order_reviews.csv',      'raw_order_reviews',
             parse_dates=['review_creation_date', 'review_answer_timestamp'])
    load_csv('mql.csv',                'raw_mql',             parse_dates=['first_contact_date'])
    load_csv('closed_deals.csv',       'raw_closed_deals',    parse_dates=['won_date'])

    client.close()
    logging.info("All raw tables loaded successfully.")


# Task 4a: Transform - Customer Payment Profiles
def transform_customer_profiles(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    orders = client.query_df("""
        SELECT order_id, customer_id, order_status
        FROM dustinia.raw_orders
        WHERE order_status IN ('delivered', 'shipped', 'approved')
    """)

    payments = client.query_df("""
        SELECT order_id, payment_type, payment_installments, payment_value
        FROM dustinia.raw_order_payments
    """)

    customers = client.query_df("""
        SELECT customer_id, customer_unique_id, customer_state, customer_city
        FROM dustinia.raw_customers
    """)

    reviews_raw = client.query_df("""
        SELECT o.customer_id, avg(r.review_score) AS avg_review_score
        FROM dustinia.raw_orders o
        JOIN dustinia.raw_order_reviews r ON o.order_id = r.order_id
        GROUP BY o.customer_id
    """)

    pay_agg = payments.groupby('order_id').agg(
        total_payment=('payment_value', 'sum'),
        payment_type=('payment_type', lambda x: x.mode()[0]),
        max_installments=('payment_installments', 'max'),
    ).reset_index()

    pay_types = payments.copy()
    for ptype in ['credit_card', 'boleto', 'voucher', 'debit_card']:
        pay_types[f'is_{ptype}'] = (pay_types['payment_type'] == ptype).astype(int)

    pay_type_agg = pay_types.groupby('order_id').agg(
        credit_card_flag=('is_credit_card', 'max'),
        boleto_flag=('is_boleto', 'max'),
        voucher_flag=('is_voucher', 'max'),
        debit_card_flag=('is_debit_card', 'max'),
    ).reset_index()

    df = orders.merge(customers, on='customer_id', how='left')
    df = df.merge(pay_agg, on='order_id', how='left')
    df = df.merge(pay_type_agg, on='order_id', how='left')

    customer_profile = df.groupby('customer_unique_id').agg(
        customer_state=('customer_state', 'first'),
        customer_city=('customer_city', 'first'),
        total_orders=('order_id', 'count'),
        total_spend=('total_payment', 'sum'),
        avg_order_value=('total_payment', 'mean'),
        preferred_payment_type=('payment_type', lambda x: x.mode()[0] if len(x.dropna()) > 0 else 'unknown'),
        avg_installments=('max_installments', 'mean'),
        max_installments_used=('max_installments', 'max'),
        credit_card_orders=('credit_card_flag', 'sum'),
        boleto_orders=('boleto_flag', 'sum'),
        voucher_orders=('voucher_flag', 'sum'),
        debit_card_orders=('debit_card_flag', 'sum'),
    ).reset_index()

    # Fill nulls in integer columns before inserting - left joins can produce
    # NaN when an order has no matching payment or item record.
    int_cols = [
        'total_orders', 'credit_card_orders', 'boleto_orders',
        'voucher_orders', 'debit_card_orders', 'max_installments_used',
        'is_high_value', 'uses_installments',
    ]
    for col in int_cols:
        if col in customer_profile.columns:
            customer_profile[col] = customer_profile[col].fillna(0).astype(int)
    customer_profile['avg_installments'] = customer_profile['avg_installments'].fillna(0.0)

    # Map reviews from customer_id space to customer_unique_id space
    cid_map = customers[['customer_id', 'customer_unique_id']].drop_duplicates()
    reviews_mapped = reviews_raw.merge(cid_map, on='customer_id', how='left')
    review_by_unique = reviews_mapped.groupby('customer_unique_id')['avg_review_score'].mean().reset_index()
    customer_profile = customer_profile.merge(review_by_unique, on='customer_unique_id', how='left')
    customer_profile['avg_review_score'] = customer_profile['avg_review_score'].fillna(0.0)

    threshold = customer_profile['total_spend'].quantile(0.75)
    customer_profile['is_high_value'] = (customer_profile['total_spend'] >= threshold).astype(int)
    customer_profile['uses_installments'] = (customer_profile['avg_installments'] > 1).astype(int)

    def installment_bucket(x):
        if x <= 1:   return '1'
        elif x <= 6: return '2-6'
        elif x <= 12: return '7-12'
        else:        return '12+'

    customer_profile['installment_bucket'] = customer_profile['max_installments_used'].apply(installment_bucket)

    logging.info(f"High-value threshold (75th pct): R$ {threshold:.2f}")
    logging.info(f"High-value customers: {customer_profile['is_high_value'].sum():,}")

    customer_profile['updated_at'] = pd.Timestamp.now()
    client.command('TRUNCATE TABLE dustinia.mart_customer_payment_profile')
    client.insert_df('dustinia.mart_customer_payment_profile', customer_profile)
    logging.info(f"Wrote {len(customer_profile):,} rows to mart_customer_payment_profile")
    client.close()


# Task 4b: Transform - Order Payment Detail
def transform_order_payments(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    orders = client.query_df("SELECT * FROM dustinia.raw_orders")
    payments = client.query_df("SELECT * FROM dustinia.raw_order_payments")
    customers = client.query_df(
        "SELECT customer_id, customer_unique_id, customer_state FROM dustinia.raw_customers"
    )
    items = client.query_df(
        "SELECT order_id, count() AS num_items, sum(price + freight_value) AS total_order_value "
        "FROM dustinia.raw_order_items GROUP BY order_id"
    )

    pay_agg = payments.groupby('order_id').agg(
        total_payment_value=('payment_value', 'sum'),
        payment_type=('payment_type', lambda x: x.mode()[0]),
        payment_installments=('payment_installments', 'max'),
    ).reset_index()

    df = orders.merge(customers, on='customer_id', how='left')
    df = df.merge(pay_agg, on='order_id', how='left')
    df = df.merge(items, on='order_id', how='left')

    df = df[[
        'order_id', 'customer_unique_id', 'customer_state',
        'order_status', 'order_purchase_timestamp',
        'total_order_value', 'total_payment_value',
        'payment_type', 'payment_installments', 'num_items',
    ]].copy()

    df['num_items'] = df['num_items'].fillna(0).astype(int)
    df['payment_installments'] = df['payment_installments'].fillna(0).astype(int)
    df['total_order_value'] = df['total_order_value'].fillna(0.0)
    df['total_payment_value'] = df['total_payment_value'].fillna(0.0)
    df['payment_type'] = df['payment_type'].fillna('unknown')
    df['customer_state'] = df['customer_state'].fillna('')
    df['customer_unique_id'] = df['customer_unique_id'].fillna('')
    df['product_categories'] = ''
    df['updated_at'] = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_order_payment_detail')
    client.insert_df('dustinia.mart_order_payment_detail', df)
    logging.info(f"Wrote {len(df):,} rows to mart_order_payment_detail")
    client.close()


# Task 4c: Transform - Geographic Payment Summary
def transform_geo_summary(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    geo = client.query_df("""
        SELECT
            customer_state,
            count(*)                                          AS total_customers,
            countIf(is_high_value = 1)                       AS high_value_customers,
            sum(total_spend)                                  AS total_revenue,
            avg(avg_order_value)                              AS avg_order_value,
            avgIf(1, preferred_payment_type = 'credit_card') AS credit_card_pct,
            avgIf(1, preferred_payment_type = 'boleto')      AS boleto_pct,
            avg(avg_installments)                            AS installment_avg,
            sumIf(total_spend, is_high_value = 1)
                / sum(total_spend)                           AS high_value_revenue_share
        FROM dustinia.mart_customer_payment_profile
        GROUP BY customer_state
    """)

    geo['updated_at'] = pd.Timestamp.now()
    client.command('TRUNCATE TABLE dustinia.mart_geo_payment_summary')
    client.insert_df('dustinia.mart_geo_payment_summary', geo)
    logging.info(f"Wrote {len(geo):,} state rows to mart_geo_payment_summary")
    client.close()


# Task 4d: Transform - Monthly Revenue by Payment Type
def transform_monthly_revenue(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    df = client.query_df("""
        SELECT order_purchase_timestamp, payment_type, total_payment_value
        FROM dustinia.mart_order_payment_detail
        WHERE order_status IN ('delivered', 'shipped', 'approved')
          AND payment_type != ''
    """)

    df['year_month'] = pd.to_datetime(df['order_purchase_timestamp']).dt.strftime('%Y-%m')

    monthly = df.groupby(['year_month', 'payment_type']).agg(
        order_count=('total_payment_value', 'count'),
        total_revenue=('total_payment_value', 'sum'),
        avg_order_value=('total_payment_value', 'mean'),
    ).reset_index()

    monthly['order_count'] = monthly['order_count'].astype('uint32')
    monthly['updated_at'] = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_monthly_revenue')
    client.insert_df('dustinia.mart_monthly_revenue', monthly)
    logging.info(f"Wrote {len(monthly):,} rows to mart_monthly_revenue")
    client.close()


# Task 4e: Transform - HV Customer Category Preference
def transform_hv_category_preference(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    hv_orders = client.query_df("""
        SELECT o.order_id, o.customer_unique_id
        FROM dustinia.mart_order_payment_detail o
        JOIN dustinia.mart_customer_payment_profile c
            ON o.customer_unique_id = c.customer_unique_id
        WHERE c.is_high_value = 1
          AND o.order_status IN ('delivered', 'shipped', 'approved')
    """)

    items = client.query_df(
        "SELECT order_id, product_id, price FROM dustinia.raw_order_items"
    )

    products = client.query_df("""
        SELECT p.product_id,
               COALESCE(t.product_category_name_english,
                        p.product_category_name, 'unknown') AS category_en
        FROM dustinia.raw_products p
        LEFT JOIN dustinia.raw_product_category_translation t
            ON p.product_category_name = t.product_category_name
    """)

    df = hv_orders.merge(items, on='order_id', how='left')
    df = df.merge(products, on='product_id', how='left')
    df['category_en'] = df['category_en'].fillna('unknown')

    pref = df.groupby(['customer_unique_id', 'category_en']).agg(
        order_count=('order_id', 'count'),
        total_spend=('price', 'sum'),
    ).reset_index()

    pref = pref.rename(columns={'category_en': 'product_category_en'})
    pref['order_count'] = pref['order_count'].astype('uint32')
    pref['updated_at'] = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_hv_category_preference')
    client.insert_df('dustinia.mart_hv_category_preference', pref)
    logging.info(f"Wrote {len(pref):,} rows to mart_hv_category_preference")
    client.close()


# Task 4f: Transform - Seller Acquisition
def transform_seller_acquisition(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    mql   = client.query_df("SELECT * FROM dustinia.raw_mql")
    deals = client.query_df("SELECT * FROM dustinia.raw_closed_deals")

    df = deals.merge(mql, on='mql_id', how='left')

    df['first_contact_date'] = pd.to_datetime(df['first_contact_date'])
    df['won_date']           = pd.to_datetime(df['won_date'])
    df['days_to_close'] = (
        (df['won_date'] - df['first_contact_date']).dt.days.fillna(0).astype(int)
    )

    for col in ['business_segment', 'lead_type', 'lead_behaviour_profile', 'business_type']:
        df[col] = df[col].fillna('unknown')

    df['declared_monthly_revenue'] = df['declared_monthly_revenue'].fillna(0.0)
    df['seller_id'] = df['seller_id'].fillna('')

    # Convert back to date objects for ClickHouse Date columns
    df['first_contact_date'] = df['first_contact_date'].dt.date
    df['won_date']           = df['won_date'].dt.date

    df = df[[
        'mql_id', 'seller_id', 'business_segment', 'lead_type',
        'lead_behaviour_profile', 'business_type',
        'first_contact_date', 'won_date', 'days_to_close',
        'declared_monthly_revenue',
    ]].copy()

    df['updated_at'] = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_seller_acquisition')
    client.insert_df('dustinia.mart_seller_acquisition', df)
    logging.info(f"Wrote {len(df):,} rows to mart_seller_acquisition")
    client.close()


# Task 4g: Transform - Seller Performance
def transform_seller_performance(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    items = client.query_df(
        "SELECT order_id, seller_id, product_id, price FROM dustinia.raw_order_items"
    )
    delivered = client.query_df(
        "SELECT order_id FROM dustinia.raw_orders "
        "WHERE order_status IN ('delivered','shipped','approved')"
    )
    sellers = client.query_df(
        "SELECT seller_id, seller_state, seller_city FROM dustinia.raw_sellers"
    )
    items = items[items['order_id'].isin(delivered['order_id'])]
    items = items.merge(sellers, on='seller_id', how='left')

    reviews = client.query_df("""
        SELECT order_id, avg(review_score) AS review_score
        FROM dustinia.raw_order_reviews
        GROUP BY order_id
    """)

    products = client.query_df("""
        SELECT p.product_id,
               COALESCE(t.product_category_name_english,
                        p.product_category_name, 'unknown') AS category_en
        FROM dustinia.raw_products p
        LEFT JOIN dustinia.raw_product_category_translation t
            ON p.product_category_name = t.product_category_name
    """)

    items = items.merge(reviews, on='order_id', how='left')
    items = items.merge(products, on='product_id', how='left')
    items['review_score'] = items['review_score'].fillna(0.0)
    items['category_en']  = items['category_en'].fillna('unknown')
    items['seller_state'] = items['seller_state'].fillna('')
    items['seller_city']  = items['seller_city'].fillna('')

    perf = items.groupby(['seller_id', 'seller_state', 'seller_city']).agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('price', 'sum'),
        avg_order_value=('price', 'mean'),
        avg_review_score=('review_score', 'mean'),
        unique_categories=('category_en', 'nunique'),
        total_items_sold=('price', 'count'),
    ).reset_index()

    perf['total_orders']      = perf['total_orders'].astype('uint32')
    perf['unique_categories'] = perf['unique_categories'].astype('uint32')
    perf['total_items_sold']  = perf['total_items_sold'].astype('uint32')
    perf['updated_at']        = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_seller_performance')
    client.insert_df('dustinia.mart_seller_performance', perf)
    logging.info(f"Wrote {len(perf):,} rows to mart_seller_performance")
    client.close()


# Task 4h: Transform - Repeat Customer
def transform_repeat_customer(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    orders = client.query_df("""
        SELECT o.order_id, o.order_purchase_timestamp,
               c.customer_unique_id, c.customer_state
        FROM dustinia.raw_orders o
        JOIN dustinia.raw_customers c ON o.customer_id = c.customer_id
        WHERE o.order_status IN ('delivered', 'shipped', 'approved')
    """)

    hv = client.query_df(
        "SELECT customer_unique_id, is_high_value FROM dustinia.mart_customer_payment_profile"
    )

    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])

    repeat = orders.groupby(['customer_unique_id', 'customer_state']).agg(
        total_orders=('order_id', 'count'),
        first_order_date=('order_purchase_timestamp', 'min'),
        last_order_date=('order_purchase_timestamp', 'max'),
    ).reset_index()

    repeat['is_repeat_customer'] = (repeat['total_orders'] > 1).astype(int)
    repeat['days_active'] = (
        repeat['last_order_date'] - repeat['first_order_date']
    ).dt.days.fillna(0).astype(int)

    repeat = repeat.merge(hv, on='customer_unique_id', how='left')
    repeat['is_high_value'] = repeat['is_high_value'].fillna(0).astype(int)

    repeat['first_order_date'] = repeat['first_order_date'].dt.date
    repeat['last_order_date']  = repeat['last_order_date'].dt.date
    repeat['total_orders']     = repeat['total_orders'].astype('uint32')
    repeat['updated_at']       = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_repeat_customer')
    client.insert_df('dustinia.mart_repeat_customer', repeat)
    logging.info(f"Wrote {len(repeat):,} rows to mart_repeat_customer")
    client.close()


# Task 4i: Transform - Product Price Range
def transform_product_price_range(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    items = client.query_df(
        "SELECT order_id, product_id, price FROM dustinia.raw_order_items"
    )
    orders = client.query_df(
        "SELECT order_id, order_purchase_timestamp FROM dustinia.raw_orders "
        "WHERE order_status IN ('delivered','shipped','approved')"
    )
    products = client.query_df("""
        SELECT p.product_id,
               COALESCE(t.product_category_name_english,
                        p.product_category_name, 'unknown') AS category_en
        FROM dustinia.raw_products p
        LEFT JOIN dustinia.raw_product_category_translation t
            ON p.product_category_name = t.product_category_name
    """)
    df = items[items['order_id'].isin(orders['order_id'])].copy()
    df = df.merge(orders[['order_id', 'order_purchase_timestamp']], on='order_id', how='left')
    df = df.merge(products, on='product_id', how='left')
    df = df.rename(columns={'category_en': 'category_en'})

    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['year_month']  = df['order_purchase_timestamp'].dt.strftime('%Y-%m')
    df['category_en'] = df['category_en'].fillna('unknown')

    def price_bucket(p):
        if p <= 50:    return 'A. 0-50'
        elif p <= 150: return 'B. 50-150'
        elif p <= 500: return 'C. 150-500'
        else:          return 'D. 500+'

    df['price_bucket'] = df['price'].apply(price_bucket)

    result = df.groupby(['year_month', 'category_en', 'price_bucket']).agg(
        order_count=('order_id', 'count'),
        total_revenue=('price', 'sum'),
        avg_price=('price', 'mean'),
    ).reset_index()

    result = result.rename(columns={'category_en': 'product_category_en'})
    result['order_count'] = result['order_count'].astype('uint32')
    result['updated_at']  = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_product_price_range')
    client.insert_df('dustinia.mart_product_price_range', result)
    logging.info(f"Wrote {len(result):,} rows to mart_product_price_range")
    client.close()


# Task 4j: Transform - Review by Segment
def transform_review_by_segment(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    orders = client.query_df("""
        SELECT o.order_id, o.order_purchase_timestamp,
               o.order_delivered_customer_date,
               o.order_estimated_delivery_date,
               c.customer_unique_id, c.customer_state
        FROM dustinia.raw_orders o
        JOIN dustinia.raw_customers c ON o.customer_id = c.customer_id
    """)

    reviews = client.query_df(
        "SELECT order_id, review_score FROM dustinia.raw_order_reviews"
    )

    payments = client.query_df(
        "SELECT order_id, payment_type FROM dustinia.raw_order_payments"
    )
    pay_dom = payments.groupby('order_id').agg(
        payment_type=('payment_type', lambda x: x.mode()[0])
    ).reset_index()

    hv = client.query_df(
        "SELECT customer_unique_id, is_high_value FROM dustinia.mart_customer_payment_profile"
    )

    for col in ['order_purchase_timestamp', 'order_delivered_customer_date',
                'order_estimated_delivery_date']:
        orders[col] = pd.to_datetime(orders[col])

    orders['delivery_days'] = (
        orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']
    ).dt.days.fillna(0)

    orders['is_late'] = (
        orders['order_delivered_customer_date'] > orders['order_estimated_delivery_date']
    ).fillna(False).astype(int)

    df = orders.merge(reviews, on='order_id', how='left')
    df = df.merge(pay_dom, on='order_id', how='left')
    df = df.merge(hv, on='customer_unique_id', how='left')

    df['review_score']  = df['review_score'].fillna(0.0)
    df['is_high_value'] = df['is_high_value'].fillna(0).astype(int)
    df['payment_type']  = df['payment_type'].fillna('unknown')

    seg = df.groupby(['is_high_value', 'payment_type', 'customer_state']).agg(
        avg_review_score=('review_score', 'mean'),
        total_reviews=('review_score', 'count'),
        pct_5star=('review_score', lambda x: (x == 5).sum() / len(x) * 100),
        pct_1star=('review_score', lambda x: (x == 1).sum() / len(x) * 100),
        avg_delivery_days=('delivery_days', 'mean'),
        late_delivery_rate=('is_late', 'mean'),
    ).reset_index()

    seg['total_reviews'] = seg['total_reviews'].astype('uint32')
    seg['updated_at']    = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_review_by_segment')
    client.insert_df('dustinia.mart_review_by_segment', seg)
    logging.info(f"Wrote {len(seg):,} rows to mart_review_by_segment")
    client.close()


# Task 4k: Transform - MQL Funnel by Origin
def transform_mql_funnel(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    mql   = client.query_df("SELECT mql_id, origin FROM dustinia.raw_mql")
    deals = client.query_df("""
        SELECT mql_id, days_to_close, business_segment
        FROM dustinia.mart_seller_acquisition
    """)

    df = mql.merge(deals, on='mql_id', how='left')
    df['converted']       = df['days_to_close'].notna().astype(int)
    df['days_to_close']   = df['days_to_close'].fillna(0.0)
    df['business_segment'] = df['business_segment'].fillna('unknown')
    df['origin']          = df['origin'].fillna('unknown')

    funnel = df.groupby('origin').agg(
        total_leads=('mql_id', 'count'),
        converted_leads=('converted', 'sum'),
        avg_days_to_close=('days_to_close', lambda x: x[x > 0].mean() if (x > 0).any() else 0.0),
    ).reset_index()

    funnel['conversion_rate'] = (
        funnel['converted_leads'] / funnel['total_leads'] * 100
    ).round(2)
    funnel['avg_days_to_close'] = funnel['avg_days_to_close'].fillna(0.0)

    top_segment = df[df['converted'] == 1].groupby('origin')['business_segment'] \
        .agg(lambda x: x.mode()[0] if len(x) > 0 else 'unknown').reset_index()
    top_segment.columns = ['origin', 'top_business_segment']

    funnel = funnel.merge(top_segment, on='origin', how='left')
    funnel['top_business_segment'] = funnel['top_business_segment'].fillna('unknown')
    funnel['total_leads']     = funnel['total_leads'].astype('uint32')
    funnel['converted_leads'] = funnel['converted_leads'].astype('uint32')
    funnel['updated_at']      = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_mql_funnel')
    client.insert_df('dustinia.mart_mql_funnel', funnel)
    logging.info(f"Wrote {len(funnel):,} rows to mart_mql_funnel")
    client.close()


# Task 4l: Transform - MQL Monthly Trend
def transform_mql_monthly(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    mql   = client.query_df("SELECT mql_id, origin, first_contact_date FROM dustinia.raw_mql")
    deals = client.query_df("SELECT mql_id FROM dustinia.mart_seller_acquisition")

    mql['first_contact_date'] = pd.to_datetime(mql['first_contact_date'])
    mql['year_month'] = mql['first_contact_date'].dt.strftime('%Y-%m')
    mql['converted']  = mql['mql_id'].isin(deals['mql_id']).astype(int)
    mql['origin']     = mql['origin'].fillna('unknown')

    monthly = mql.groupby(['year_month', 'origin']).agg(
        total_leads=('mql_id', 'count'),
        converted_leads=('converted', 'sum'),
    ).reset_index()

    monthly['conversion_rate'] = (
        monthly['converted_leads'] / monthly['total_leads'] * 100
    ).round(2)
    monthly['total_leads']     = monthly['total_leads'].astype('uint32')
    monthly['converted_leads'] = monthly['converted_leads'].astype('uint32')
    monthly['updated_at']      = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_mql_monthly')
    client.insert_df('dustinia.mart_mql_monthly', monthly)
    logging.info(f"Wrote {len(monthly):,} rows to mart_mql_monthly")
    client.close()


# Task 4m: Transform - Lead Behaviour Profile
def transform_lead_behaviour(**context):
    import pandas as pd
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    deals = client.query_df("SELECT * FROM dustinia.mart_seller_acquisition")
    deals['lead_behaviour_profile']  = deals['lead_behaviour_profile'].replace('', 'unknown')
    deals['declared_monthly_revenue'] = deals['declared_monthly_revenue'].fillna(0.0)

    behaviour = deals.groupby('lead_behaviour_profile').agg(
        total_deals=('mql_id', 'count'),
        avg_days_to_close=('days_to_close', 'mean'),
        avg_declared_revenue=('declared_monthly_revenue', 'mean'),
    ).reset_index()

    top_seg = deals.groupby('lead_behaviour_profile')['business_segment'] \
        .agg(lambda x: x.mode()[0] if len(x) > 0 else 'unknown').reset_index()
    top_seg.columns = ['lead_behaviour_profile', 'top_business_segment']

    behaviour = behaviour.merge(top_seg, on='lead_behaviour_profile', how='left')
    behaviour['total_deals'] = behaviour['total_deals'].astype('uint32')
    behaviour['updated_at']  = pd.Timestamp.now()

    client.command('TRUNCATE TABLE dustinia.mart_lead_behaviour')
    client.insert_df('dustinia.mart_lead_behaviour', behaviour)
    logging.info(f"Wrote {len(behaviour):,} rows to mart_lead_behaviour")
    client.close()


# Task 5: Export Marts to PostgreSQL
def export_marts_to_postgres(**context):
    import pandas as pd
    import clickhouse_connect
    from sqlalchemy import create_engine, text

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/metabase')

    with engine.begin() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS dustinia'))

    marts = [
        'mart_customer_payment_profile',
        'mart_order_payment_detail',
        'mart_geo_payment_summary',
        'mart_payment_leakage',
        'mart_monthly_revenue',
        'mart_hv_category_preference',
        'mart_seller_acquisition',
        'mart_seller_performance',
        'mart_repeat_customer',
        'mart_product_price_range',
        'mart_review_by_segment',
        'mart_mql_funnel',
        'mart_mql_monthly',
        'mart_lead_behaviour',
    ]

    for mart in marts:
        df = client.query_df(f'SELECT * FROM dustinia.{mart}')
        df.to_sql(mart, engine, schema='dustinia', if_exists='replace', index=False)
        logging.info(f"Exported {len(df):,} rows to postgres.dustinia.{mart}")

    engine.dispose()
    client.close()
    logging.info("All marts exported to PostgreSQL.")


# Task 6: Compute Payment Leakage
def transform_payment_leakage(**context):
    import pandas as pd
    import clickhouse_connect
    from datetime import date

    client = clickhouse_connect.get_client(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        database='dustinia',
        username='default',
        password='dustinia123',
    )

    detail = client.query_df("""
        SELECT customer_state, payment_type, total_payment_value
        FROM dustinia.mart_order_payment_detail
    """)

    national_avg = detail['total_payment_value'].mean()

    leakage = detail.groupby(['customer_state', 'payment_type']).agg(
        order_count=('total_payment_value', 'count'),
        avg_value=('total_payment_value', 'mean'),
    ).reset_index()

    leakage['leakage_score'] = (
        (national_avg - leakage['avg_value']) / national_avg * leakage['order_count']
    ).clip(lower=0)

    leakage['analysis_date'] = date.today()
    leakage['state']         = leakage['customer_state']
    leakage['updated_at']    = pd.Timestamp.now()

    leakage = leakage[[
        'analysis_date', 'state', 'payment_type',
        'order_count', 'avg_value', 'leakage_score', 'updated_at'
    ]]

    client.command('TRUNCATE TABLE dustinia.mart_payment_leakage')
    client.insert_df('dustinia.mart_payment_leakage', leakage)
    logging.info(f"Wrote {len(leakage):,} rows to mart_payment_leakage")
    client.close()


# Task definitions
t0_schema = PythonOperator(
    task_id='setup_schema',
    python_callable=setup_schema,
    dag=dag,
)

t1_extract = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_raw_data,
    dag=dag,
)

t2_validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

t3_load = PythonOperator(
    task_id='load_raw_to_clickhouse',
    python_callable=load_raw_to_clickhouse,
    dag=dag,
)

t4a_customer = PythonOperator(
    task_id='transform_customer_profiles',
    python_callable=transform_customer_profiles,
    dag=dag,
)

t4b_orders = PythonOperator(
    task_id='transform_order_payments',
    python_callable=transform_order_payments,
    dag=dag,
)

t4c_geo = PythonOperator(
    task_id='transform_geo_summary',
    python_callable=transform_geo_summary,
    dag=dag,
)

t4d_monthly = PythonOperator(
    task_id='transform_monthly_revenue',
    python_callable=transform_monthly_revenue,
    dag=dag,
)

t4e_hv_cat = PythonOperator(
    task_id='transform_hv_category_preference',
    python_callable=transform_hv_category_preference,
    dag=dag,
)

t4f_seller_acq = PythonOperator(
    task_id='transform_seller_acquisition',
    python_callable=transform_seller_acquisition,
    dag=dag,
)

t5_leakage = PythonOperator(
    task_id='compute_leakage_scores',
    python_callable=transform_payment_leakage,
    dag=dag,
)

t4g_seller_perf = PythonOperator(
    task_id='transform_seller_performance',
    python_callable=transform_seller_performance,
    dag=dag,
)

t4h_repeat = PythonOperator(
    task_id='transform_repeat_customer',
    python_callable=transform_repeat_customer,
    dag=dag,
)

t4i_price_range = PythonOperator(
    task_id='transform_product_price_range',
    python_callable=transform_product_price_range,
    dag=dag,
)

t4j_review_seg = PythonOperator(
    task_id='transform_review_by_segment',
    python_callable=transform_review_by_segment,
    dag=dag,
)

t4k_mql_funnel = PythonOperator(
    task_id='transform_mql_funnel',
    python_callable=transform_mql_funnel,
    dag=dag,
)

t4l_mql_monthly = PythonOperator(
    task_id='transform_mql_monthly',
    python_callable=transform_mql_monthly,
    dag=dag,
)

t4m_lead_behaviour = PythonOperator(
    task_id='transform_lead_behaviour',
    python_callable=transform_lead_behaviour,
    dag=dag,
)

t6_export = PythonOperator(
    task_id='export_marts_to_postgres',
    python_callable=export_marts_to_postgres,
    dag=dag,
)


# Execution order
#
#  t0_schema -> t1_extract -> t2_validate -> t3_load
#    t3_load -> t4a_customer -> t4c_geo -> t5_leakage
#    t3_load -> t4b_orders -> t5_leakage
#    t4b_orders -> t4d_monthly
#    t4a_customer + t4b_orders -> t4e_hv_cat
#    t3_load -> t4f_seller_acq

t0_schema >> t1_extract >> t2_validate >> t3_load

t3_load >> [t4a_customer, t4b_orders, t4f_seller_acq]

t4a_customer >> t4c_geo
[t4b_orders, t4c_geo] >> t5_leakage

t4b_orders >> t4d_monthly
[t4a_customer, t4b_orders] >> t4e_hv_cat

t3_load >> [t4g_seller_perf, t4i_price_range]
t4a_customer >> [t4h_repeat, t4j_review_seg]

t4f_seller_acq >> [t4k_mql_funnel, t4l_mql_monthly, t4m_lead_behaviour]

[t5_leakage, t4d_monthly, t4e_hv_cat, t4f_seller_acq,
 t4g_seller_perf, t4h_repeat, t4i_price_range, t4j_review_seg,
 t4k_mql_funnel, t4l_mql_monthly, t4m_lead_behaviour] >> t6_export

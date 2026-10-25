ALTER TABLE IF EXISTS fact_orders
    DROP CONSTRAINT IF EXISTS fk_orders_customer,
    DROP CONSTRAINT IF EXISTS fk_orders_date,
    DROP CONSTRAINT IF EXISTS fk_orders_status,
    DROP CONSTRAINT IF EXISTS fk_orders_location;

ALTER TABLE IF EXISTS fact_sales
    DROP CONSTRAINT IF EXISTS fk_sales_customer,
    DROP CONSTRAINT IF EXISTS fk_sales_product,
    DROP CONSTRAINT IF EXISTS fk_sales_date,
    DROP CONSTRAINT IF EXISTS fk_sales_status,
    DROP CONSTRAINT IF EXISTS fk_sales_location,
    DROP CONSTRAINT IF EXISTS fk_sales_order;

ALTER TABLE IF EXISTS fact_payments
    DROP CONSTRAINT IF EXISTS fk_payments_customer,
    DROP CONSTRAINT IF EXISTS fk_payments_date,
    DROP CONSTRAINT IF EXISTS fk_payments_method,
    DROP CONSTRAINT IF EXISTS fk_payments_status,
    DROP CONSTRAINT IF EXISTS fk_payments_order;

-- ----------------------------------------------------------------------------
-- 1. Primary keys: dimensions (surrogate keys)
-- ----------------------------------------------------------------------------
ALTER TABLE dim_customer          DROP CONSTRAINT IF EXISTS pk_dim_customer,          ADD CONSTRAINT pk_dim_customer          PRIMARY KEY (customer_key);
ALTER TABLE dim_product           DROP CONSTRAINT IF EXISTS pk_dim_product,           ADD CONSTRAINT pk_dim_product           PRIMARY KEY (product_key);
ALTER TABLE dim_date              DROP CONSTRAINT IF EXISTS pk_dim_date,              ADD CONSTRAINT pk_dim_date              PRIMARY KEY (date_key);
ALTER TABLE dim_order_status      DROP CONSTRAINT IF EXISTS pk_dim_order_status,      ADD CONSTRAINT pk_dim_order_status      PRIMARY KEY (order_status_key);
ALTER TABLE dim_shipping_location DROP CONSTRAINT IF EXISTS pk_dim_shipping_location, ADD CONSTRAINT pk_dim_shipping_location PRIMARY KEY (ship_location_key);
ALTER TABLE dim_payment_method    DROP CONSTRAINT IF EXISTS pk_dim_payment_method,    ADD CONSTRAINT pk_dim_payment_method    PRIMARY KEY (method_key);
ALTER TABLE dim_payment_status    DROP CONSTRAINT IF EXISTS pk_dim_payment_status,    ADD CONSTRAINT pk_dim_payment_status    PRIMARY KEY (payment_status_key);

-- SCD2 rule: at most one current version per customer
DROP INDEX IF EXISTS uq_dim_customer_current;
CREATE UNIQUE INDEX uq_dim_customer_current ON dim_customer (customer_id) WHERE is_current;

-- ----------------------------------------------------------------------------
-- 2. Primary keys: facts and OBT (one row per grain)
-- ----------------------------------------------------------------------------
ALTER TABLE fact_orders   DROP CONSTRAINT IF EXISTS pk_fact_orders,   ADD CONSTRAINT pk_fact_orders   PRIMARY KEY (order_id);
ALTER TABLE fact_sales    DROP CONSTRAINT IF EXISTS pk_fact_sales,    ADD CONSTRAINT pk_fact_sales    PRIMARY KEY (order_item_id);
ALTER TABLE fact_payments DROP CONSTRAINT IF EXISTS pk_fact_payments, ADD CONSTRAINT pk_fact_payments PRIMARY KEY (payment_id);
ALTER TABLE obt_sales     DROP CONSTRAINT IF EXISTS pk_obt_sales,     ADD CONSTRAINT pk_obt_sales     PRIMARY KEY (order_item_id);

-- ----------------------------------------------------------------------------
-- 3. Foreign keys: every fact points only at dimensions
-- ----------------------------------------------------------------------------
ALTER TABLE fact_orders
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customer_key)      REFERENCES dim_customer(customer_key),
    ADD CONSTRAINT fk_orders_date     FOREIGN KEY (date_key)          REFERENCES dim_date(date_key),
    ADD CONSTRAINT fk_orders_status   FOREIGN KEY (order_status_key)  REFERENCES dim_order_status(order_status_key),
    ADD CONSTRAINT fk_orders_location FOREIGN KEY (ship_location_key) REFERENCES dim_shipping_location(ship_location_key);

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_sales_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    ADD CONSTRAINT fk_sales_product  FOREIGN KEY (product_key)  REFERENCES dim_product(product_key),
    ADD CONSTRAINT fk_sales_date     FOREIGN KEY (date_key)     REFERENCES dim_date(date_key);

-- These two columns exist in fact_sales only if you added them to the Gold build
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'fact_sales' AND column_name = 'order_status_key') THEN
        ALTER TABLE fact_sales ADD CONSTRAINT fk_sales_status
            FOREIGN KEY (order_status_key) REFERENCES dim_order_status(order_status_key);
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'fact_sales' AND column_name = 'ship_location_key') THEN
        ALTER TABLE fact_sales ADD CONSTRAINT fk_sales_location
            FOREIGN KEY (ship_location_key) REFERENCES dim_shipping_location(ship_location_key);
    END IF;
END $$;

ALTER TABLE fact_payments
    ADD CONSTRAINT fk_payments_customer FOREIGN KEY (customer_key)       REFERENCES dim_customer(customer_key),
    ADD CONSTRAINT fk_payments_date     FOREIGN KEY (date_key)           REFERENCES dim_date(date_key),
    ADD CONSTRAINT fk_payments_method   FOREIGN KEY (method_key)         REFERENCES dim_payment_method(method_key),
    ADD CONSTRAINT fk_payments_status   FOREIGN KEY (payment_status_key) REFERENCES dim_payment_status(payment_status_key);

-- ----------------------------------------------------------------------------
-- 4. Feature table: key is (entity, point in time). Skipped if not synced yet.
--    No foreign key to dim_customer: customer_id repeats across SCD2 versions.
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('public.feat_customer_orders_90d') IS NOT NULL THEN
        ALTER TABLE feat_customer_orders_90d DROP CONSTRAINT IF EXISTS pk_feat_customer_orders;
        ALTER TABLE feat_customer_orders_90d
            ADD CONSTRAINT pk_feat_customer_orders PRIMARY KEY (customer_id, event_timestamp);
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- 5. OPTIONAL fact-to-fact links (off by default).
--    order_id is a degenerate dimension; linking facts to each other goes against the
--    "facts connect only through shared dimensions" design. Enable only if you want the
--    header/line relationship enforced, and expect it to fail if orphans exist.
-- ----------------------------------------------------------------------------
ALTER TABLE fact_sales    ADD CONSTRAINT fk_sales_order    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id);
ALTER TABLE fact_payments ADD CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES fact_orders(order_id);
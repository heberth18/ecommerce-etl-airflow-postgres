CREATE TABLE IF NOT EXISTS sales (
    order_id        SERIAL PRIMARY KEY,
    customer_id     VARCHAR(50) NOT NULL,
    product         VARCHAR(100) NOT NULL,
    category        VARCHAR(50),
    price           NUMERIC(10, 2) CHECK (price >= 0),
    quantity        INT CHECK (quantity > 0),
    order_date      TIMESTAMP NOT NULL,
    payment_type    VARCHAR(50),
    country         VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);

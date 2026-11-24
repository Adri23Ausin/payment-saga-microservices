CREATE TABLE orders (
    id          UUID PRIMARY KEY,
    customer_id VARCHAR(50)    NOT NULL,
    status      VARCHAR(20)    NOT NULL,
    total       NUMERIC(12, 2) NOT NULL,
    reason      TEXT,
    created_at  TIMESTAMPTZ    NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL
);

CREATE INDEX orders_customer_idx ON orders (customer_id, created_at DESC);

CREATE TABLE order_items (
    order_id   UUID           NOT NULL REFERENCES orders (id),
    line       INTEGER        NOT NULL,
    sku        VARCHAR(50)    NOT NULL,
    quantity   INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    PRIMARY KEY (order_id, line)
);

-- What happened to the order and when: the saga as the customer would see it
CREATE TABLE order_history (
    order_id    UUID        NOT NULL REFERENCES orders (id),
    occurred_at TIMESTAMPTZ NOT NULL,
    step        VARCHAR(50) NOT NULL,
    detail      TEXT
);

CREATE INDEX order_history_order_idx ON order_history (order_id, occurred_at);

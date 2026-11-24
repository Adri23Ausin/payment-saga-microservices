CREATE TABLE accounts (
    customer_id VARCHAR(50) PRIMARY KEY,
    -- The CHECK is the last line of defence: charges are conditional on the balance anyway
    balance     NUMERIC(12, 2) NOT NULL CHECK (balance >= 0)
);

-- One payment per order: the order id is the key, so a charge can't happen twice
CREATE TABLE payments (
    order_id    UUID PRIMARY KEY,
    customer_id VARCHAR(50)    NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    status      VARCHAR(20)    NOT NULL,
    reason      TEXT,
    created_at  TIMESTAMPTZ    NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL
);

-- Demo customers
INSERT INTO accounts (customer_id, balance) VALUES
    ('alice', 1000.00),
    ('bob', 50.00),
    ('carol', 250.00);

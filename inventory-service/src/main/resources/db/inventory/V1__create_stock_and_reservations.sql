CREATE TABLE stock (
    sku       VARCHAR(50) PRIMARY KEY,
    available INTEGER     NOT NULL CHECK (available >= 0)
);

CREATE TABLE reservations (
    order_id UUID        NOT NULL,
    sku      VARCHAR(50) NOT NULL REFERENCES stock (sku),
    quantity INTEGER     NOT NULL CHECK (quantity > 0),
    reserved_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, sku)
);

-- Demo catalogue: few monitors and no webcams, to see the compensation happen
INSERT INTO stock (sku, available) VALUES
    ('KEYBOARD', 50),
    ('MOUSE', 100),
    ('MONITOR', 2),
    ('WEBCAM', 0);

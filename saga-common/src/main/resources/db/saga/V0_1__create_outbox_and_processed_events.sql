-- Shared by every service (Flyway location classpath:db/saga), each in its own database

CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    topic        VARCHAR(100) NOT NULL,
    message_key  VARCHAR(100) NOT NULL,
    event_type   VARCHAR(50)  NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ
);

-- The relay only looks for what is still unpublished
CREATE INDEX outbox_unpublished_idx ON outbox (created_at, id) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    topic        VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL
);

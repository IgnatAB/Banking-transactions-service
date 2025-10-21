CREATE TABLE IF NOT EXISTS dlq_messages
(
    id             BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL UNIQUE,
    client_id      UUID,
    from_account   VARCHAR(100),
    to_account     VARCHAR(100),
    type           VARCHAR(32),
    amount         DECIMAL(15, 2),
    created_at     TIMESTAMP WITH TIME ZONE,
    status         VARCHAR(32),
    error_reason   TEXT
);
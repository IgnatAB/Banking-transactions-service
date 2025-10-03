CREATE TABLE IF NOT EXISTS transactions
(
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  UUID  NOT NULL UNIQUE,
    client_id       UUID  NOT NULL ,
    from_account    VARCHAR(100)             NOT NULL,
    to_account      VARCHAR(100)             NOT NULL,
    type            VARCHAR(32)              NOT NULL,
    amount          DECIMAL(15, 2)           NOT NULL,
    created_at      timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status          VARCHAR(32)              NOT NULL
);
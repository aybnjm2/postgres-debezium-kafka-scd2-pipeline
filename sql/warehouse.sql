CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE warehouse.dim_customers (
    surrogate_key   BIGSERIAL PRIMARY KEY,
    customer_id     INT NOT NULL,
    full_name       TEXT,
    email           TEXT,
    city            TEXT,
    valid_from      TIMESTAMPTZ NOT NULL,
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN NOT NULL DEFAULT true,
    source_lsn      BIGINT
);

CREATE TABLE warehouse.dim_accounts (
    surrogate_key   BIGSERIAL PRIMARY KEY,
    account_id      INT NOT NULL,
    customer_id     INT NOT NULL,
    account_type    TEXT,
    balance         NUMERIC(14,2),
    status          TEXT,
    valid_from      TIMESTAMPTZ NOT NULL,
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN NOT NULL DEFAULT true,
    source_lsn      BIGINT
);

-- table de faits 
CREATE TABLE warehouse.fact_transactions (
    txn_id       BIGINT PRIMARY KEY,
    account_id   INT NOT NULL,
    amount       NUMERIC(14,2),
    created_at   TIMESTAMPTZ,
    source_lsn   BIGINT
);

-- accelere "trouve la ligne courante pour ce client"
CREATE INDEX idx_dim_customers_current ON warehouse.dim_customers(customer_id) WHERE is_current;
CREATE INDEX idx_dim_accounts_current ON warehouse.dim_accounts(account_id) WHERE is_current;
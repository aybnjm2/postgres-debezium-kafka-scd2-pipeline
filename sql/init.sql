    CREATE TABLE customers (
    customer_id  SERIAL PRIMARY KEY,
    full_name    TEXT NOT NULL,
    email        TEXT UNIQUE NOT NULL,
    city         TEXT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    account_id   SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(customer_id),
    account_type TEXT NOT NULL,            -- checking / savings / credit
    balance      NUMERIC(14,2) NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'open',  -- open / closed
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    txn_id       BIGSERIAL PRIMARY KEY,
    account_id   INT NOT NULL REFERENCES accounts(account_id),
    amount       NUMERIC(14,2) NOT NULL,   -- negative = withdrawal
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ces lignes sont necessaires pour que Debezium puisse capturer les changements de ces tables - PROBELM (before:null) - inconvenient lots of I/O ops and too much data is written on WAL
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
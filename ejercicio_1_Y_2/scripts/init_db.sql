CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.customers (
    id INT PRIMARY KEY,
    name TEXT,
    status TEXT,
    created_at TIMESTAMP,
    run_id UUID,
    created_on TIMESTAMP DEFAULT NOW()
);
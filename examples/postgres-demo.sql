-- Synthetic discovery/archive-planning fixture for a local development database.
-- Run as the source user. Deliberately fails if coldctl_demo already exists;
-- the transaction preserves existing data rather than overwriting it.
BEGIN;
CREATE SCHEMA coldctl_demo;

CREATE TABLE coldctl_demo.customers (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL
);

CREATE TABLE coldctl_demo.orders (
    id bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES coldctl_demo.customers(id),
    status text NOT NULL CHECK (status IN ('completed', 'pending', 'cancelled')),
    total numeric(12,2) NOT NULL CHECK (total >= 0),
    created_at timestamptz NOT NULL,
    completed_at timestamptz
);
CREATE INDEX orders_created_at_idx ON coldctl_demo.orders (created_at);
CREATE INDEX orders_customer_id_idx ON coldctl_demo.orders (customer_id);
CREATE INDEX orders_completed_at_idx ON coldctl_demo.orders (completed_at)
    WHERE status = 'completed';

CREATE TABLE coldctl_demo.events (
    event_date date NOT NULL,
    id bigint NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (event_date, id)
);
CREATE INDEX events_occurred_at_idx ON coldctl_demo.events (occurred_at);

-- An empty table without a primary key exercises missing-key discovery.
CREATE TABLE coldctl_demo.import_staging (
    external_id text,
    received_at timestamp,
    note text
);

INSERT INTO coldctl_demo.customers
SELECT n, 'Demo customer ' || n, 'customer' || n || '@example.test',
       CURRENT_TIMESTAMP - INTERVAL '3 years' + n * INTERVAL '1 day'
FROM generate_series(1, 10) AS g(n);

-- 60 old orders and 40 recent orders, all synthetic.
INSERT INTO coldctl_demo.orders
SELECT n, ((n - 1) % 10) + 1,
       CASE WHEN n <= 60 THEN 'completed'
            WHEN n % 5 = 0 THEN 'cancelled' ELSE 'pending' END,
       (20 + n * 3.75)::numeric(12,2),
       CURRENT_TIMESTAMP - (CASE WHEN n <= 60 THEN 400 + n ELSE n - 60 END) * INTERVAL '1 day',
       CASE WHEN n <= 60 THEN CURRENT_TIMESTAMP - (399 + n) * INTERVAL '1 day' ELSE NULL END
FROM generate_series(1, 100) AS g(n);

INSERT INTO coldctl_demo.events
SELECT (CURRENT_TIMESTAMP - n * INTERVAL '2 days')::date, n,
       CASE WHEN n % 2 = 0 THEN 'order_created' ELSE 'customer_login' END,
       CURRENT_TIMESTAMP - n * INTERVAL '2 days',
       jsonb_build_object('synthetic', true, 'sequence', n)
FROM generate_series(1, 250) AS g(n);

ANALYZE coldctl_demo.customers;
ANALYZE coldctl_demo.orders;
ANALYZE coldctl_demo.events;
ANALYZE coldctl_demo.import_staging;
COMMIT;

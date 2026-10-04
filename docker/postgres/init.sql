CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';

CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO public.users (name, email)
VALUES
('Kafka CDC', 'cdc@test.com'),
('Debezium User', 'debezium@test.com');
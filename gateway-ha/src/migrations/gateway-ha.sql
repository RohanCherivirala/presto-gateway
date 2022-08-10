CREATE DATABASE IF NOT EXISTS prestogateway;
USE prestogateway;

CREATE TABLE IF NOT EXISTS routing_groups (
name VARCHAR(256) PRIMARY KEY,
active BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS gateway_backend (
name VARCHAR(256) PRIMARY KEY,
routing_group VARCHAR (256),
backend_url VARCHAR (256),
active BOOLEAN
);

CREATE TABLE IF NOT EXISTS query_history (
query_id VARCHAR(256) PRIMARY KEY,
query_text VARCHAR (256),
created bigint,
backend_url VARCHAR (256),
user_name VARCHAR(256),
source VARCHAR(256)
);

INSERT INTO routing_groups SELECT DISTINCT routing_group, true FROM gateway_backend;
ALTER TABLE gateway_backend ADD CONSTRAINT routing_group_constraint FOREIGN KEY (routing_group) references routing_groups(name) ON DELETE CASCADE;

CREATE INDEX query_history_created_idx ON query_history(created);

#!/bin/sh
# Runs once, when the Postgres volume is first created: one database and one least-privilege role per service.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE n8n LOGIN PASSWORD '${N8N_DB_PASSWORD}';
CREATE DATABASE n8n OWNER n8n;
CREATE ROLE ops_tools LOGIN PASSWORD '${OPS_DB_PASSWORD}';
CREATE DATABASE ops OWNER ops_tools;
REVOKE ALL ON DATABASE n8n FROM PUBLIC;
REVOKE ALL ON DATABASE ops FROM PUBLIC;
SQL

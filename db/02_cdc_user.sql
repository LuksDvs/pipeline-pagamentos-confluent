-- Usuário do conector de CDC com privilégio mínimo:
-- pode conectar, replicar (ler o WAL) e ler as cinco tabelas. Não escreve nada.
-- Uso: psql ... -v cdc_user=cdc_user -v cdc_password='senha' -f db/02_cdc_user.sql

SELECT format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user')
\gexec

SELECT format('ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password')
\gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'cdc_user')
\gexec

GRANT USAGE ON SCHEMA public TO :"cdc_user";
GRANT SELECT ON customers, accounts, cards, merchants, transactions TO :"cdc_user";

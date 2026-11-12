-- Limpeza no Postgres ao derrubar o pipeline.
-- O slot de replicação PRECISA ser removido: um slot sem consumidor
-- segura o WAL e pode encher o disco do banco.
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'desafio_slot' AND NOT active;

DROP PUBLICATION IF EXISTS desafio_pub;

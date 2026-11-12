-- Camada 3: gera um INSERT, um UPDATE e um DELETE para ver os três
-- tipos de envelope do Debezium (op=c, op=u, op=d + tombstone) no tópico
-- desafio.public.merchants. Rode depois que o conector estiver RUNNING.

INSERT INTO merchants (legal_name, mcc, city, bpo_segment)
VALUES ('Padaria Demonstração CDC Ltda', '5814', 'Belo Horizonte', 'alimentacao');

UPDATE merchants
SET city = 'Contagem'
WHERE legal_name = 'Padaria Demonstração CDC Ltda';

DELETE FROM merchants
WHERE legal_name = 'Padaria Demonstração CDC Ltda';

-- Uma nova versão de conta: o temporal join da Camada 4 deve usar
-- a versão vigente no momento de cada transação.
UPDATE accounts
SET risk_tier = 'alto', updated_at = now()
WHERE account_id = 1;

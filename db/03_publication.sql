-- Publicação criada à mão (o conector usa publication.autocreate.mode=disabled),
-- assim o usuário de CDC não precisa ser dono das tabelas.
DROP PUBLICATION IF EXISTS desafio_pub;
CREATE PUBLICATION desafio_pub
    FOR TABLE customers, accounts, cards, merchants, transactions;

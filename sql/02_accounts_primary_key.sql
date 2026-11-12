-- Tabela versionada do temporal join: precisa de chave primária.
-- (Se o Flink já inferiu a chave a partir da key do tópico, este statement
-- falha com "already has a primary key" e pode ser ignorado.)
ALTER TABLE `desafio.public.accounts`
  ADD CONSTRAINT pk_accounts PRIMARY KEY (account_id) NOT ENFORCED;

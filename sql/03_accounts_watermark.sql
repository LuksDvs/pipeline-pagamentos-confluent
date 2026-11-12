-- O tempo de versão da conta é o updated_at gravado no Postgres.
ALTER TABLE `desafio.public.accounts`
  MODIFY WATERMARK FOR updated_at AS updated_at;

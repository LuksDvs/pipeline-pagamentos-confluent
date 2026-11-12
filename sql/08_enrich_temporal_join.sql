-- Temporal join: cada transação recebe a versão da conta vigente
-- NO MOMENTO da transação (e não a versão atual).
INSERT INTO `desafio.payments.enriched`
SELECT
  t.transaction_id,
  t.card_id,
  t.account_id,
  t.merchant_id,
  t.amount,
  t.channel,
  t.status,
  a.customer_id,
  a.account_type,
  a.status AS account_status,
  a.risk_tier,
  a.credit_limit,
  t.event_time
FROM `desafio.payments.transactions` AS t
JOIN `desafio.public.accounts` FOR SYSTEM_TIME AS OF t.event_time AS a
  ON t.account_id = a.account_id;

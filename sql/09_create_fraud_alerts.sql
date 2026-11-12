-- alert_id é determinístico: o consumidor pode deduplicar por ele (idempotência).
CREATE TABLE `desafio.fraud.detected` (
  alert_id     STRING,
  rule_name    STRING,
  card_id      INT,
  account_id   INT,
  risk_tier    STRING,
  tx_count     BIGINT,
  total_amount DECIMAL(15, 2),
  first_tx_id  BIGINT,
  last_tx_id   BIGINT,
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3)
)
DISTRIBUTED INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');

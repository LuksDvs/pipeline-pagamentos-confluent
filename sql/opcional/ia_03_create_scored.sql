CREATE TABLE `desafio.fraud.scored` (
  alert_id        STRING,
  rule_name       STRING,
  card_id         INT,
  total_amount    DECIMAL(15, 2),
  risk_assessment STRING
)
DISTRIBUTED INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');

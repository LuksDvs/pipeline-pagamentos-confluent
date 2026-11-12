-- Fila separada para eventos com defeito: o pipeline não para por causa deles,
-- e alguém pode analisar e reprocessar depois.
CREATE TABLE `desafio.payments.quarantine` (
  transaction_id BIGINT,
  card_id        INT,
  merchant_id    INT,
  amount         DECIMAL(15, 2),
  reason         STRING,
  event_time     TIMESTAMP_LTZ(3),
  detected_at    TIMESTAMP_LTZ(3)
)
DISTRIBUTED INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');

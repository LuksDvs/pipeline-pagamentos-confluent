-- Transações válidas, achatadas e com tempo de evento (created_at).
-- 1 bucket: com uma partição só, o watermark não fica preso em partição ociosa.
CREATE TABLE `desafio.payments.transactions` (
  transaction_id BIGINT,
  card_id        INT,
  account_id     INT,
  merchant_id    INT,
  amount         DECIMAL(15, 2),
  currency       STRING,
  channel        STRING,
  status         STRING,
  event_time     TIMESTAMP_LTZ(3) NOT NULL,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
DISTRIBUTED INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');

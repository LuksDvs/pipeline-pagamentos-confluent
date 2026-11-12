CREATE TABLE `desafio.payments.enriched` (
  transaction_id BIGINT,
  card_id        INT,
  account_id     INT,
  merchant_id    INT,
  amount         DECIMAL(15, 2),
  channel        STRING,
  status         STRING,
  customer_id    INT,
  account_type   STRING,
  account_status STRING,
  risk_tier      STRING,
  credit_limit   DECIMAL(15, 2),
  event_time     TIMESTAMP_LTZ(3) NOT NULL,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
DISTRIBUTED BY HASH(card_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');

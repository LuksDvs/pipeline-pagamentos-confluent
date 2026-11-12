-- RULE_VELOCITY: três transações no mesmo cartão em até 60 segundos.
INSERT INTO `desafio.fraud.detected`
SELECT
  CONCAT('VEL-', CAST(card_id AS STRING), '-', CAST(first_tx_id AS STRING)) AS alert_id,
  'RULE_VELOCITY' AS rule_name,
  card_id, account_id, risk_tier, tx_count, total_amount,
  first_tx_id, last_tx_id, window_start, window_end
FROM `desafio.payments.enriched`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY event_time
  MEASURES
    FIRST(T.transaction_id)               AS first_tx_id,
    LAST(T.transaction_id)                AS last_tx_id,
    LAST(T.account_id)                    AS account_id,
    LAST(T.risk_tier)                     AS risk_tier,
    COUNT(T.transaction_id)               AS tx_count,
    CAST(SUM(T.amount) AS DECIMAL(15, 2)) AS total_amount,
    FIRST(T.event_time)                   AS window_start,
    LAST(T.event_time)                    AS window_end
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3}) WITHIN INTERVAL '60' SECOND
  DEFINE T AS TRUE
) AS m;

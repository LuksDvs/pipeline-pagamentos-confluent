-- RULE_DUPLICATE (2ª regra, outra janela e outro critério): o mesmo cartão
-- paga o mesmo valor ao mesmo estabelecimento duas vezes seguidas em até
-- 5 minutos. É o "pagamento em dobro" que um BPO financeiro precisa pegar
-- antes da conciliação.
INSERT INTO `desafio.fraud.detected`
SELECT
  CONCAT('DUP-', CAST(card_id AS STRING), '-', CAST(first_tx_id AS STRING)) AS alert_id,
  'RULE_DUPLICATE' AS rule_name,
  card_id, account_id, risk_tier, CAST(2 AS BIGINT) AS tx_count, total_amount,
  first_tx_id, last_tx_id, window_start, window_end
FROM `desafio.payments.enriched`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY event_time
  MEASURES
    A.transaction_id                            AS first_tx_id,
    B.transaction_id                            AS last_tx_id,
    B.account_id                                AS account_id,
    B.risk_tier                                 AS risk_tier,
    CAST(A.amount + B.amount AS DECIMAL(15, 2)) AS total_amount,
    A.event_time                                AS window_start,
    B.event_time                                AS window_end
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A B) WITHIN INTERVAL '5' MINUTE
  DEFINE
    A AS TRUE,
    B AS B.merchant_id = A.merchant_id AND B.amount = A.amount
) AS m;

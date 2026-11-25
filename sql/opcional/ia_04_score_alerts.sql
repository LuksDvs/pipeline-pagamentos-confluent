-- A IA só é chamada para os ALERTAS (poucas linhas), não para cada
-- transação: o volume de chamadas ao provedor e o custo ficam sob controle.
INSERT INTO `desafio.fraud.scored`
SELECT a.alert_id, a.rule_name, a.card_id, a.total_amount, p.risk_assessment
FROM `desafio.fraud.detected` AS a,
LATERAL TABLE(
  ML_PREDICT(
    'desafio_risk_model',
    CONCAT(
      'Regra: ', a.rule_name,
      '; transações: ', CAST(a.tx_count AS STRING),
      '; valor total: ', CAST(a.total_amount AS STRING),
      '; intervalo: ', CAST(TIMESTAMPDIFF(SECOND, a.window_start, a.window_end) AS STRING), ' s'
    )
  )
) AS p (risk_assessment);

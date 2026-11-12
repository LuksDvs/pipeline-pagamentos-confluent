-- Um único job (statement set) separa as transações válidas das com defeito.
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO `desafio.payments.transactions`
  SELECT
    `after`.transaction_id,
    `after`.card_id,
    `after`.account_id,
    `after`.merchant_id,
    `after`.amount,
    `after`.currency,
    `after`.channel,
    `after`.status,
    CAST(`after`.created_at AS TIMESTAMP_LTZ(3))
  FROM `desafio.public.transactions`
  WHERE op IN ('c', 'r')
    AND `after`.amount IS NOT NULL
    AND `after`.amount > 0
    AND `after`.merchant_id IS NOT NULL;

  INSERT INTO `desafio.payments.quarantine`
  SELECT
    `after`.transaction_id,
    `after`.card_id,
    `after`.merchant_id,
    `after`.amount,
    CASE
      WHEN `after`.amount IS NULL THEN 'amount_nulo'
      WHEN `after`.amount <= 0 THEN 'amount_nao_positivo'
      ELSE 'merchant_ausente'
    END,
    CAST(`after`.created_at AS TIMESTAMP_LTZ(3)),
    CURRENT_TIMESTAMP
  FROM `desafio.public.transactions`
  WHERE op IN ('c', 'r')
    AND (`after`.amount IS NULL OR `after`.amount <= 0 OR `after`.merchant_id IS NULL);
END;

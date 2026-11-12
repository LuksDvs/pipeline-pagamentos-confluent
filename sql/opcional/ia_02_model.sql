CREATE MODEL `desafio_risk_model`
INPUT (prompt STRING)
OUTPUT (risk_assessment STRING)
WITH (
  'provider'             = 'openai',
  'task'                 = 'text_generation',
  'openai.connection'    = 'desafio-openai',
  'openai.model_version' = 'gpt-4o-mini',
  'openai.system_prompt' = 'Você é analista de fraude de cartões. Responda apenas com BAIXO, MEDIO ou ALTO, seguido de um traço e uma justificativa de até 15 palavras.'
);

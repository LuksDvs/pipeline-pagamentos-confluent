-- Evolução opcional: score de risco com IA (ENABLE_AI_SCORE=true no .env).
-- A chave fica guardada na connection, fora das queries e do catálogo.
CREATE CONNECTION `desafio-openai` WITH (
  'type'     = 'openai',
  'endpoint' = 'https://api.openai.com/v1/chat/completions',
  'api-key'  = '${OPENAI_API_KEY}'
);

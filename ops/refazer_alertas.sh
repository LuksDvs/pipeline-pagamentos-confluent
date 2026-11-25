#!/usr/bin/env bash
# Recria a tabela de alertas e as duas regras de fraude do zero.
# Uso: ./ops/refazer_alertas.sh  (depois de mudar sql/09, sql/10 ou sql/11)
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_tools confluent jq
load_env
need_state ENV_ID CLUSTER_ID SA_FLINK_ID FLINK_POOL_ID
flink_region

log "Parando as regras e apagando a tabela de alertas"
for name in desafio-10-rule-velocity desafio-11-rule-duplicate desafio-09-create-fraud-alerts; do
  confluent flink statement delete "$name" --environment "$ENV_ID" --force >/dev/null 2>&1 \
    && ok "$name apagado" || warn "$name não existia"
done
confluent flink statement create "desafio-drop-alertas-$(date +%s)" \
  --sql 'DROP TABLE `desafio.fraud.detected`;' \
  --compute-pool "$FLINK_POOL_ID" --service-account "$SA_FLINK_ID" \
  --database "$CLUSTER_ID" --environment "$ENV_ID" \
  --property "sql.current-catalog=$CC_ENV_NAME" --wait >/dev/null \
  && ok "tabela desafio.fraud.detected apagada" || warn "a tabela não existia"
echo "    aguardando 30 s para o tópico ser removido..."; sleep 30

for file in "$ROOT_DIR"/sql/09_*.sql "$ROOT_DIR"/sql/10_*.sql "$ROOT_DIR"/sql/11_*.sql; do
  run_flink "$file"
done

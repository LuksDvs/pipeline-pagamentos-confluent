#!/usr/bin/env bash
# Derruba tudo o que o setup.sh criou, na ordem certa, e prova que nada
# ficou cobrando. Uso: ./teardown.sh
set -euo pipefail
# shellcheck source=ops/lib.sh
source "$(dirname "$0")/ops/lib.sh"
require_tools confluent jq psql
load_env
mkdir -p "$EVID_DIR"

if [[ -n "${ENV_ID:-}" ]]; then
  confluent environment use "$ENV_ID" >/dev/null

  log "Parando e apagando os statements do Flink"
  confluent flink statement list --environment "$ENV_ID" -o json 2>/dev/null \
    | jq -r '.[] | select(.name | startswith("desafio-")) | .name' \
    | while read -r name; do
        confluent flink statement delete "$name" --environment "$ENV_ID" --force && ok "$name"
      done

  if [[ -n "${FLINK_POOL_ID:-}" ]]; then
    log "Apagando o compute pool"
    confluent flink compute-pool delete "$FLINK_POOL_ID" --environment "$ENV_ID" --force || true
  fi

  if [[ -n "${CONNECTOR_ID:-}" && -n "${CLUSTER_ID:-}" ]]; then
    log "Apagando o conector de CDC"
    confluent connect cluster delete "$CONNECTOR_ID" --cluster "$CLUSTER_ID" --force || true
  fi

  log "Evidência: nada de conector nem de statement ativo"
  {
    echo "## confluent connect cluster list"
    confluent connect cluster list --cluster "$CLUSTER_ID" 2>&1 || true
    echo; echo "## confluent flink statement list"
    confluent flink statement list --environment "$ENV_ID" 2>&1 || true
  } | tee "$EVID_DIR/teardown.txt"

  log "Apagando o environment (leva junto cluster, tópicos e schemas)"
  confluent environment delete "$ENV_ID" --force || true
fi

log "Apagando API keys e service accounts"
for key in PRODUCER_API_KEY CONSUMER_API_KEY CONSUMER_SR_API_KEY OBSERVER_CLOUD_KEY; do
  [[ -n "${!key:-}" ]] && { confluent api-key delete "${!key}" --force || true; }
done
for sa in SA_PRODUCER_ID SA_CONSUMER_ID SA_FLINK_ID SA_OBSERVER_ID; do
  [[ -n "${!sa:-}" ]] && { confluent iam service-account delete "${!sa}" --force || true; }
done

log "Postgres: removendo o slot de replicação e a publicação"
# Um slot esquecido segura o WAL e pode encher o disco do banco.
psql_admin -f "$ROOT_DIR/db/99_cleanup.sql" || warn "limpe o slot manualmente: db/99_cleanup.sql"

rm -f "$STATE_FILE"
ok "pipeline derrubado. Confira também em https://confluent.cloud que não sobrou nada."

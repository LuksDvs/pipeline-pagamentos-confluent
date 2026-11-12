#!/usr/bin/env bash
# Sobe o pipeline do zero, uma camada por vez.
#
#   ./setup.sh fundacao        Camada 1: environment, cluster, identidades, ACLs
#   ./setup.sh contrato        Camada 2: schema no Schema Registry + testes de compatibilidade
#   ./setup.sh ingestao        Camada 3: Postgres, seed e conector de CDC
#   ./setup.sh processamento   Camada 4: Flink SQL (enriquecimento, quarentena, regras)
#   ./setup.sh opcionais       Tableflow e/ou IA, se ligados no .env
#   ./setup.sh all             Todas as camadas em sequência
#
# Pré-requisitos: confluent CLI (logado com `confluent login --save`), jq, psql, python3.
set -euo pipefail
# shellcheck source=ops/lib.sh
source "$(dirname "$0")/ops/lib.sh"

TOPICS_CDC=(customers accounts cards merchants transactions)
CONTRACT_SUBJECT="desafio.payments.contract-value"

camada_fundacao() {
  log "Camada 1 - Fundação"

  if [[ -z "${ENV_ID:-}" ]]; then
    save_state ENV_ID "$(confluent environment create "$CC_ENV_NAME" \
      --governance-package essentials -o json | jq -r .id)"
  fi
  confluent environment use "$ENV_ID" >/dev/null
  ok "environment $CC_ENV_NAME ($ENV_ID)"

  if [[ -z "${CLUSTER_ID:-}" ]]; then
    save_state CLUSTER_ID "$(confluent kafka cluster create "$CC_CLUSTER_NAME" \
      --cloud "$CC_CLOUD" --region "$CC_REGION" --type basic -o json | jq -r .id)"
  fi
  confluent kafka cluster use "$CLUSTER_ID" >/dev/null
  until [[ "$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r .status)" == "UP" ]]; do
    echo "    aguardando o cluster ficar UP..."; sleep 15
  done
  ok "cluster $CC_CLUSTER_NAME ($CLUSTER_ID) UP"

  # Uma identidade por papel. A conta pessoal só é usada aqui e no teardown.
  local sa
  for sa in producer consumer flink observer; do
    local var="SA_${sa^^}_ID"
    if [[ -z "${!var:-}" ]]; then
      save_state "$var" "$(confluent iam service-account create "desafio-$sa" \
        --description "Desafio pipeline de pagamentos: $sa" -o json | jq -r .id)"
    fi
    ok "service account desafio-$sa (${!var})"
  done

  if [[ -z "${PRODUCER_API_KEY:-}" ]]; then
    local key_json
    key_json="$(confluent api-key create --resource "$CLUSTER_ID" --service-account "$SA_PRODUCER_ID" -o json)"
    save_state PRODUCER_API_KEY "$(jq -r .api_key <<<"$key_json")"
    save_state PRODUCER_API_SECRET "$(jq -r .api_secret <<<"$key_json")"
  fi
  if [[ -z "${CONSUMER_API_KEY:-}" ]]; then
    local key_json
    key_json="$(confluent api-key create --resource "$CLUSTER_ID" --service-account "$SA_CONSUMER_ID" -o json)"
    save_state CONSUMER_API_KEY "$(jq -r .api_key <<<"$key_json")"
    save_state CONSUMER_API_SECRET "$(jq -r .api_secret <<<"$key_json")"
  fi
  ok "API keys de producer e consumer guardadas no .state"

  log "ACLs (mínimo privilégio)"
  # producer = o conector de CDC: só cria e escreve nos tópicos do desafio
  confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" \
    --operations create,write,describe,describe-configs --topic desafio --prefix --cluster "$CLUSTER_ID"
  confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" \
    --operations create,write,describe --topic __debezium-heartbeat --prefix --cluster "$CLUSTER_ID"
  confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" \
    --operations describe,idempotent-write --cluster-scope --cluster "$CLUSTER_ID"
  # consumer: só lê os tópicos do desafio e usa grupos desafio-*
  confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" \
    --operations read,describe --topic desafio --prefix --cluster "$CLUSTER_ID"
  confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" \
    --operations read --consumer-group desafio --prefix --cluster "$CLUSTER_ID"
  ok "ACLs criadas"

  log "Tópicos de CDC com 1 partição (watermark sem partição ociosa)"
  local t
  for t in "${TOPICS_CDC[@]}"; do
    confluent kafka topic create "desafio.public.$t" --partitions 1 --cluster "$CLUSTER_ID" 2>/dev/null \
      && ok "desafio.public.$t" || warn "desafio.public.$t já existe"
  done

  log "Schema Registry"
  local sr_json
  sr_json="$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json)"
  save_state SR_ID "$(jq -r '.cluster_id // .cluster // .id' <<<"$sr_json")"
  save_state SR_URL "$(jq -r '.endpoint_url // .url // .endpoint' <<<"$sr_json")"
  if [[ -z "${CONSUMER_SR_API_KEY:-}" ]]; then
    local key_json
    key_json="$(confluent api-key create --resource "$SR_ID" --service-account "$SA_CONSUMER_ID" -o json)"
    save_state CONSUMER_SR_API_KEY "$(jq -r .api_key <<<"$key_json")"
    save_state CONSUMER_SR_API_SECRET "$(jq -r .api_secret <<<"$key_json")"
  fi
  ok "Schema Registry $SR_ID"

  log "RBAC do Flink, do consumer (schemas) e do observer (métricas)"
  local role
  for role in DeveloperRead DeveloperWrite DeveloperManage; do
    rb --principal "User:$SA_FLINK_ID" --role "$role" \
      --environment "$ENV_ID" --cloud-cluster "$CLUSTER_ID" --kafka-cluster "$CLUSTER_ID" \
      --resource Topic:desafio --prefix
    rb --principal "User:$SA_FLINK_ID" --role "$role" \
      --environment "$ENV_ID" --schema-registry-cluster "$SR_ID" \
      --resource Subject:desafio --prefix
  done
  rb --principal "User:$SA_FLINK_ID" --role DeveloperWrite \
    --environment "$ENV_ID" --cloud-cluster "$CLUSTER_ID" --kafka-cluster "$CLUSTER_ID" \
    --resource Transactional-Id:_confluent-flink_ --prefix
  rb --principal "User:$SA_CONSUMER_ID" --role DeveloperRead \
    --environment "$ENV_ID" --schema-registry-cluster "$SR_ID" \
    --resource Subject:desafio --prefix
  rb --principal "User:$SA_OBSERVER_ID" --role MetricsViewer \
    --environment "$ENV_ID"
  if [[ -z "${OBSERVER_CLOUD_KEY:-}" ]]; then
    local key_json
    key_json="$(confluent api-key create --resource cloud --service-account "$SA_OBSERVER_ID" -o json)"
    save_state OBSERVER_CLOUD_KEY "$(jq -r .api_key <<<"$key_json")"
    save_state OBSERVER_CLOUD_SECRET "$(jq -r .api_secret <<<"$key_json")"
  fi
  ok "role bindings criados"

  log "Evidência da Camada 1"
  {
    echo "## confluent iam service-account list"
    confluent iam service-account list | grep -E 'ID|desafio-' || true
    echo; echo "## confluent kafka acl list"
    confluent kafka acl list --cluster "$CLUSTER_ID"
  } | tee "$EVID_DIR/camada1-identidades-acls.txt"
}

camada_contrato() {
  log "Camada 2 - Contrato de dados"
  need_state ENV_ID
  confluent schema-registry schema create --subject "$CONTRACT_SUBJECT" \
    --schema "$ROOT_DIR/schemas/payment_event_v1.avsc" --type avro --environment "$ENV_ID"
  confluent schema-registry subject update "$CONTRACT_SUBJECT" --compatibility backward \
    --environment "$ENV_ID"

  {
    echo "## Teste 1: campo novo COM default (esperado: compatível)"
    confluent schema-registry schema compatibility validate --subject "$CONTRACT_SUBJECT" \
      --version latest --schema "$ROOT_DIR/schemas/payment_event_v2_compativel.avsc" \
      --type avro --environment "$ENV_ID"
    echo; echo "## Teste 2: campo obrigatório novo SEM default (esperado: incompatível)"
    confluent schema-registry schema compatibility validate --subject "$CONTRACT_SUBJECT" \
      --version latest --schema "$ROOT_DIR/schemas/payment_event_v2_incompativel.avsc" \
      --type avro --environment "$ENV_ID" || true
    echo; echo "## Nível de compatibilidade do subject"
    confluent schema-registry subject describe "$CONTRACT_SUBJECT" --environment "$ENV_ID"
  } | tee "$EVID_DIR/camada2-compatibilidade.txt"
}

camada_ingestao() {
  log "Camada 3 - Ingestão (CDC)"
  need_state ENV_ID CLUSTER_ID PRODUCER_API_KEY

  log "Postgres: tabelas, usuário de CDC e publicação"
  psql_admin -q -f "$ROOT_DIR/db/01_schema.sql"
  psql_admin -q -v cdc_user="$PG_CDC_USER" -v cdc_password="$PG_CDC_PASSWORD" -f "$ROOT_DIR/db/02_cdc_user.sql"
  psql_admin -q -f "$ROOT_DIR/db/03_publication.sql"
  [[ "$(psql_admin -tAc 'SHOW wal_level')" == "logical" ]] \
    || die "wal_level não é 'logical': ative a replicação lógica no Neon"
  ok "wal_level=logical, publicação desafio_pub criada"

  log "Seed determinístico (seed=42)"
  python3 "$ROOT_DIR/seed/gerar_dados.py" | tee "$EVID_DIR/camada3-seed.txt"

  log "Conector PostgresCdcSourceV2"
  local rendered="$ROOT_DIR/connectors/cdc.rendered.json"
  render "$ROOT_DIR/connectors/cdc.json" > "$rendered"
  chmod 600 "$rendered"
  if [[ -z "${CONNECTOR_ID:-}" ]]; then
    save_state CONNECTOR_ID "$(confluent connect cluster create --config-file "$rendered" \
      --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r .id)"
  fi
  rm -f "$rendered"
  local status=""
  for _ in $(seq 1 40); do
    status="$(confluent connect cluster describe "$CONNECTOR_ID" --cluster "$CLUSTER_ID" -o json \
      | jq -r '.connector.status // .status')"
    [[ "$status" == "RUNNING" ]] && break
    [[ "$status" == "FAILED" ]] && die "conector FAILED: veja 'confluent connect cluster describe $CONNECTOR_ID'"
    echo "    conector: ${status:-provisionando}..."; sleep 20
  done
  [[ "$status" == "RUNNING" ]] || die "conector não chegou a RUNNING a tempo"
  ok "conector $CONNECTOR_ID RUNNING"
  echo "Próximo passo: psql -f scripts/demo_cdc.sql e depois ./ops/ver_envelopes.sh"
}

camada_processamento() {
  log "Camada 4 - Processamento (Flink SQL)"
  need_state ENV_ID CLUSTER_ID SA_FLINK_ID
  if [[ -z "${FLINK_POOL_ID:-}" ]]; then
    save_state FLINK_POOL_ID "$(confluent flink compute-pool create "$CC_FLINK_POOL_NAME" \
      --cloud "$CC_CLOUD" --region "$CC_REGION" --max-cfu "$CC_FLINK_MAX_CFU" \
      --environment "$ENV_ID" -o json | jq -r .id)"
  fi
  until [[ "$(confluent flink compute-pool describe "$FLINK_POOL_ID" --environment "$ENV_ID" -o json \
      | jq -r .status)" =~ ^(PROVISIONED|RUNNING)$ ]]; do
    echo "    aguardando o compute pool..."; sleep 15
  done
  ok "compute pool $FLINK_POOL_ID"

  local file
  for file in "$ROOT_DIR"/sql/[0-9]*.sql; do
    run_flink "$file"
  done
  confluent flink statement list --environment "$ENV_ID" | tee "$EVID_DIR/camada4-statements.txt"
  echo "Próximo passo: python3 scripts/simular_fraude.py e depois ./ops/consumir_alertas.sh"
}

camada_opcionais() {
  need_state ENV_ID CLUSTER_ID
  if [[ "${ENABLE_TABLEFLOW:-false}" == "true" ]]; then
    log "Opcional - Tableflow (Iceberg, storage gerenciado)"
    local topic
    for topic in desafio.payments.enriched desafio.fraud.detected; do
      confluent tableflow topic enable "$topic" --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
        --storage-type MANAGED --table-formats ICEBERG \
        || warn "não consegui ativar pela CLI; ative em Topics > $topic > Enable Tableflow"
    done
  fi
  if [[ "${ENABLE_AI_SCORE:-false}" == "true" ]]; then
    log "Opcional - score de risco com IA"
    [[ -n "${OPENAI_API_KEY:-}" ]] || die "preencha OPENAI_API_KEY no .env"
    local file rendered
    for file in "$ROOT_DIR"/sql/opcional/ia_*.sql; do
      rendered="${file%.sql}.rendered.sql"
      render "$file" > "$rendered"
      run_flink "$rendered"
      rm -f "$rendered"
    done
  fi
}

main() {
  require_tools confluent jq psql python3
  load_env
  mkdir -p "$EVID_DIR"
  case "${1:-}" in
    fundacao)      camada_fundacao ;;
    contrato)      camada_contrato ;;
    ingestao)      camada_ingestao ;;
    processamento) camada_processamento ;;
    opcionais)     camada_opcionais ;;
    all)           camada_fundacao; camada_contrato; camada_ingestao; camada_processamento; camada_opcionais ;;
    *) sed -n '2,12p' "$0"; exit 1 ;;
  esac
}

main "$@"

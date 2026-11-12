#!/usr/bin/env bash
# Lê um tópico com a identidade de LEITURA (desafio-consumer), em Avro.
# Uso: ./ops/consumir.sh <tópico> [segundos] [arquivo-de-saída]
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_tools confluent timeout
load_env
need_state CLUSTER_ID CONSUMER_API_KEY CONSUMER_SR_API_KEY SR_URL

topic="${1:?informe o tópico}"
seconds="${2:-30}"
out="${3:-/dev/stdout}"

timeout "$seconds" confluent kafka topic consume "$topic" \
  --from-beginning --print-key --key-format avro --value-format avro \
  --group "desafio-evidencias-$(date +%s)" \
  --cluster "$CLUSTER_ID" \
  --api-key "$CONSUMER_API_KEY" --api-secret "$CONSUMER_API_SECRET" \
  --schema-registry-endpoint "$SR_URL" \
  --schema-registry-api-key "$CONSUMER_SR_API_KEY" \
  --schema-registry-api-secret "$CONSUMER_SR_API_SECRET" > "$out" || true

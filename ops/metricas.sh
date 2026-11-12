#!/usr/bin/env bash
# Camada 5 - Observabilidade: bytes recebidos por tópico e lag por grupo,
# pela Metrics API, com a identidade desafio-observer (MetricsViewer).
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_tools curl jq
load_env
need_state CLUSTER_ID OBSERVER_CLOUD_KEY

query() {
  curl -s -u "$OBSERVER_CLOUD_KEY:$OBSERVER_CLOUD_SECRET" \
    -H 'Content-Type: application/json' \
    -X POST https://api.telemetry.confluent.cloud/v2/metrics/cloud/query -d "$1"
}

{
  echo "## received_bytes por tópico (pico por minuto, última 1 h)"
  query '{"aggregations":[{"metric":"io.confluent.kafka.server/received_bytes"}],
          "filter":{"field":"resource.kafka.id","op":"EQ","value":"'"$CLUSTER_ID"'"},
          "granularity":"PT1M","intervals":["now-1h/now"],"group_by":["metric.topic"]}' \
    | jq -r '.data | group_by(."metric.topic")[] | "\(.[0]."metric.topic"): pico \(map(.value)|max) bytes/min"'
  echo
  echo "## consumer lag por grupo (offsets, última 1 h)"
  query '{"aggregations":[{"metric":"io.confluent.kafka.server/consumer_lag_offsets"}],
          "filter":{"field":"resource.kafka.id","op":"EQ","value":"'"$CLUSTER_ID"'"},
          "granularity":"PT1M","intervals":["now-1h/now"],"group_by":["metric.consumer_group_id"]}' \
    | jq -r '.data | group_by(."metric.consumer_group_id")[] | "\(.[0]."metric.consumer_group_id"): lag máximo \(map(.value)|max)"'
} | tee "$EVID_DIR/camada5-metricas.txt"

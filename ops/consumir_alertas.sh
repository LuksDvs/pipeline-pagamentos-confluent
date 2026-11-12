#!/usr/bin/env bash
# Camada 4: salva os alertas de fraude e a quarentena em evidencias/.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
"$(dirname "$0")/consumir.sh" desafio.fraud.detected 45 "$EVID_DIR/02-alertas-fraude.json"
"$(dirname "$0")/consumir.sh" desafio.payments.quarantine 30 "$EVID_DIR/03-quarentena.json"
echo "Alertas por regra:"
grep -o '"rule_name":"[A-Z_]*"' "$EVID_DIR/02-alertas-fraude.json" | sort | uniq -c || true
echo "Eventos em quarentena: $(grep -c 'reason' "$EVID_DIR/03-quarentena.json" || true)"

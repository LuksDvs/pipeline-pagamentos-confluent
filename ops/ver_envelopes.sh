#!/usr/bin/env bash
# Camada 3: salva os envelopes do Debezium (op=c, u, d e o tombstone)
# do tópico de estabelecimentos em evidencias/01-envelopes-cdc.json.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
"$(dirname "$0")/consumir.sh" desafio.public.merchants 40 "$EVID_DIR/01-envelopes-cdc.json"
echo "Envelopes com 'Padaria Demonstração' (insert, update e delete):"
grep -n 'Demonstra' "$EVID_DIR/01-envelopes-cdc.json" || echo "nenhum; rode antes scripts/demo_cdc.sql"

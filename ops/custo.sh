#!/usr/bin/env bash
# Camada 5 - Custo: custo do período em que o pipeline rodou.
# A fatura do Confluent Cloud consolida com algumas horas de atraso:
# rode no dia seguinte ao teste para ver o valor completo.
# Uso: ./ops/custo.sh AAAA-MM-DD AAAA-MM-DD
set -euo pipefail
source "$(dirname "$0")/lib.sh"
start="${1:?data inicial AAAA-MM-DD}"
end="${2:?data final AAAA-MM-DD (exclusiva)}"
confluent billing cost list --start-date "$start" --end-date "$end" | tee "$EVID_DIR/camada5-custo.txt"

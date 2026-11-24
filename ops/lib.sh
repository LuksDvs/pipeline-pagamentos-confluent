#!/usr/bin/env bash
# Funções comuns dos scripts. Carregado por setup.sh, teardown.sh e ops/*.sh.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_FILE="$ROOT_DIR/.state"
# shellcheck disable=SC2034  # usado pelos scripts que carregam este arquivo
EVID_DIR="$ROOT_DIR/evidencias"

log()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m    ok: %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m    aviso: %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m    erro: %s\033[0m\n' "$*" >&2; exit 1; }

require_tools() {
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || die "ferramenta '$tool' não encontrada no PATH"
  done
}

load_env() {
  [[ -f "$ROOT_DIR/.env" ]] || die "crie o .env a partir do .env.example (cp .env.example .env)"
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  # shellcheck disable=SC1090
  [[ -f "$STATE_FILE" ]] && source "$STATE_FILE"
  set +a
}

# Grava/atualiza CHAVE=valor no .state (arquivo local, fora do Git)
save_state() {
  local key="$1" value="$2"
  # Um comando de criação que falhou devolve vazio ou "null": para aqui,
  # em vez de seguir com um ID inválido.
  [[ -n "$value" && "$value" != "null" ]] \
    || die "não consegui obter $key: veja a mensagem de erro logo acima"
  touch "$STATE_FILE"
  chmod 600 "$STATE_FILE"
  grep -v "^${key}=" "$STATE_FILE" > "$STATE_FILE.tmp" || true
  printf '%s=%q\n' "$key" "$value" >> "$STATE_FILE.tmp"
  mv "$STATE_FILE.tmp" "$STATE_FILE"
  export "$key=$value"
}

need_state() {
  for key in "$@"; do
    [[ -n "${!key:-}" ]] || die "variável $key ausente no .state; rode a camada anterior antes"
  done
}

# Substitui ${VAR} de um template pelos valores do ambiente
render() {
  python3 - "$1" <<'PY'
import os, string, sys
text = open(sys.argv[1], encoding="utf-8").read()
print(string.Template(text).substitute(os.environ), end="")
PY
}

psql_admin() {
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE="${PG_SSLMODE:-require}" \
    psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -p "${PG_PORT:-5432}" -U "$PG_ADMIN_USER" -d "$PG_DB" "$@"
}

# Executa um arquivo .sql como um statement do Flink (um statement por arquivo)
run_flink() {
  local file="$1" name
  name="desafio-$(basename "$file" .sql | sed 's/\.rendered$//' | tr '_' '-')"
  log "Flink: $name"
  if confluent flink statement describe "$name" --environment "$ENV_ID" >/dev/null 2>&1; then
    warn "statement $name já existe; pulando"
    return 0
  fi
  confluent flink statement create "$name" \
    --sql "$(cat "$file")" \
    --compute-pool "$FLINK_POOL_ID" \
    --service-account "$SA_FLINK_ID" \
    --database "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --property "sql.current-catalog=$CC_ENV_NAME" \
    --property "sql.tables.scan.idle-timeout=5s" \
    --wait \
    || warn "statement $name falhou: veja 'confluent flink statement describe $name'"
}

# role binding idempotente: se já existir, só avisa
rb() {
  local out
  if ! out="$(confluent iam rbac role-binding create "$@" 2>&1)"; then
    warn "role binding não criado (${out##*$'\n'})"
  fi
}

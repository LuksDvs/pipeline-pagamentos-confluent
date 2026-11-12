#!/usr/bin/env bash
# Abre o psql no Postgres do .env como dono das tabelas.
# Uso: ./ops/psql.sh -f scripts/demo_cdc.sql
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_tools psql
load_env
psql_admin "$@"

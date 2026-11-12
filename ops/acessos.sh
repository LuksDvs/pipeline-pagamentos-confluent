#!/usr/bin/env bash
# Camada 5 - Segurança: identidades, ACLs e role bindings do desafio.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
load_env
need_state ENV_ID CLUSTER_ID
{
  echo "## Service accounts"
  confluent iam service-account list | grep -E 'ID|desafio-' || true
  echo; echo "## ACLs do cluster"
  confluent kafka acl list --cluster "$CLUSTER_ID"
  for sa in SA_FLINK_ID SA_CONSUMER_ID SA_OBSERVER_ID; do
    echo; echo "## Role bindings de ${!sa}"
    confluent iam rbac role-binding list --principal "User:${!sa}" --environment "$ENV_ID" --inclusive || true
  done
} | tee "$EVID_DIR/camada5-acessos.txt"

"""Gera e carrega os dados sintéticos do pipeline (determinístico, seed=42).

Uso:
    python seed/gerar_dados.py            # carrega no Postgres definido no .env
    python seed/gerar_dados.py --dry-run  # só gera e mostra o resumo, sem banco

Além dos dados "normais", o script planta de propósito:
  * rajadas de velocidade: 3 transações no mesmo cartão em menos de 60 s
    (devem virar alerta RULE_VELOCITY no Flink);
  * pagamentos duplicados: mesmo cartão, mesmo estabelecimento e mesmo valor
    em até 5 min (devem virar alerta RULE_DUPLICATE);
  * eventos com defeito: valor nulo/negativo ou estabelecimento ausente
    (devem ir para a quarentena, não para o processamento).

No fim, o script calcula quantos alertas o Flink deve gerar com os dados
carregados. Use esse número para conferir a saída da Camada 4.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

SEED = 42
N_CUSTOMERS, N_ACCOUNTS, N_CARDS, N_MERCHANTS, N_TRANSACTIONS = 200, 240, 320, 60, 2000
N_VELOCITY_BURSTS, N_DUPLICATES, N_DEFECTIVE = 8, 5, 5

# Datas fixas para o resultado ser idêntico em qualquer execução.
ACCOUNTS_VERSION_AT = datetime(2026, 10, 1, 0, 0, 0)
TX_START = datetime(2026, 11, 1, 0, 0, 0)
TX_END = datetime(2026, 11, 7, 23, 59, 0)

FIRST = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Heitor",
         "Isabela", "João", "Larissa", "Marcos", "Natália", "Otávio", "Paula", "Rafael",
         "Sofia", "Tiago", "Vanessa", "William"]
LAST = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Lima", "Carvalho", "Ribeiro",
        "Almeida", "Costa", "Gomes", "Martins", "Araújo", "Rocha", "Barbosa"]
CITIES = ["Belo Horizonte", "Contagem", "Betim", "São Paulo", "Rio de Janeiro",
          "Curitiba", "Salvador", "Recife", "Porto Alegre", "Goiânia"]
MCC = {"5411": "Supermercado", "5812": "Restaurante", "5541": "Posto", "5912": "Farmácia",
       "5651": "Vestuário", "5732": "Eletrônicos", "4121": "Transporte", "5814": "Lanchonete"}
SEGMENTS = ["varejo", "alimentacao", "servicos", "saude"]


def money(rng: random.Random, lo: float, hi: float) -> Decimal:
    return Decimal(f"{rng.uniform(lo, hi):.2f}")


def gerar() -> dict[str, list[dict]]:
    rng = random.Random(SEED)

    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        customers.append({
            "customer_id": i,
            "full_name": name,
            "document_cpf": "".join(str(rng.randint(0, 9)) for _ in range(11)),
            "email": f"cliente{i:03d}@exemplo.com.br",
            "city": rng.choice(CITIES),
            "created_at": datetime(2025, 1, 1) + timedelta(days=rng.randint(0, 500)),
        })

    accounts = []
    for i in range(1, N_ACCOUNTS + 1):
        # 200 contas, uma por cliente, e mais 40 contas extras para clientes sorteados
        customer_id = i if i <= N_CUSTOMERS else rng.randint(1, N_CUSTOMERS)
        accounts.append({
            "account_id": i,
            "customer_id": customer_id,
            "account_type": rng.choice(["corrente", "pagamento", "credito"]),
            "status": "ativa" if rng.random() > 0.05 else "bloqueada",
            "risk_tier": rng.choices(["baixo", "medio", "alto"], weights=[70, 22, 8])[0],
            "credit_limit": money(rng, 500, 20000),
            "updated_at": ACCOUNTS_VERSION_AT,
        })

    cards = []
    for i in range(1, N_CARDS + 1):
        account_id = i if i <= N_ACCOUNTS else rng.randint(1, N_ACCOUNTS)
        cards.append({
            "card_id": i,
            "account_id": account_id,
            "card_last4": f"{rng.randint(0, 9999):04d}",
            "brand": rng.choice(["visa", "mastercard", "elo"]),
            "status": "ativo",
            "issued_at": datetime(2024, 1, 1) + timedelta(days=rng.randint(0, 600)),
        })
    card_account = {c["card_id"]: c["account_id"] for c in cards}

    merchants = []
    for i in range(1, N_MERCHANTS + 1):
        mcc = rng.choice(list(MCC))
        merchants.append({
            "merchant_id": i,
            "legal_name": f"{MCC[mcc]} {rng.choice(LAST)} Ltda",
            "mcc": mcc,
            "city": rng.choice(CITIES),
            "bpo_segment": rng.choice(SEGMENTS),
            "created_at": datetime(2024, 6, 1) + timedelta(days=rng.randint(0, 300)),
        })

    def tx(card_id: int, when: datetime, merchant_id, amount, channel=None) -> dict:
        return {
            "card_id": card_id,
            "account_id": card_account[card_id],
            "merchant_id": merchant_id,
            "amount": amount,
            "currency": "BRL",
            "channel": channel or rng.choice(["pos", "ecommerce", "app"]),
            "status": "aprovada" if rng.random() > 0.03 else "negada",
            "created_at": when.replace(microsecond=(when.microsecond // 1000) * 1000),
        }

    span = int((TX_END - TX_START).total_seconds())
    planted: list[dict] = []

    # Rajadas de velocidade: 3 transações em ~40 s no mesmo cartão
    for _ in range(N_VELOCITY_BURSTS):
        card = rng.randint(1, N_CARDS)
        t0 = TX_START + timedelta(seconds=rng.randint(3600, span - 3600))
        for k in range(3):
            planted.append(tx(card, t0 + timedelta(seconds=k * rng.randint(12, 20)),
                              rng.randint(1, N_MERCHANTS), money(rng, 20, 900), "ecommerce"))

    # Pagamentos duplicados: mesmo cartão, estabelecimento e valor, 2 min depois
    for _ in range(N_DUPLICATES):
        card = rng.randint(1, N_CARDS)
        merchant = rng.randint(1, N_MERCHANTS)
        value = money(rng, 50, 3000)
        t0 = TX_START + timedelta(seconds=rng.randint(3600, span - 3600))
        planted.append(tx(card, t0, merchant, value, "pos"))
        planted.append(tx(card, t0 + timedelta(seconds=120), merchant, value, "pos"))

    # Eventos com defeito (vão para a quarentena)
    defects = [
        tx(rng.randint(1, N_CARDS), TX_START + timedelta(hours=10), rng.randint(1, N_MERCHANTS), None),
        tx(rng.randint(1, N_CARDS), TX_START + timedelta(hours=30), rng.randint(1, N_MERCHANTS), Decimal("-15.00")),
        tx(rng.randint(1, N_CARDS), TX_START + timedelta(hours=50), None, money(rng, 10, 100)),
        tx(rng.randint(1, N_CARDS), TX_START + timedelta(hours=70), rng.randint(1, N_MERCHANTS), Decimal("0.00")),
        tx(rng.randint(1, N_CARDS), TX_START + timedelta(hours=90), None, None),
    ]
    assert len(defects) == N_DEFECTIVE

    background = [
        tx(rng.randint(1, N_CARDS), TX_START + timedelta(seconds=rng.randint(0, span)),
           rng.randint(1, N_MERCHANTS), money(rng, 5, 1500))
        for _ in range(N_TRANSACTIONS - len(planted) - len(defects))
    ]

    # Inserir em ordem de tempo: assim o Flink recebe os eventos em ordem
    # e nenhum é descartado como atrasado pelo watermark.
    transactions = sorted(background + planted + defects, key=lambda r: r["created_at"])
    for i, row in enumerate(transactions, start=1):
        row["transaction_id"] = i

    return {"customers": customers, "accounts": accounts, "cards": cards,
            "merchants": merchants, "transactions": transactions}


def is_valid(row: dict) -> bool:
    return row["amount"] is not None and row["amount"] > 0 and row["merchant_id"] is not None


def alertas_esperados(transactions: list[dict]) -> tuple[int, int]:
    """Replica as duas regras do Flink (AFTER MATCH SKIP PAST LAST ROW)."""
    by_card: dict[int, list[dict]] = {}
    for row in transactions:
        if is_valid(row):
            by_card.setdefault(row["card_id"], []).append(row)

    velocity = duplicate = 0
    for rows in by_card.values():
        rows.sort(key=lambda r: (r["created_at"], r["transaction_id"]))
        i = 0
        while i + 2 < len(rows):
            if rows[i + 2]["created_at"] - rows[i]["created_at"] <= timedelta(seconds=60):
                velocity += 1
                i += 3
            else:
                i += 1
        i = 0
        while i + 1 < len(rows):
            a, b = rows[i], rows[i + 1]
            if (b["merchant_id"] == a["merchant_id"] and b["amount"] == a["amount"]
                    and b["created_at"] - a["created_at"] <= timedelta(minutes=5)):
                duplicate += 1
                i += 2
            else:
                i += 1
    return velocity, duplicate


def carregar(data: dict[str, list[dict]]) -> None:
    import psycopg  # import aqui para o --dry-run funcionar sem a biblioteca

    env = ler_env()
    conninfo = (f"host={env['PG_HOST']} port={env.get('PG_PORT', '5432')} dbname={env['PG_DB']} "
                f"user={env['PG_ADMIN_USER']} password={env['PG_ADMIN_PASSWORD']} "
                f"sslmode={env.get('PG_SSLMODE', 'require')}")
    order = ["customers", "accounts", "cards", "merchants", "transactions"]
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE transactions, cards, accounts, merchants, customers RESTART IDENTITY CASCADE")
        for table in order:
            rows = data[table]
            cols = list(rows[0].keys())
            sql = (f"INSERT INTO {table} ({', '.join(cols)}) OVERRIDING SYSTEM VALUE "
                   f"VALUES ({', '.join(['%s'] * len(cols))})")
            cur.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
            pk = f"{table[:-1]}_id"  # customers -> customer_id, transactions -> transaction_id
            cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', '{pk}'), "
                        f"(SELECT max({pk}) FROM {table}))")
        conn.commit()


def ler_env() -> dict[str, str]:
    env = dict(os.environ)
    path = Path(__file__).resolve().parent.parent / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env.setdefault(key.strip(), value.strip())
    return env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="não conecta no banco")
    args = parser.parse_args()

    data = gerar()
    if not args.dry_run:
        carregar(data)

    valid = sum(is_valid(r) for r in data["transactions"])
    velocity, duplicate = alertas_esperados(data["transactions"])
    print("Tabela        | Linhas")
    print("--------------|-------")
    for table, rows in data.items():
        print(f"{table:<13} | {len(rows)}")
    print()
    print(f"Transações válidas: {valid} | em quarentena: {len(data['transactions']) - valid}")
    print(f"Alertas esperados no Flink: RULE_VELOCITY={velocity} | RULE_DUPLICATE={duplicate}")
    print("Carga concluída." if not args.dry_run else "Dry-run: nada foi gravado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

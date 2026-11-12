"""Camada 4: injeta, ao vivo, os casos que devem gerar alerta.

Uso: python scripts/simular_fraude.py

O que é inserido (com a hora atual):
  1. três compras no mesmo cartão em cerca de 20 s  -> RULE_VELOCITY
  2. a mesma compra duas vezes em 30 s               -> RULE_DUPLICATE
  3. um evento com valor negativo                    -> quarentena
  4. depois de 15 s, uma compra comum em outro cartão, só para avançar o
     watermark e o Flink fechar as janelas e emitir os alertas.
"""
from __future__ import annotations

import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "seed"))
from gerar_dados import ler_env  # noqa: E402

import psycopg  # noqa: E402

CARD_VELOCITY, CARD_DUPLICATE, CARD_FLUSH = 11, 22, 33

INSERT = """
INSERT INTO transactions (card_id, account_id, merchant_id, amount, channel, status, created_at)
SELECT %s, account_id, %s, %s, %s, 'aprovada', clock_timestamp()::timestamp(3)
FROM cards WHERE card_id = %s
RETURNING transaction_id, card_id, amount, created_at
"""


def main() -> int:
    env = ler_env()
    conninfo = (f"host={env['PG_HOST']} port={env.get('PG_PORT', '5432')} dbname={env['PG_DB']} "
                f"user={env['PG_ADMIN_USER']} password={env['PG_ADMIN_PASSWORD']} "
                f"sslmode={env.get('PG_SSLMODE', 'require')}")
    with psycopg.connect(conninfo, autocommit=True) as conn, conn.cursor() as cur:
        def inserir(card, merchant, amount, channel, label):
            cur.execute(INSERT, (card, merchant, amount, channel, card))
            row = cur.fetchone()
            print(f"[{label}] transaction_id={row[0]} card_id={row[1]} amount={row[2]} at={row[3]}")

        for value in ("49.90", "129.00", "899.99"):
            inserir(CARD_VELOCITY, 5, Decimal(value), "ecommerce", "velocidade")
            time.sleep(10)

        inserir(CARD_DUPLICATE, 7, Decimal("250.00"), "pos", "duplicado")
        time.sleep(30)
        inserir(CARD_DUPLICATE, 7, Decimal("250.00"), "pos", "duplicado")

        inserir(CARD_FLUSH, 9, Decimal("-10.00"), "app", "defeito")

        print("Aguardando 15 s para avançar o watermark...")
        time.sleep(15)
        inserir(CARD_FLUSH, 9, Decimal("35.00"), "app", "flush")
    print("Pronto. Consulte o tópico desafio.fraud.detected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

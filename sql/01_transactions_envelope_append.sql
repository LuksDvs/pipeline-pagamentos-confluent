-- O tópico de transações chega com o envelope do Debezium e o Flink o trata
-- como changelog. MATCH_RECOGNIZE e o lado "probe" do temporal join exigem
-- um stream append-only. Como transação é um fato que só é inserido, lemos
-- o envelope bruto (before/after/op) como append e filtramos op=c/r adiante.
ALTER TABLE `desafio.public.transactions` SET (
  'value.format' = 'avro-registry',
  'changelog.mode' = 'append'
);

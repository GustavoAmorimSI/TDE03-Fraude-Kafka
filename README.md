
# TDE03 - Detecção de Fraudes com Kafka

## Objetivo

Desenvolver um sistema de processamento de transações em tempo real
utilizando Apache Kafka, Python e PostgreSQL.

## Tecnologias

- Python
- Apache Kafka
- Docker
- PostgreSQL
- kafka-python
- psycopg2-binary

## Regras de detecção

- ALTO_VALOR: transações de valor igual ou superior a R$ 10.000.
- TEMPO_60s: quatro ou mais transações do mesmo cliente em menos
  de 60 segundos.
- GEO_10m: transações do mesmo cliente em cidades diferentes
  dentro de 10 minutos.

## Execução

1. Iniciar os containers com `docker compose up -d`.
2. Executar o Consumer.
3. Executar o Producer em outro terminal.
4. Consultar as transações no PostgreSQL.

## Banco de dados

Banco: `fraude_db`

Tabela: `transacoes`

Todas as transações processadas devem ser armazenadas,
com indicação de fraude e seus respectivos motivos.

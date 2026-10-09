# TDE03 — Sistema de Detecção de Fraudes com Kafka

## 1. Sobre o projeto

Este projeto acadêmico implementa um sistema de processamento de transações em tempo real utilizando Apache Kafka, Python e PostgreSQL. O objetivo é identificar possíveis fraudes por meio de regras simples de análise de transações.

## 2. Tecnologias utilizadas

- **Python:** desenvolvimento do Producer e do Consumer.
- **Apache Kafka:** transmissão das transações entre os componentes.
- **PostgreSQL:** armazenamento das transações e dos resultados da análise.
- **Docker Compose:** execução dos serviços em contêineres.

## 3. Funcionamento do sistema

O Producer gera transações simuladas e publica uma mensagem no tópico `transacoes` a cada 3 segundos.

O Consumer recebe as mensagens, consulta o histórico do cliente, verifica as regras de fraude e registra os resultados no PostgreSQL. As transações são identificadas por um ID único para evitar inserções duplicadas.

## 4. Regras de detecção de fraude

1. **ALTO_VALOR:** transações com valor igual ou superior a R$ 10.000,00.
2. **TEMPO_60s:** quatro ou mais transações do mesmo cliente em um intervalo inferior a 60 segundos, contando a transação atual.
3. **GEO_10m:** transações do mesmo cliente em cidades diferentes dentro de um intervalo de 10 minutos.

Uma transação pode atender a mais de uma regra ao mesmo tempo.

## 5. Estrutura do projeto

```text
TDE03-Fraude-Kafka/
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
├── database/
│   └── init.sql
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

## 6. Como executar

### Pré-requisitos

- Docker Desktop
- Python instalado
- Git

### Iniciar os serviços

Na pasta do projeto, execute:

docker compose up -d

### Ativar o ambiente virtual

.\.venv\Scripts\Activate.ps1

### Instalar as dependências

python -m pip install -r requirements.txt

### Criar o tópico Kafka, se necessário

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transacoes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Se o tópico já existir, não é necessário criá-lo novamente.

### Executar o Consumer

Em um terminal:

```powershell
python consumer/consumer.py
```

### Executar o Producer

Em outro terminal:

```powershell
python producer/producer.py
```

## 7. Consultar os resultados

Para consultar o total de transações, fraudes e transações normais:

```sql
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE fraude = TRUE) AS fraudes,
    COUNT(*) FILTER (WHERE fraude = FALSE) AS normais
FROM transacoes;
```

Para consultar os motivos de fraude registrados:

```sql
SELECT motivo, COUNT(*) AS quantidade
FROM transacoes
WHERE fraude = TRUE
GROUP BY motivo
ORDER BY quantidade DESC;
```

## 8. Considerações finais

O projeto demonstra o uso de mensageria com Kafka para receber transações, processamento com Python para aplicar regras de detecção e PostgreSQL para persistir os resultados. A solução permite acompanhar as transações em tempo real e consultar posteriormente os registros e os motivos identificados.

As regras são simplificadas para fins acadêmicos e não representam um sistema completo de prevenção a fraudes financeiras.
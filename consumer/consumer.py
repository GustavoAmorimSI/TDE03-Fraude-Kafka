
from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json
import psycopg2
from decimal import Decimal

KAFKA_SERVIDOR = "localhost:9092"
TOPICO = "transacoes"

# Conexão com o PostgreSQL
def conectar_banco():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="fraude_db",
        user="postgres",
        password="postgres"
    )


# Histórico recente de cada cliente
historico = defaultdict(deque)


def detectar_fraudes(transacao):
    cliente = transacao["id_cliente"]
    valor = Decimal(str(transacao["valor"]))
    cidade = transacao["city"]
    momento = datetime.fromisoformat(transacao["data_hora"])

    motivos = []
    eventos = historico[cliente]

    # Remove eventos com mais de 10 minutos
    limite_geo = momento - timedelta(minutes=10)

    while eventos and eventos[0]["data_hora"] < limite_geo:
        eventos.popleft()

    # Regra 1: valor alto
    if valor >= Decimal("10000"):
        motivos.append("ALTO_VALOR")

    # Inclui a transação atual no histórico
    eventos.append({
        "data_hora": momento,
        "city": cidade
    })

    # Regra 2: quatro transações em menos de 60 segundos
    limite_tempo = momento - timedelta(seconds=60)

    transacoes_60s = [
        evento for evento in eventos
        if limite_tempo < evento["data_hora"] <= momento
    ]

    if len(transacoes_60s) >= 4:
        motivos.append("TEMPO_60s")

    # Regra 3: cidades diferentes dentro de 10 minutos
    cidades_10m = {
        evento["city"]
        for evento in eventos
        if limite_geo <= evento["data_hora"] <= momento
    }

    if len(cidades_10m) >= 2:
        motivos.append("GEO_10m")

    return motivos


def salvar_transacao(cursor, transacao, motivos):
    cursor.execute(
        """
        INSERT INTO transacoes (
            id_transacao,
            id_cliente,
            valor,
            country,
            city,
            data_hora,
            fraude,
            motivo
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            transacao["id_transacao"],
            transacao["id_cliente"],
            transacao["valor"],
            transacao["country"],
            transacao["city"],
            datetime.fromisoformat(transacao["data_hora"]),
            bool(motivos),
            ", ".join(motivos) if motivos else None
        )
    )


def main():
    conexao = conectar_banco()
    cursor = conexao.cursor()

    consumer = KafkaConsumer(
        TOPICO,
        bootstrap_servers=KAFKA_SERVIDOR,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="grupo-fraude-v1",
        value_deserializer=lambda mensagem: json.loads(
            mensagem.decode("utf-8")
        )
    )

    print("Consumer iniciado!")
    print("Aguardando transações...\n")

    try:
        for mensagem in consumer:
            transacao = mensagem.value

            try:
                motivos = detectar_fraudes(transacao)

                salvar_transacao(cursor, transacao, motivos)
                conexao.commit()

                print("=" * 55)
                print(f"ID: {transacao['id_transacao']}")
                print(f"Cliente: {transacao['id_cliente']}")
                print(f"Valor: R$ {float(transacao['valor']):.2f}")
                print(f"Cidade: {transacao['city']}")
                print(f"Data/Hora: {transacao['data_hora']}")

                if motivos:
                    print("ALERTA DE FRAUDE!")
                    print(f"Motivos: {', '.join(motivos)}")
                else:
                    print("Status: Transação sem fraude identificada")

                # Só confirma a mensagem no Kafka após salvar no banco
                consumer.commit()

            except Exception as erro:
                conexao.rollback()
                print(f"Erro ao processar transação: {erro}")

    except KeyboardInterrupt:
        print("\nConsumer encerrado.")

    finally:
        consumer.close()
        cursor.close()
        conexao.close()


if __name__ == "__main__":
    main()


from kafka import KafkaConsumer
from datetime import datetime, timedelta
from decimal import Decimal
import json
import psycopg2

KAFKA_SERVIDOR = "localhost:9092"
TOPICO = "transacoes"


def conectar_banco():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="fraude_db",
        user="postgres",
        password="postgres"
    )


def detectar_fraudes(cursor, transacao):
    cliente = transacao["id_cliente"]
    valor = Decimal(str(transacao["valor"]))
    cidade = transacao["city"]
    momento = datetime.fromisoformat(transacao["data_hora"])

    motivos = []

    # Regra 1: transação de R$ 10.000 ou mais
    if valor >= Decimal("10000"):
        motivos.append("ALTO_VALOR")

    # Busca as transações anteriores do cliente nos últimos 10 minutos.
    # O registro atual ainda não foi inserido no banco.
    cursor.execute(
        """
        SELECT data_hora, city
        FROM transacoes
        WHERE id_cliente = %s
          AND data_hora >= %s
          AND data_hora < %s
        ORDER BY data_hora
        """,
        (
            cliente,
            momento - timedelta(minutes=10),
            momento
        )
    )

    anteriores = cursor.fetchall()

    # Regra 2: quatro transações em menos de 60 segundos,
    # contando a transação atual.
    limite_60s = momento - timedelta(seconds=60)

    transacoes_60s = [
        data_hora
        for data_hora, _ in anteriores
        if limite_60s < data_hora < momento
    ]

    if len(transacoes_60s) + 1 >= 4:
        motivos.append("TEMPO_60s")

    # Regra 3: cidades diferentes em até 10 minutos,
    # considerando as transações anteriores e a atual.
    cidades_10m = {cidade}
    cidades_10m.update(
        cidade_anterior
        for _, cidade_anterior in anteriores
    )

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
        ON CONFLICT (id_transacao) DO NOTHING
        RETURNING id
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

    return cursor.fetchone() is not None


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
    print("Detectando fraudes e salvando no PostgreSQL.\n")

    try:
        for mensagem in consumer:
            transacao = mensagem.value

            try:
                motivos = detectar_fraudes(cursor, transacao)

                inserida = salvar_transacao(
                    cursor, transacao, motivos
                )

                # Salva no banco antes de confirmar a mensagem no Kafka.
                conexao.commit()

                if inserida:
                    print("-" * 55)
                    print(f"ID: {transacao['id_transacao']}")
                    print(f"Cliente: {transacao['id_cliente']}")
                    print(f"Valor: R$ {float(transacao['valor']):.2f}")
                    print(f"Cidade: {transacao['city']}")

                    if motivos:
                        print("ALERTA DE FRAUDE!")
                        print(f"Motivos: {', '.join(motivos)}")
                    else:
                        print("Transação sem fraude identificada.")
                else:
                    print(
                        "Transação repetida ignorada: "
                        f"{transacao['id_transacao']}"
                    )

                consumer.commit()

            except (KeyError, ValueError, TypeError) as erro:
                conexao.rollback()
                print(f"Transação inválida: {erro}")

            except psycopg2.Error as erro:
                conexao.rollback()
                print(f"Erro no banco de dados: {erro}")
                # Não confirma a mensagem no Kafka se o banco falhar.
                raise

    except KeyboardInterrupt:
        print("\nConsumer encerrado.")

    finally:
        consumer.close()
        cursor.close()
        conexao.close()


if __name__ == "__main__":
    main()

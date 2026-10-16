
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

    if valor >= Decimal("10000"):
        motivos.append("ALTO_VALOR")

    cursor.execute(
        """
        SELECT data_hora, city
        FROM transacoes
        WHERE id_cliente = %s
          AND data_hora >= %s
          AND data_hora < %s
        """,
        (cliente, momento - timedelta(minutes=10), momento)
    )

    anteriores = cursor.fetchall()

    limite = momento - timedelta(seconds=60)
    recentes = [data for data, _ in anteriores if data > limite]

    if len(recentes) >= 3:
        motivos.append("TEMPO_60s")

    cidades_cliente = {cidade}
    cidades_cliente.update(cidade_antiga for _, cidade_antiga in anteriores)

    if len(cidades_cliente) >= 2:
        motivos.append("GEO_10m")

    return motivos


def salvar_transacao(cursor, transacao, motivos):
    cursor.execute(
        """
        INSERT INTO transacoes
        (id_transacao, id_cliente, valor, country, city,
         data_hora, fraude, motivo)
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

try:
    for mensagem in consumer:
        transacao = mensagem.value

        try:
            motivos = detectar_fraudes(cursor, transacao)
            inserida = salvar_transacao(cursor, transacao, motivos)

            conexao.commit()

            if inserida:
                print("-" * 40)
                print(f"Cliente: {transacao['id_cliente']}")
                print(f"Valor: R$ {float(transacao['valor']):.2f}")
                print(f"Cidade: {transacao['city']}")

                if motivos:
                    print("ALERTA DE FRAUDE:", ", ".join(motivos))
                else:
                    print("Transação normal.")

            else:
                print("Transação repetida ignorada.")

            consumer.commit()

        except (KeyError, ValueError, TypeError) as erro:
            conexao.rollback()
            print(f"Transação inválida: {erro}")

        except psycopg2.Error as erro:
            conexao.rollback()
            print(f"Erro no banco: {erro}")
            raise

except KeyboardInterrupt:
    print("\nConsumer encerrado.")

finally:
    consumer.close()
    cursor.close()
    conexao.close()

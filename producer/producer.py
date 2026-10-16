
from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime

KAFKA_SERVIDOR = "localhost:9092"
TOPICO = "transacoes"

clientes = ["cliente01", "cliente02", "cliente03", "cliente04", "cliente05"]

cidades = [
    "Fortaleza",
    "Juazeiro do Norte",
    "Crato",
    "Barbalha",
    "São Paulo"
]


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVIDOR,
    value_serializer=lambda dados: json.dumps(dados).encode("utf-8")
)


def gerar_transacao():
    return {
        "id_transacao": str(uuid.uuid4()),
        "id_cliente": random.choice(clientes),
        "valor": round(random.uniform(100, 15000), 2),
        "country": "BR",
        "city": random.choice(cidades),
        "data_hora": datetime.now().isoformat(timespec="seconds")
    }


try:
    print("Producer iniciado!")

    while True:
        transacao = gerar_transacao()

        try:
            producer.send(TOPICO, transacao).get(timeout=10)

            print(
                f"Transação enviada | Cliente: {transacao['id_cliente']} | "
                f"Valor: R$ {transacao['valor']:.2f} | "
                f"Cidade: {transacao['city']}"
            )

        except Exception as erro:
            print(f"Erro ao enviar: {erro}")

        time.sleep(3)

except KeyboardInterrupt:
    print("\nProducer encerrado.")

finally:
    producer.close()

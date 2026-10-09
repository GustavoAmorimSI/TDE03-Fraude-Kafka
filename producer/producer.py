
from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime

KAFKA_SERVIDOR = "localhost:9092"
TOPICO = "transacoes"

clientes = [
    "cliente01",
    "cliente02",
    "cliente03",
    "cliente04",
    "cliente05"
]

cidades = [
    "Fortaleza",
    "Juazeiro do Norte",
    "Crato",
    "Barbalha",
    "São Paulo"
]


def criar_producer():
    return KafkaProducer(
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


def main():
    producer = criar_producer()

    print("Producer iniciado!")
    print("Enviando transações a cada 3 segundos.")
    print("Pressione Ctrl+C para encerrar.\n")

    try:
        while True:
            transacao = gerar_transacao()

            try:
                producer.send(TOPICO, transacao).get(timeout=10)
                print(
                    f"Enviada | Cliente: {transacao['id_cliente']} | "
                    f"Valor: R$ {transacao['valor']:.2f} | "
                    f"Cidade: {transacao['city']}"
                )
            except Exception as erro:
                print(f"Erro ao enviar transação: {erro}")

            time.sleep(3)

    except KeyboardInterrupt:
        print("\nProducer encerrado.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()

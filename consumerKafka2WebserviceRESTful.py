from kafka import KafkaConsumer
from const import *
from flask import Flask, jsonify, request
import json
import threading

TOPICO_ENTRADA = TOPICO_ALERTAS

app = Flask(__name__)

# Armazenamento em memória
alertas = []
ultimo_alerta_por_sensor = {}

# Lock para evitar conflito entre a thread do Kafka e as requisições REST
lock = threading.Lock()


def criar_resposta_sem_dados():
    return {
        "sensor_id": "",
        "temperatura": 0.0,
        "variacao": 0.0,
        "unidade": "",
        "classificacao": "sem_dados",
        "alerta": False,
        "mensagem": "Nenhum alerta encontrado",
        "timestamp": ""
    }


def consumir_alertas_kafka():
    consumer = KafkaConsumer(
        TOPICO_ENTRADA,
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="grupo-rest-alertas-temperatura"
    )

    print(f"Consumindo eventos do tópico Kafka: {TOPICO_ENTRADA}")

    for mensagem in consumer:
        evento = mensagem.value

        with lock:
            alertas.append(evento)
            ultimo_alerta_por_sensor[evento["sensor_id"]] = evento

        print(f"Alerta recebido do Kafka: {evento}")


@app.route("/alertas/ultimo", methods=["GET"])
def obter_ultimo_alerta():
    """
    Serviço equivalente ao ObterUltimoAlerta do gRPC.

    Pode ser chamado de duas formas:

    GET /alertas/ultimo
    -> retorna o último alerta geral

    GET /alertas/ultimo?sensor_id=sensor-01
    -> retorna o último alerta de um sensor específico
    """

    sensor_id = request.args.get("sensor_id", "")

    with lock:
        if sensor_id:
            evento = ultimo_alerta_por_sensor.get(sensor_id)
        else:
            evento = alertas[-1] if alertas else None

    if evento is None:
        return jsonify(criar_resposta_sem_dados())

    return jsonify(evento)


@app.route("/alertas", methods=["GET"])
def listar_alertas():
    """
    Serviço equivalente ao ListarAlertas do gRPC.

    Pode ser chamado de duas formas:

    GET /alertas
    -> lista todos os alertas

    GET /alertas?sensor_id=sensor-01
    -> lista apenas os alertas de um sensor específico
    """

    sensor_id = request.args.get("sensor_id", "")

    with lock:
        if sensor_id:
            eventos_filtrados = [
                evento for evento in alertas
                if evento["sensor_id"] == sensor_id
            ]
        else:
            eventos_filtrados = list(alertas)

    return jsonify({
        "alertas": eventos_filtrados
    })


@app.route("/health", methods=["GET"])
def health_check():
    """
    Endpoint simples para verificar se o serviço REST está rodando.
    """

    return jsonify({
        "status": "ok",
        "servico": "temperature-rest-service"
    })


def iniciar_servidor_rest():
    print("Servidor RESTful rodando na porta 5000")
    app.run(host="0.0.0.0", port=5000, threaded=True)


if __name__ == "__main__":
    thread_kafka = threading.Thread(target=consumir_alertas_kafka)
    thread_kafka.daemon = True
    thread_kafka.start()

    iniciar_servidor_rest()

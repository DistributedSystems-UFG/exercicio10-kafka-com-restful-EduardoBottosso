from kafka import KafkaProducer
from const import *
import json
import time
import random
from datetime import datetime

TOPICO = TOPICO_MEDIDAS

producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

temperatura_atual = 25.0

while True:
    agora = datetime.now()

    variacao = random.uniform(-1.5, 1.5)
    temperatura_atual += variacao

    evento = {
        "sensor_id": "sensor-01",
        "medida": {
            "temperatura": round(temperatura_atual, 2),
            "variacao": round(variacao, 2),
            "unidade": "celsius"
        },
        "tempo": {
            "timestamp": agora.strftime("%Y-%m-%d %H:%M:%S"),
            "data": agora.strftime("%Y-%m-%d"),
            "hora": agora.strftime("%H:%M:%S"),
            "hora_do_dia": agora.hour
        }
    }

    producer.send(TOPICO, evento)
    producer.flush()

    print(f"Evento publicado: {evento}")

    time.sleep(2)

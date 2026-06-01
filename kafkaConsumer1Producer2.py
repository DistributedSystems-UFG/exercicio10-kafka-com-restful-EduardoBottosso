from kafka import KafkaConsumer, KafkaProducer
from const import *
import json

TOPICO_ENTRADA = TOPICO_MEDIDAS
TOPICO_SAIDA = TOPICO_ALERTAS

consumer = KafkaConsumer(
    TOPICO_ENTRADA,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="grupo-processamento-temperatura"
)

producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Processador consumindo de {TOPICO_ENTRADA}")
print(f"Publicando eventos processados em {TOPICO_SAIDA}")

for mensagem in consumer:
    evento = mensagem.value

    sensor_id = evento["sensor_id"]
    temperatura = evento["medida"]["temperatura"]
    variacao = evento["medida"]["variacao"]
    unidade = evento["medida"]["unidade"]
    timestamp = evento["tempo"]["timestamp"]

    if temperatura >= 35:
        classificacao = "critica"
        alerta = True
        texto = "Temperatura crítica detectada"
    elif temperatura >= 30:
        classificacao = "alta"
        alerta = True
        texto = "Temperatura alta detectada"
    elif temperatura <= 10:
        classificacao = "baixa"
        alerta = True
        texto = "Temperatura baixa detectada"
    else:
        classificacao = "normal"
        alerta = False
        texto = "Temperatura dentro da faixa normal"

    evento_processado = {
        "sensor_id": sensor_id,
        "temperatura": temperatura,
        "variacao": variacao,
        "unidade": unidade,
        "classificacao": classificacao,
        "alerta": alerta,
        "mensagem": texto,
        "timestamp": timestamp
    }

    producer.send(TOPICO_SAIDA, evento_processado)
    producer.flush()

    print(f"Evento processado publicado: {evento_processado}")

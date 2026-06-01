import requests
from const import *


BASE_URL = f"http://{GRPC_HOST}:{GRPC_PORT}"


def imprimir_alerta(alerta):
    print(f"Sensor: {alerta.get('sensor_id')}")
    print(f"Temperatura: {alerta.get('temperatura')}")
    print(f"Variação: {alerta.get('variacao')}")
    print(f"Unidade: {alerta.get('unidade')}")
    print(f"Classificação: {alerta.get('classificacao')}")
    print(f"Alerta: {alerta.get('alerta')}")
    print(f"Mensagem: {alerta.get('mensagem')}")
    print(f"Timestamp: {alerta.get('timestamp')}")


def obter_ultimo_alerta(sensor_id=""):
    url = f"{BASE_URL}/alertas/ultimo"

    params = {}
    if sensor_id:
        params["sensor_id"] = sensor_id

    resposta = requests.get(url, params=params)

    if resposta.status_code == 200:
        return resposta.json()

    print(f"Erro ao obter último alerta: {resposta.status_code}")
    print(resposta.text)
    return None


def listar_alertas(sensor_id=""):
    url = f"{BASE_URL}/alertas"

    params = {}
    if sensor_id:
        params["sensor_id"] = sensor_id

    resposta = requests.get(url, params=params)

    if resposta.status_code == 200:
        return resposta.json()

    print(f"Erro ao listar alertas: {resposta.status_code}")
    print(resposta.text)
    return None


def main():
    print("Último alerta recebido:")
    resposta_ultimo = obter_ultimo_alerta()

    if resposta_ultimo:
        imprimir_alerta(resposta_ultimo)

    print("\nLista de alertas recebida:")
    resposta_lista = listar_alertas()

    if resposta_lista:
        alertas = resposta_lista.get("alertas", [])

        for alerta in alertas:
            print("-" * 40)
            imprimir_alerta(alerta)


if __name__ == "__main__":
    main()

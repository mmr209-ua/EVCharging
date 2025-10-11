import sys
import json
import time
import datetime
import threading
import socket
from kafka import KafkaProducer, KafkaConsumer
from topics import *

def main():
    # --- ARGUMENTOS ---
    # 1: broker Kafka, 2: ID del CP, 3: puerto donde este engine escuchará al monitor
    if len(sys.argv) < 4:
        print("Uso: python cp_engine.py <broker_ip:puerto> <cp_id> <listen_port>")
        sys.exit(1)

    broker = sys.argv[1]
    cp_id = str(sys.argv[2])
    listen_port = int(sys.argv[3])

    # --- PRODUCTOR KAFKA ---
    # Se usa para enviar mensajes hacia la central (por ejemplo: consumo, estado, ticket...)
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # --- CONSUMIDORES KAFKA ---
    # 1. Recibe comandos de control desde la central (parar o reanudar)
    consumer_control = KafkaConsumer(
        CP_CONTROL,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}"
    )

    # 2. Recibe autorizaciones de suministro (cuando un conductor pide recargar)
    consumer_authorize = KafkaConsumer(
        CP_AUTHORIZE_SUPPLY,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}"
    )

    # Estado interno del CP
    estado = "ACTIVADO"      # Estado inicial
    en_suministro = False    # Bandera de si está suministrando o no

    # ==============================================================
    # SERVIDOR TCP -> para que el monitor pueda comprobar si el CP responde
    # ==============================================================
    def health_server():
        # Creamos un socket TCP
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Escucha en todas las interfaces (0.0.0.0) en el puerto indicado
        s.bind(("0.0.0.0", listen_port))
        s.listen(1)
        print(f"[CP_ENGINE {cp_id}] Health server escuchando en puerto {listen_port}")

        # Bucle infinito: acepta conexiones entrantes del monitor
        while True:
            conn, addr = s.accept()
            data = conn.recv(1024).decode().strip()
            # Si el monitor envía "PING", respondemos "PONG"
            if data == "PING":
                conn.sendall(b"PONG")
            conn.close()

    # El servidor de salud corre en un hilo aparte
    threading.Thread(target=health_server, daemon=True).start()

    # ==============================================================
    # FUNCIÓN PARA SIMULAR UN SUMINISTRO DE ENERGÍA
    # ==============================================================
    def start_supply():
        nonlocal estado, en_suministro
        if estado != "ACTIVADO":
            print(f"[CP_ENGINE {cp_id}] No puede suministrar: estado {estado}")
            return

        estado = "SUMINISTRANDO"
        en_suministro = True
        print(f"[CP_ENGINE {cp_id}] Suministro iniciado")

        consumo_total = 0
        hora_inicio = datetime.datetime.now().isoformat()

        # Simulamos consumo durante 5 segundos
        for _ in range(5):
            if estado != "SUMINISTRANDO":
                # Si cambia el estado, se interrumpe
                print(f"[CP_ENGINE {cp_id}] Suministro interrumpido")
                en_suministro = False
                return
            consumo_total += 1
            # Enviamos actualización de consumo a la central
            producer.send(CP_CONSUMPTION, {"idCP": cp_id, "consumo": consumo_total})
            time.sleep(1)

        # Al terminar el suministro, generamos ticket con los datos
        hora_fin = datetime.datetime.now().isoformat()
        ticket = {
            "energia": consumo_total,
            "precio_total": round(consumo_total * 0.25, 2),  # Precio ficticio
            "hora_inicio": hora_inicio,
            "hora_fin": hora_fin
        }
        # Enviamos ticket final al topic correspondiente
        producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
        print(f"[CP_ENGINE {cp_id}] Ticket enviado: {ticket}")

        # Volvemos a estado reposo
        estado = "ACTIVADO"
        en_suministro = False

    # ==============================================================
    # CONSUMIDOR DE COMANDOS DE CONTROL
    # ==============================================================
    def consume_control_loop():
        nonlocal estado
        for msg in consumer_control:
            event = msg.value
            if str(event.get("idCP")) != cp_id:
                continue  # Ignoramos mensajes destinados a otros CPs
            action = event.get("action")
            if action == "stop":
                estado = "PARADO"
                producer.send(CP_STATUS, {"idCP": cp_id, "estado": "PARADO"})
                print(f"[CP_ENGINE {cp_id}] Parado por orden central")
            elif action == "resume":
                estado = "ACTIVADO"
                producer.send(CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
                print(f"[CP_ENGINE {cp_id}] Reactivado por central")

    # ==============================================================
    # CONSUMIDOR DE AUTORIZACIONES DE SUMINISTRO
    # ==============================================================
    def consume_authorize_loop():
        for msg in consumer_authorize:
            event = msg.value
            if str(event.get("idCP")) != cp_id:
                continue
            if event.get("action") == "authorize":
                print(f"[CP_ENGINE {cp_id}] Autorizado para suministrar")
                start_supply()  # Inicia la simulación de carga

    # Lanzamos ambos consumidores en hilos independientes
    threading.Thread(target=consume_control_loop, daemon=True).start()
    threading.Thread(target=consume_authorize_loop, daemon=True).start()

    # Bucle principal (solo mantiene vivo el proceso)
    print(f"[CP_ENGINE {cp_id}] Esperando comandos de CENTRAL...")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()

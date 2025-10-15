import sys
import json
import time
import datetime
import threading
import socket
from kafka import KafkaProducer, KafkaConsumer
from topics import *

def main():
    if len(sys.argv) < 4:
        print("Uso: python cp_engine.py <broker_ip:puerto> <cp_id> <listen_port>")
        sys.exit(1)

    broker = sys.argv[1]
    cp_id = str(sys.argv[2])
    listen_port = int(sys.argv[3])

    # --- PRODUCTOR KAFKA ---
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # --- CONSUMIDORES KAFKA ---
    consumer_control = KafkaConsumer(
        CP_CONTROL,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}"
    )
    consumer_authorize = KafkaConsumer(
        CP_AUTHORIZE_SUPPLY,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}"
    )

    estado = "ACTIVADO"
    en_suministro = False

    # ==========================================================
    # SERVIDOR TCP para monitor
    # ==========================================================
    def health_server():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", listen_port))
        s.listen(1)
        print(f"[CP_ENGINE {cp_id}] Health server escuchando en puerto {listen_port}")

        while True:
            conn, addr = s.accept()
            try:
                data = conn.recv(1024).decode().strip()
                if data == "PING":
                    conn.sendall(b"PONG")
            except Exception as e:
                print(f"[CP_ENGINE {cp_id}] Error en health_server: {e}")
            finally:
                conn.close()

    threading.Thread(target=health_server, daemon=True).start()

    # ==========================================================
    # FUNCION PARA SIMULAR SUMINISTRO
    # ==========================================================
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

        for _ in range(5):
            if estado != "SUMINISTRANDO":
                print(f"[CP_ENGINE {cp_id}] Suministro interrumpido")
                en_suministro = False
                return
            consumo_total += 1
            producer.send(CP_CONSUMPTION, {"idCP": cp_id, "consumo": consumo_total})
            time.sleep(1)

        hora_fin = datetime.datetime.now().isoformat()
        ticket = {
            "energia": consumo_total,
            "precio_total": round(consumo_total * 0.25, 2),
            "hora_inicio": hora_inicio,
            "hora_fin": hora_fin
        }
        producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
        print(f"[CP_ENGINE {cp_id}] Ticket enviado: {ticket}")

        estado = "ACTIVADO"
        en_suministro = False

    # ==========================================================
    # CONSUMIDOR DE COMANDOS
    # ==========================================================
    def consume_control_loop():
        nonlocal estado
        for msg in consumer_control:
            event = msg.value
            if str(event.get("idCP")) != cp_id:
                continue
            action = event.get("action")
            if action == "stop":
                estado = "PARADO"
                producer.send(CP_STATUS, {"idCP": cp_id, "estado": "PARADO"})
                print(f"[CP_ENGINE {cp_id}] Parado por orden central")
            elif action == "resume":
                estado = "ACTIVADO"
                producer.send(CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
                print(f"[CP_ENGINE {cp_id}] Reactivado por central")

    # ==========================================================
    # CONSUMIDOR DE AUTORIZACIONES
    # ==========================================================
    def consume_authorize_loop():
        for msg in consumer_authorize:
            event = msg.value
            if str(event.get("idCP")) != cp_id:
                continue
            if event.get("action") == "authorize":
                print(f"[CP_ENGINE {cp_id}] Autorizado para suministrar")
                start_supply()

    threading.Thread(target=consume_control_loop, daemon=True).start()
    threading.Thread(target=consume_authorize_loop, daemon=True).start()

    print(f"[CP_ENGINE {cp_id}] Esperando comandos de CENTRAL...")
    while True:
        time.sleep(1)

if _name_ == "_main_":
    main()
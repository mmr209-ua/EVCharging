import sys
import json
import time
import socket
from kafka import KafkaProducer
from topics import *

def main():
    # Argumentos:
    # 1. broker Kafka
    # 2. ID del CP
    # 3. IP del Engine
    # 4. Puerto del Engine
    if len(sys.argv) < 5:
        print("Uso: python cp_monitor.py <broker_ip:puerto> <cp_id> <engine_ip> <engine_port>")
        sys.exit(1)

    broker = sys.argv[1]
    cp_id = str(sys.argv[2])
    engine_ip = sys.argv[3]
    engine_port = int(sys.argv[4])

    # --- Productor Kafka  
    # Envia los mensajes de estado, salud y alertas a la central
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # --- Registro inicial ---
    # Al iniciar, el monitor registra el CP en la central
    register_msg = {"idCP": cp_id, "precio": 0.30, "ubicacion": f"Zona-{cp_id}"}
    producer.send(CP_REGISTER, register_msg)
    producer.flush()
    print(f"[CP_MONITOR {cp_id}] Registrado en CENTRAL")

    fallo_prev = False  # Controla si ya se reportó un fallo (para no repetirlo constantemente)

    # ==============================================================
    # BUCLE PRINCIPAL DE MONITORIZACIÓN
    # ==============================================================
    while True:
        ok = False  # Asumimos que el engine no responde hasta comprobarlo

        try:
            # Intentamos conectar por socket TCP al engine
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)  # Máx 2 segundos de espera
            s.connect((engine_ip, engine_port))
            s.sendall(b"PING")               # Enviamos "PING"
            data = s.recv(1024).decode().strip()  # Esperamos "PONG"
            s.close()
            if data == "PONG":
                ok = True
        except Exception:
            ok = False  # Si hay error, el engine no está respondiendo

        # ==========================================================
        # CASO 1: ENGINE NO RESPONDE
        # ==========================================================
        if not ok:
            if not fallo_prev:
                # Solo notificamos una vez cada vez que entra en fallo
                producer.send(CP_ALERTS, {"idCP": cp_id, "alerta": "ENGINE_NO_RESPONDE"})
                print(f"[CP_MONITOR {cp_id}] ❌ FALLO detectado: ENGINE no responde")
                fallo_prev = True

            # Actualizamos el estado del CP a "AVERIADO"
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "AVERIADO"})

        # ==========================================================
        # CASO 2: ENGINE RESPONDE CORRECTAMENTE
        # ==========================================================
        else:
            if fallo_prev:
                # Si antes estaba en fallo, notificamos recuperación
                producer.send(CP_HEALTH, {"idCP": cp_id, "salud": "RECUPERADO"})
                print(f"[CP_MONITOR {cp_id}] ✅ ENGINE recuperado")
                fallo_prev = False

            # Informamos estado normal a la central
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
            producer.send(CP_HEALTH, {"idCP": cp_id, "salud": "OK"})

        # Forzamos el envío inmediato de los mensajes
        producer.flush()
        # Esperamos 5 segundos antes de volver a comprobar
        time.sleep(5)


if __name__ == "__main__":
    main()

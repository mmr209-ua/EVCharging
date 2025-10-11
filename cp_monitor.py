import sys
import json
import time
from kafka import KafkaProducer
from EV_Topics import *

def main():
    if len(sys.argv) < 3:
        print("Uso: python cp_monitor.py <broker_ip:puerto> <cp_id>")
        sys.exit(1)

    broker = sys.argv[1]
    cp_id = sys.argv[2]

    # Producer -> manda info a la central
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Registro inicial
    register_msg = {"idCP": cp_id, "precio" : 0.30, "ubicacion": "Alicante"} # PREGUNTAR ESTO AL PROFE
    producer.send(CP_REGISTER, register_msg)
    print(f"[CP_MONITOR {cp_id}] Registrado en CENTRAL")

    # Bucle de salud
    while True:
        health_msg = {"cp_id": cp_id, "status": "ok"}
        producer.send(CP_STATUS, health_msg)
        print(f"[CP_MONITOR {cp_id}] Reportando salud OK")
        time.sleep(5)


# Para evitar que se ejecute de nuevo el main cuando se llame desde otros módulos
if __name__ == "__main__":
    main()
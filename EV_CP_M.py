import sys
import json
import time
import socket
from kafka import KafkaProducer
from EV_Topics import *

def main():
    if len(sys.argv) < 5:
        print("Uso: python EV_CP_M.py <ip_engine> <ip_central> <id_cp> <broker_ip:puerto>")
        sys.exit(1)

    ip_engine = sys.argv[1]
    ip_central = sys.argv[2]
    id_cp = sys.argv[3]
    broker = sys.argv[4]

    producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    # Registrar CP en la central
    registro = {"idCP": id_cp, "precio": 0.3, "ubicacion": "Alicante"}
    producer.send(CP_REGISTER, registro)
    print(f"[MONITOR {id_cp}] Registrado en la central.")

    while True:
        try:
            with socket.create_connection(("localhost", 6000 + int(id_cp)), timeout=2) as sock:
                sock.send(b"PING")
                respuesta = sock.recv(1024).decode()
                if respuesta == "KO":
                    print(f"[MONITOR {id_cp}] ¡Fallo detectado! Reportando a central.")
                    producer.send(CP_HEALTH, {"idCP": id_cp, "salud": "KO"})
                else:
                    print(f"[MONITOR {id_cp}] CP saludable.")
        except Exception:
            print(f"[MONITOR {id_cp}] No se pudo contactar con ENGINE. Reportando KO.")
            producer.send(CP_HEALTH, {"idCP": id_cp, "salud": "KO"})

        time.sleep(1)

if __name__ == "__main__":
    main()

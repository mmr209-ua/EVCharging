import sys
import json
import time
import socket
from kafka import KafkaProducer
from EV_Topics import *

def main():
    if len(sys.argv) < 4:
        print("Uso: py EV_CP_M.py <ip_engine:puerto_engine> <ip_central:puerto_central> <cp_id>")
        sys.exit(1)

    engine = sys.argv[1]
    ip_engine, puerto_engine = engine.split(":")
    broker = sys.argv[2]
    id_cp = sys.argv[3]

    producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    # Registrar CP en la central
    registro = {"idCP": id_cp, "precio": 0.3, "ubicacion": "Alicante"}
    producer.send(CP_REGISTER, registro)
    producer.flush()
    print(f"[MONITOR {id_cp}] Registrado en la central.")

    ultimo_estado_reportado = None

    while True:
        try:
            with socket.socket((ip_engine, 6000 + int(id_cp)), timeout=2) as sock:
                sock.send(b"PING")
                respuesta = sock.recv(1024).decode()

                if respuesta == "KO":
                    if ultimo_estado_reportado != "KO":
                        print(f"[MONITOR {id_cp}] ¡Fallo detectado! Reportando a central.")
                        producer.send(CP_HEALTH, {"idCP": id_cp, "salud": "KO"})
                        ultimo_estado_reportado = "KO"
                    else:
                        print(f"[MONITOR {id_cp}] CP sigue en estado KO.")
                else:
                    if ultimo_estado_reportado != "OK":
                        print(f"[MONITOR {id_cp}] CP saludable. Reportando recuperación.")
                        producer.send(CP_HEALTH, {"idCP": id_cp, "salud": "OK"})
                        ultimo_estado_reportado = "OK"
                    else:
                        print(f"[MONITOR {id_cp}] CP sigue saludable.")
        except Exception:
            if ultimo_estado_reportado != "KO":
                print(f"[MONITOR {id_cp}] No se pudo contactar con ENGINE. Reportando KO.")
                producer.send(CP_HEALTH, {"idCP": id_cp, "salud": "KO"})
                ultimo_estado_reportado = "KO"
            else:
                print(f"[MONITOR {id_cp}] ENGINE sigue sin responder.")

        time.sleep(1)


if __name__ == "__main__":
    main()

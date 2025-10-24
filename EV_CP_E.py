import sys
import json
import threading
import time
import msvcrt
from kafka import KafkaConsumer, KafkaProducer
from EV_Topics import *

estado_salud = "OK"
id_cp = None

def escuchar_comandos(consumer, producer):
    global estado_salud
    for msg in consumer:
        event = msg.value
        if msg.topic == CP_AUTHORIZE_SUPPLY and event["idCP"] == id_cp:
            if estado_salud == "OK":
                print(f"[ENGINE {id_cp}] Autorizado. Iniciando suministro...")
                # Simular consumo
                for i in range(5):
                    consumo = {"idCP": id_cp, "kwh": i+1, "importe": (i+1)*0.3, "conductor": "user123"}
                    producer.send(CP_CONSUMPTION, consumo)
                    time.sleep(1)
                # Finalizar
                ticket = {"kwh": 5, "importe": 1.5, "fecha": time.ctime()}
                producer.send(CP_SUPPLY_COMPLETE, {"idCP": id_cp, "ticket": ticket})
                print(f"[ENGINE {id_cp}] Suministro finalizado.")
            else:
                print(f"[ENGINE {id_cp}] No se puede suministrar. Estado KO.")

        elif msg.topic == CP_CONTROL and event["idCP"] in [id_cp, "todos"]:
            accion = event["accion"]
            destino = event["idCP"]
            print(f"[ENGINE {id_cp}] Acción recibida: {accion} (para {destino})")

def responder_salud(server_socket):
    global estado_salud
    while True:
        conn, _ = server_socket.accept()
        data = conn.recv(1024).decode()
        if data == "PING":
            conn.send(estado_salud.encode())
        conn.close()

def simular_fallos():
    global estado_salud
    buffer = ""
    while True:
        if msvcrt.kbhit():
            char = msvcrt.getwch()

            # ENTER → procesar comando
            if char == "\r":
                comando = buffer.strip().lower()
                buffer = ""

                if comando == "ko":
                    estado_salud = "KO"
                    print(f"\n[ENGINE {id_cp}] Estado cambiado a KO")
                elif comando == "ok":
                    estado_salud = "OK"
                    print(f"\n[ENGINE {id_cp}] Estado cambiado a OK")
                elif comando:
                    print(f"\n[ENGINE {id_cp}] Comando no reconocido: {comando}")

                print("> ", end="", flush=True)

            # Retroceso
            elif char == "\b":
                if buffer:
                    buffer = buffer[:-1]
                    print("\b \b", end="", flush=True)

            # Cualquier otro carácter
            else:
                buffer += char
                print(char, end="", flush=True)

        time.sleep(0.1)


def main():
    global id_cp
    if len(sys.argv) < 3:
        print("Uso: py EV_CP_E.py <broker_ip:puerto> <cp_id>")
        sys.exit(1)

    broker = sys.argv[1]
    id_cp = sys.argv[2]

    # Crear producer   
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=5
    ) 

    # Crear consumer
    consumer = KafkaConsumer(CP_AUTHORIZE_SUPPLY, CP_CONTROL,
                             bootstrap_servers=[broker],
                             value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                             group_id=f"engine_{id_cp}",
                             auto_offset_reset='earliest')

    # Socket para responder al monitor
    import socket
    server_socket = socket.socket()
    server_socket.bind(("localhost", 6000 + int(id_cp)))  # Puerto único por CP
    server_socket.listen(1)

    threading.Thread(target=escuchar_comandos, args=(consumer, producer), daemon=True).start()
    threading.Thread(target=responder_salud, args=(server_socket,), daemon=True).start()
    threading.Thread(target=simular_fallos, daemon=True).start()

    print("\n===========================================")
    print(f"       ENGINE CP {id_cp} INICIADO")
    print("============================================")
    print("[ENGINE] Comandos disponibles desde teclado:")
    print("  - Pulsar 'k' → Estado KO (averiado)")
    print("  - Pulsar 'o' → Estado OK (recuperado)")
    print("============================================\n")
    print(f"[ENGINE {id_cp}] Esperando comandos...")

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()

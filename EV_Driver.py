import sys
import json
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
from EV_Topics import *

# ----------------------------------------
# Enviar una solicitud de suministro
# ----------------------------------------
def solicitar_recarga(cp_id, driver_id, producer_central, consumer_status, consumer_complete):
    print(f"[DRIVER {driver_id}] Solicitando carga en CP {cp_id}...")
    request = {"driver_id": driver_id, "cp_id": int(cp_id)}

    # Enviar a CENTRAL
    producer_central.send(SUPPLY_REQUEST_TO_CENTRAL, request)
    producer_central.flush()

    supply_finished = False

    # Esperar actualizaciones
    while not supply_finished:
        # Estado del suministro
        for msg in consumer_status:
            event = msg.value
            if event.get("driver_id") != driver_id or event.get("cp_id") != int(cp_id):
                continue
            print(f"[DRIVER {driver_id}] Estado suministro CP {cp_id}: {event.get('status')}")
            if event.get("status") == "END":
                break

        # Ticket final
        for msg in consumer_complete:
            event = msg.value
            if event.get("driver_id") != driver_id or event.get("cp_id") != int(cp_id):
                continue
            ticket = event.get("ticket")
            print(f"[DRIVER {driver_id}] Suministro finalizado CP {cp_id}, ticket: {ticket}")
            supply_finished = True
            break

    print(f"[DRIVER {driver_id}] Esperando 4 segundos antes de la siguiente solicitud...\n")
    time.sleep(4)


# ----------------------------------------
# Solicitudes automáticas desde fichero
# ----------------------------------------
def procesar_solicitudes_automaticas(driver_id, producer_central, consumer_status, consumer_complete):
    try:
        with open("solicitudes.txt", "r") as f:
            cp_list = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("[DRIVER] No se encontró solicitudes.txt.")
        return

    for cp_id in cp_list:
        solicitar_recarga(cp_id, driver_id, producer_central, consumer_status, consumer_complete)


# ----------------------------------------
# Comandos manuales desde teclado
# ----------------------------------------
def escuchar_comandos(driver_id, producer_central, producer_cp, consumer_status, consumer_complete):
    while True:
        cmd = input(f"[DRIVER {driver_id}] Escribe comando (cp <idCP> / central / salir): ").strip().lower()

        if cmd == "salir":
            print(f"[DRIVER {driver_id}] Cerrando driver...")
            sys.exit(0)

        elif cmd.startswith("cp "):
            # Solicitud directa al CP (sin pasar por CENTRAL)
            parts = cmd.split()
            if len(parts) == 2 and parts[1].isdigit():
                cp_id = int(parts[1])
                print(f"[DRIVER {driver_id}] Enviando solicitud DIRECTA a CP {cp_id}...")
                producer_cp.send(SUPPLY_REQUEST_TO_CP, {"driver_id": driver_id, "cp_id": cp_id})
                producer_cp.flush()
            else:
                print("Uso: cp <idCP>  (idCP debe ser numérico)")

        elif cmd == "central":
            # Solicitud normal (pasa por CENTRAL) — lee lista de CPs desde el archivo
            print(f"[DRIVER {driver_id}] Procesando solicitudes desde archivo solicitudes.txt...")
            procesar_solicitudes_automaticas(driver_id, producer_central, consumer_status, consumer_complete)

        else:
            print("Comando no reconocido. Opciones: cp <idCP> / central / salir")


# ----------------------------------------
# Programa principal
# ----------------------------------------
def main():
    if len(sys.argv) < 3:
        print("Uso: py EV_Driver.py <broker_ip:puerto> <driver_id>")
        sys.exit(1)

    broker = sys.argv[1]
    driver_id = sys.argv[2]

    # Producers
    producer_central = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer_cp = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumers
    consumer_status = KafkaConsumer(
        SUPPLY_STATUS,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"driver_{driver_id}"
    )
    consumer_complete = KafkaConsumer(
        DRIVER_SUPPLY_COMPLETE,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"driver_{driver_id}"
    )

    print(f"[DRIVER {driver_id}] Escuchando en topics: {SUPPLY_STATUS}, {DRIVER_SUPPLY_COMPLETE}")

    # Hilo 1: solicitudes automáticas
    threading.Thread(
        target=procesar_solicitudes_automaticas,
        args=(driver_id, producer_central, consumer_status, consumer_complete),
        daemon=True
    ).start()

    # Hilo 2: comandos manuales
    escuchar_comandos(driver_id, producer_central, producer_cp, consumer_status, consumer_complete)



if __name__ == "__main__":
    main()
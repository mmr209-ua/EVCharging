import sys
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from EV_Topics import *

def main():
    if len(sys.argv) < 3:
        print("Uso: python driver.py <broker_ip:puerto> <driver_id>")
        sys.exit(1)

    broker = sys.argv[1]
    driver_id = sys.argv[2]

    # Producer -> envía solicitudes de recarga a CENTRAL
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumer -> recibe actualizaciones sobre la autorización del suministro
    consumer_status = KafkaConsumer(
        SUPPLY_STATUS,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"driver_{driver_id}"
    )

    # Consumer -> recibe ticket cuando finalice el suministro
    consumer_complete = KafkaConsumer(
        DRIVER_SUPPLY_COMPLETE,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"driver_{driver_id}"
    )

    print(f"[DRIVER {driver_id}] Escuchando en topics: {SUPPLY_STATUS}, {DRIVER_SUPPLY_COMPLETE}")

    # Leer solicitudes desde archivo
    try:
        with open("solicitudes.txt", "r") as f:
            cp_list = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("[DRIVER] No se encontró solicitudes.txt — usando CP=1 por defecto.")
        cp_list = ["1"]

    for cp_id in cp_list:
        print(f"[DRIVER {driver_id}] Solicitando carga en CP {cp_id}...")
        request = {"driver_id": driver_id, "cp_id": int(cp_id)}
        producer.send(CHARGING_REQUESTS, request)
        producer.flush()

        supply_finished = False

        # Esperar actualizaciones
        while not supply_finished:
            # Procesar estados intermedios
            for msg in consumer_status:
                event = msg.value
                if event.get("driver_id") != driver_id or event.get("cp_id") != int(cp_id):
                    continue
                print(f"[DRIVER {driver_id}] Estado suministro CP {cp_id}: {event.get('status')}")
                if event.get("status") == "END":
                    break  # Por si accidentalmente el mensaje final llega aquí

            # Procesar ticket final
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


if __name__ == "__main__":
    main()

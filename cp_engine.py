import datetime
import sys
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from topics import *

def main():
    if len(sys.argv) < 3:
        print("Uso: python cp_engine.py <broker_ip:puerto> <cp_id>")
        sys.exit(1)

    broker = sys.argv[1]
    cp_id = sys.argv[2]

    # Producer -> manda actualizaciones de suministro a CENTRAL
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumers -> recibe órdenes de la CENTRAL
    consumer_control = KafkaConsumer(
        CP_CONTROL,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_{cp_id}"
    )

    consumer_authorize = KafkaConsumer(
        CP_AUTHORIZE_SUPPLY,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_{cp_id}"
    )

    print(f"[CP_ENGINE {cp_id}] Escuchando comandos en topics {CP_CONTROL} y {CP_AUTHORIZE_SUPPLY}...")

    # Estado interno del CP
    estado = "DESCONECTADO"
    autorizado = False

    def start_supply():
        nonlocal estado
        estado = "SUMINISTRANDO"
        print(f"[CP_ENGINE {cp_id}] Iniciando suministro...")
        consumo_total = 0

        # Inicia el suministro
        hora_inicio = datetime.now().isoformat()

        # Simulación de consumo progresivo
        for i in range(5):
            consumo_total += 1  # kWh por segundo
            update = {"idCP": cp_id, "consumo": consumo_total}
            producer.send(CP_CONSUMPTION, update)
            time.sleep(1)

        # Termina el suministro
        hora_fin = datetime.now().isoformat()

        # Generar ticket final
        ticket = {
            "energia": consumo_total,
            "precio_total": consumo_total * 0.25,
            "hora_inicio": hora_inicio,
            "hora_fin": hora_fin
        }
        
        producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
        print(f"[CP_ENGINE {cp_id}] Suministro finalizado, ticket enviado")
        estado = "REPOSO"

    # Hilo para escuchar comandos de control
    def consume_control_loop():
        nonlocal estado
        for msg in consumer_control:
            event = msg.value
            if event.get("cp_id") != cp_id:
                continue
            action = event.get("action")
            print(f"[CP_ENGINE {cp_id}] Comando recibido: {action}")
            if action == "stop":
                estado = "FUERA_DE_SERVICIO"
                print(f"[CP_ENGINE {cp_id}] Parando suministro y CP")
            elif action == "resume":
                estado = "ACTIVADO"
                print(f"[CP_ENGINE {cp_id}] CP reanudado")

    # Hilo para escuchar autorizaciones de suministro
    def consume_authorize_loop():
        nonlocal autorizado
        for msg in consumer_authorize:
            event = msg.value
            if event.get("cp_id") != cp_id:
                continue
            if event.get("action") == "authorize":
                print(f"[CP_ENGINE {cp_id}] Suministro autorizado")
                autorizado = True
                start_supply()

    import threading
    threading.Thread(target=consume_control_loop, daemon=True).start()
    threading.Thread(target=consume_authorize_loop, daemon=True).start()

    # Mantener el proceso vivo
    while True:
        time.sleep(1)


# Para evitar que se ejecute de nuevo el main cuando se llame desde otros módulos
if __name__ == "__main__":
    main()

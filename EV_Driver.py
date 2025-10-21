# EV_Driver
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

    # Producer -> envía solicitudes de recarga
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumer -> recibe actualizaciones de consumo (cada segundo)
    consumer_consumption = KafkaConsumer(
        CP_CONSUMPTION,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"driver_{driver_id}_cons"
    )

    # Consumer -> recibe ticket final del suministro y autorizaciones
    consumer_ticket = KafkaConsumer(
        DRIVER_SUPPLY_COMPLETE,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"driver_{driver_id}_ticket"
    )

    print(f"[DRIVER {driver_id}] Escuchando topics: {CP_CONSUMPTION}, {DRIVER_SUPPLY_COMPLETE}")

    # Leer solicitudes desde archivo (lista de CPs)
    try:
        with open("solicitudes.txt", "r") as f:
            cp_list = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("[DRIVER] No se encontró solicitudes.txt — usando CP=1 por defecto.")
        cp_list = ["1"]

    for cp_id in cp_list:
        print(f"\n[DRIVER {driver_id}] 🚗 Solicitando carga en CP {cp_id}...")
        request = {"idDriver": driver_id, "idCP": cp_id}
        producer.send(CHARGING_REQUESTS, request)
        producer.flush()

        supply_finished = False
        suministro_activo = False
        ultimo_consumo = None
        
        # CORRECCIÓN ERROR 3: Esperar respuesta de autorización
        print(f"[DRIVER {driver_id}] ⏳ Esperando autorización de CENTRAL...")

        while not supply_finished:
            # 1️⃣ CORRECCIÓN ERROR 3: Revisar si hay respuesta de autorización/rechazo
            for msg in consumer_ticket:
                event = msg.value
                ticket_info = event.get("ticket", {})
                
                # Verificar que el mensaje es para este driver y CP
                if str(ticket_info.get("idDriver")) != str(driver_id):
                    continue
                    
                estado_respuesta = ticket_info.get("estado")
                motivo = ticket_info.get("motivo", "")
                
                if estado_respuesta == "AUTORIZADO":
                    print(f"[DRIVER {driver_id}] ✅ AUTORIZADO para cargar en CP {cp_id}")
                    print(f"[DRIVER {driver_id}] 💡 {ticket_info.get('mensaje', 'Puede comenzar el suministro')}")
                    suministro_activo = True
                    break
                elif estado_respuesta == "RECHAZADO":
                    print(f"[DRIVER {driver_id}] ❌ RECHAZADO en CP {cp_id}: {motivo}")
                    supply_finished = True
                    break
                elif ticket_info.get("energia") is not None:  # Ticket final
                    print(f"[DRIVER {driver_id}] ✅ RECARGA COMPLETADA en CP {cp_id}")
                    print(f"[DRIVER {driver_id}] 🧾 TICKET FINAL:")
                    print(f"    Energía: {ticket_info.get('energia', 0)} kWh")
                    print(f"    Importe: {ticket_info.get('precio_total', 0)} €")
                    print(f"    Inicio: {ticket_info.get('hora_inicio', '')}")
                    print(f"    Fin: {ticket_info.get('hora_fin', '')}")
                    if ticket_info.get("motivo"):
                        print(f"    Motivo: {ticket_info.get('motivo')}")
                    supply_finished = True
                    break
            
            if supply_finished:
                break

            # 2️⃣ CORRECCIÓN ERROR 2: Mostrar actualizaciones de consumo durante suministro
            if suministro_activo:
                for msg in consumer_consumption:
                    data = msg.value
                    if str(data.get("idCP")) != str(cp_id):
                        continue
                    
                    consumo_actual = data.get("consumo", 0)
                    importe_actual = data.get("importe", 0)
                    
                    # Mostrar solo si hay cambio en el consumo
                    if consumo_actual != ultimo_consumo:
                        print(f"[DRIVER {driver_id}] 🔋 CP {cp_id} -> {consumo_actual} kWh / {importe_actual} €")
                        ultimo_consumo = consumo_actual
                    
                    break  # salimos para seguir revisando ticket

            time.sleep(0.5)  # Pequeña pausa para no saturar

        if not supply_finished and suministro_activo:
            print(f"[DRIVER {driver_id}] ⚠️ Suministro interrumpido inesperadamente")

        print(f"[DRIVER {driver_id}] Esperando 4 segundos antes de la siguiente solicitud...")
        time.sleep(4)

    print(f"[DRIVER {driver_id}] 🏁 Todas las recargas completadas")

if _name_ == "_main_":
    main()
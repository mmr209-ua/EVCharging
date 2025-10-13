import sys
import threading
import json
from kafka import KafkaConsumer, KafkaProducer
from db import *
from topics import *

def main():
    if len(sys.argv) < 4:
        print("Uso: python central.py <puerto> <broker_ip:puerto> <db_ip>")
        sys.exit(1)

    listen_port = sys.argv[1]
    broker = sys.argv[2]
    db_host = sys.argv[3]

    # Conexión DB
    conn = get_connection(db_host)
    init_db(conn)

    # Producer -> envía mensajes a los otros módulos
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumers -> reciben mensajes de otros módulos
    consumers = {
        CP_REGISTER: KafkaConsumer(CP_REGISTER, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_STATUS: KafkaConsumer(CP_STATUS, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_ALERTS: KafkaConsumer(CP_ALERTS, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_HEALTH: KafkaConsumer(CP_HEALTH, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_CONSUMPTION: KafkaConsumer(CP_CONSUMPTION, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_SUPPLY_COMPLETE: KafkaConsumer(CP_SUPPLY_COMPLETE, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CHARGING_REQUESTS: KafkaConsumer(CHARGING_REQUESTS, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        FILE_REQUESTS: KafkaConsumer(FILE_REQUESTS, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central")
    }

    print("[CENTRAL] Escuchando en 4 topics...")

    # Central está a la escucha en los distintos topics
    def consume_loop(topic, consumer):
        for msg in consumer:
            event = msg.value
            print(f"[CENTRAL] Mensaje en {topic}: {event}")

            # Registrar CP en BD
            if topic == CP_REGISTER:
                id = event["idCP"]
                precio = event["precio"]
                ubicacion = event["ubicacion"]

                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO CP (idCP, estado, precio, ubicacion)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE precio=%s, ubicacion=%s;
                """, (id, "DESCONECTADO", precio, ubicacion, precio, ubicacion))
                conn.commit()

                print(f"[CENTRAL] Registrado CP {id} en BD")

            # Cambiar el estado del CP
            elif topic == CP_STATUS:
                id = event["idCP"]
                estado = event["estado"]
                cursor = conn.cursor()
                cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (estado, id))
                conn.commit()
                print(f"[CENTRAL] Estado actualizado CP {id}: {estado}")

            # 
            elif topic == CP_ALERTS:
                id = event["idCP"]
                alerta = event["alerta"]
                cursor = conn.cursor()
                cursor.execute("UPDATE CP SET alerta=%s WHERE idCP=%s;", (alerta, id))
                conn.commit()
                print(f"[CENTRAL] Alerta registrada CP {id}: {alerta}")

            elif topic == CP_HEALTH:
                id = event["idCP"]
                salud = event["salud"]
                cursor = conn.cursor()
                cursor.execute("UPDATE CP SET salud=%s WHERE idCP=%s;", (salud, id))
                conn.commit()
                print(f"[CENTRAL] Salud CP {id}: {salud}")

            elif topic == CP_CONSUMPTION:
                id = event["idCP"]
                consumo = event["consumo"]
                cursor = conn.cursor()
                cursor.execute("UPDATE CP SET consumo_actual=%s WHERE idCP=%s;", (consumo, id))
                conn.commit()
                print(f"[CENTRAL] Consumo actualizado CP {id}: {consumo}")

            elif topic == CP_SUPPLY_COMPLETE:
                id = event["idCP"]
                ticket = event["ticket"]
                producer.send(DRIVER_SUPPLY_COMPLETE, {"idCP": id, "ticket": ticket})
                print(f"[CENTRAL] Suministro finalizado CP {id}, ticket enviado")

            elif topic == CHARGING_REQUESTS:
                print(f"[CENTRAL] Petición de recarga recibida: {event}")

            elif topic == FILE_REQUESTS:
                print(f"[CENTRAL] Petición desde archivo: {event}")

    # Cada consumer corre en su propio hilo
    for t, c in consumers.items():
        threading.Thread(target=consume_loop, args=(t, c), daemon=True).start()

    # Mantener proceso vivo
    while True:
        pass


# Para evitar que se ejecute de nuevo el main cuando se llame desde otros módulos
if __name__ == "__main__":
    main()
import sys
import threading
import json
from kafka import KafkaConsumer, KafkaProducer
from EV_db import *
from EV_Topics import *

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

    # Al arrancar, comprobar si ya hay CPs conectados y mostrarlos por pantalla
    CPs_conectados(conn)

    # Producer -> envía mensajes a los otros módulos
    # CP_CONTROL, ...
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumers -> reciben mensajes de otros módulos
    consumers = {
        CP_REGISTER: KafkaConsumer(CP_REGISTER, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_HEALTH: KafkaConsumer(CP_HEALTH, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_STATUS: KafkaConsumer(CP_STATUS, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_CONSUMPTION: KafkaConsumer(CP_INFO, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_SUPPLY_COMPLETE: KafkaConsumer(CP_SUPPLY_COMPLETE, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CHARGING_REQUESTS: KafkaConsumer(CHARGING_REQUESTS, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
    }

    # Central está a la escucha en distintos topics
    print("[CENTRAL] Escuchando en varios topics...")

    def consume_loop(topic, consumer):
        for msg in consumer:
            event = msg.value
            print(f"[CENTRAL] Mensaje en {topic}: {event}")

            # Registrar un CP en la base de datos
            if topic == CP_REGISTER:
                registrar_CP(event, conn)

            # Comprobar si ha habido alguna avería en el CP
            elif topic == CP_HEALTH:
                comprobar_salud_CP(event, conn)

            # Cambiar el estado del CP en la base de datos
            elif topic == CP_STATUS:
                cambiar_estado_CP(event, conn)
                
            # Gestionar petición de autorización de recarga de suministro
            elif topic == CHARGING_REQUESTS:
                procesar_peticion_suministro(event, conn, producer)

            # Obtener info del consumo e importes del CP en tiempo real durante el suministro
            elif topic == CP_CONSUMPTION:
                monitorizar_CP(event)

            # Al finalizar el suministro se envía el ticket final al conductor y se cambia el estado del CP
            elif topic == CP_SUPPLY_COMPLETE:
                enviar_ticket(event, conn, producer)

    # Hilo interactivo que permite enviar órdenes manuales a los CPs
    def command_loop(producer, conn):

        while True:
            cmd = input("[CENTRAL] Escribe comando (parar/reanudar/listar/salir): ").strip().lower()

            if cmd == "salir":
                print("[CENTRAL] Cerrando CENTRAL...")
                sys.exit(0)

            elif cmd == "listar":
                cursor = conn.cursor()
                cursor.execute("SELECT idCP, estado FROM CP;")
                cps = cursor.fetchall()
                print("[CENTRAL] Lista de CPs:")
                for idCP, estado in cps:
                    print(f"  - {idCP}: {estado}")

            elif cmd.startswith("parar"):
                parts = cmd.split()
                if len(parts) == 2:
                    target = parts[1]
                    producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": target})
                    print(f"[CENTRAL] Enviada orden PARAR a {target}")
                elif len(parts) == 1:
                    producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": "ALL"})
                    print("[CENTRAL] Enviada orden PARAR a todos los CPs")
                else:
                    print("Uso: parar [<idCP>|todos]")

            elif cmd.startswith("reanudar"):
                parts = cmd.split()
                if len(parts) == 2:
                    target = parts[1]
                    producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": target})
                    print(f"[CENTRAL] Enviada orden REANUDAR a {target}")
                elif len(parts) == 1:
                    producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": "ALL"})
                    print("[CENTRAL] Enviada orden REANUDAR a todos los CPs")
                else:
                    print("Uso: reanudar [<idCP>|todos]")

    # Cada consumer corre en su propio hilo
    for t, c in consumers.items():
        threading.Thread(target=consume_loop, args=(t, c), daemon=True).start()

    # Añadimos un hilo para poder enviar distintas órdenes a los CPs de forma arbitraria
    threading.Thread(target=command_loop, args=(producer, conn), daemon=True).start()

    # Mantener proceso vivo
    while True:
        pass


# Comprobar si hay CPs conectados y mostrarlos por pantalla
def CPs_conectados(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT idCP, estado, precio, ubicacion FROM CP;")
    cps = cursor.fetchall()

    if cps:
        print("[CENTRAL] CPs registrados previamente:")
        for cp in cps:
            idCP, estado, precio, ubicacion = cp
            print(f"  - CP {idCP} en {ubicacion} (precio {precio} €/kWh) -> Estado: {estado}")
    else:
        print("[CENTRAL] No hay CPs registrados todavía.")


# Registrar un CP
def registrar_CP (event, conn):
    id = event["idCP"]
    precio = event["precio"]
    ubicacion = event["ubicacion"]

    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO CP (idCP, estado, precio, ubicacion)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE precio=%s, ubicacion=%s;
    """, (id, "DESCONECTADO", precio, ubicacion))
    conn.commit()

    print(f"[CENTRAL] Registrado CP {id} en BD")
            

# Comprobar si ha habido alguna avería en el CP
def comprobar_salud_CP(event, conn):
    id_cp = event["idCP"]
    salud = event["salud"]
    cursor = conn.cursor()
    
    # CP averiado
    if salud == "KO":
        nuevo_estado = "AVERIADO"
        print(f"[CENTRAL] CP {id_cp} se ha averiado")

    # CP recuperado
    else:
        nuevo_estado = "ACTIVADO"
        print(f"[CENTRAL] CP {id_cp} se ha recuperado")

    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (nuevo_estado, id_cp))
    conn.commit()


# Cambiar el estado del CP
def cambiar_estado_CP (event, conn):
    id = event["idCP"]
    estado = event["estado"]
    cursor = conn.cursor()
    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (estado, id))
    conn.commit()
    print(f"[CENTRAL] Estado actualizado CP {id}: {estado}")


# Gestionar petición de autorización de suministro
def procesar_peticion_suministro (event, conn, producer):
    print(f"[CENTRAL] Petición de recarga recibida: {event}")
    id = event["idCP"]
    cursor = conn.cursor()

    # Comprobar si el CP está disponible
    cursor.execute("SELECT estado FROM CP WHERE idCP=%s;", (id))
    row = cursor.fetchone()

    # Si el CP no se encuentra registrado en la BD...
    if row is None:
        print(f"[CENTRAL] ERROR: El CP {id} no está registrado en la BD.")

    # Si el CP se encuentra registrado en la BD...
    else:
        estado = row[0]

        # En caso afirmativo, solicitar autorización al CP para que proceda al suministro
        if estado == "ACTIVADO":
            producer.send(CP_AUTHORIZE_SUPPLY, {"idCP": id})
            print(f"[CENTRAL] CP {id} disponible. Autorizando suministro...")

        # En caso negativo, informar de que el CP no se encuentra disponible
        else:
            print(f"[CENTRAL] CP {id} no disponible (estado actual: {estado}).")


# Obtener info del consumo e importes del CP en tiempo real durante el suministro
def monitorizar_CP(event):
    id_cp = event["idCP"]
    kwh = event["kwh"]
    importe = event["importe"]
    print(f"[CENTRAL] CP {id_cp} SUMINISTRANDO -> {kwh:.2f} kWh | {importe:.2f} €")


# Al finalizar el suministro se envía el ticket final al conductor y se cambia el estado del CP
def enviar_ticket (event, conn, producer):
    id = event["idCP"]
    ticket = event["ticket"]

    # Se envía ticket
    producer.send(DRIVER_SUPPLY_COMPLETE, {"idCP": id, "ticket": ticket})
    print(f"[CENTRAL] Suministro finalizado CP {id}, ticket enviado")

    # Cambiar el estado del CP para que vuelva a estar disponible para otro suministro
    cursor = conn.cursor()
    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("ACTIVADO", id))


# Para evitar que se ejecute de nuevo el main cuando se llame desde otros módulos
if __name__ == "__main__":
    main()
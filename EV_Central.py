import sys
import os
import threading
import json
from kafka import KafkaConsumer, KafkaProducer
from EV_DB import *
from EV_Topics import *
from colorama import init, Fore, Back, Style
init(autoreset=True) # Para q los colores se reseteeen automaticamente después de cada print

# Evento global para detener todos los hilos
stop_event = threading.Event()

# Evento para señalizar la actualización de la pantalla
actualizar_pantalla = threading.Event()

# Colores para los distintos estados de los CPs
COLORS = {
    "ACTIVADO": Back.GREEN,
    "SUMINISTRANDO": Back.GREEN,
    "PARADO": Back.YELLOW,
    "AVERIADO": Back.RED,
    "DESCONECTADO": Back.WHITE + Fore.BLACK,
}

# Diccionario para almacenar datos de consumo en tiempo real de los CPs
# clave = idCP, valor = kwh, importe, conductor
CP_CONSUMPTION_DATA = {}  

# Limpiar la pantalla
def limpiar_pantalla():
    os.system('cls' if os.name == 'nt' else 'clear')


# Bucle de consumo de mensajes
def consume_loop(topic, producer, consumer, conn):

    try:
        for msg in consumer:
            event = msg.value
            print(f"[CENTRAL] Mensaje en {topic}: {event}")

            # Si se cierra central, sal del bucle
            if stop_event.is_set():
                break

            # Registrar un CP en la base de datos
            elif topic == CP_REGISTER:
                registrar_CP(event, conn)

            # Comprobar si ha habido alguna avería en el CP
            elif topic == CP_HEALTH:
                comprobar_salud_CP(event, conn)

            # Cambiar el estado del CP en la base de datos
            elif topic == CP_STATUS:
                cambiar_estado_CP(event, conn)
                
            # Gestionar petición de autorización de recarga de suministro
            elif topic == SUPPLY_REQUEST_TO_CENTRAL:
                procesar_peticion_suministro(event, conn, producer)

            # Obtener info del consumo e importes del CP en tiempo real durante el suministro
            elif topic == CP_CONSUMPTION:
                monitorizar_CP(event)

            # Al finalizar el suministro se envía el ticket final al conductor y se cambia el estado del CP
            elif topic == CP_SUPPLY_COMPLETE:
                enviar_ticket(event, conn, producer)
    
    # Capturar excepciones cuando se cierren lso consumers
    except Exception as e:
        print(f"[CENTRAL] Hilo de {topic} terminado.")


# Hilo interactivo que permite enviar órdenes manuales a los CPs
def command_loop(producer, conn):

    while True:
        cmd = input("[CENTRAL] Escribe comando (parar <id-CP>/reanudar <id-CP>/listar/salir): ").strip().lower()

        if cmd == "salir":
            print("[CENTRAL] Cerrando CENTRAL...")
            stop_event.set()
            break

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


# Hilo que refresca la pantalla
def mostrar_CPs_loop(conn):
    while not stop_event.is_set():
        actualizar_pantalla.wait()  # Espera a que alguien mande señal para actualizar pantalla
        actualizar_pantalla.clear()
        mostrar_CPs(conn)


# Programa principal
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
    print("[CENTRAL] Conexión a la base de datos establecida.")

    # Al arrancar, comprobar si ya hay CPs conectados y mostrarlos por pantalla
    CPs_conectados(conn)

    # Producer -> envía mensajes a los otros módulos
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumers -> reciben mensajes de otros módulos
    consumers = {
        CP_REGISTER: KafkaConsumer(CP_REGISTER, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_HEALTH: KafkaConsumer(CP_HEALTH, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_STATUS: KafkaConsumer(CP_STATUS, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_CONSUMPTION: KafkaConsumer(CP_CONSUMPTION, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_SUPPLY_COMPLETE: KafkaConsumer(CP_SUPPLY_COMPLETE, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        SUPPLY_REQUEST_TO_CENTRAL: KafkaConsumer(SUPPLY_REQUEST_TO_CENTRAL, bootstrap_servers=broker, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
    }

    # Central está a la escucha en distintos topics
    print("[CENTRAL] Escuchando en varios topics...")

    # Cada consumer corre en su propio hilo
    for topic, consumer in consumers.items():
        threading.Thread(target=consume_loop, args=(topic, producer, consumer, conn), daemon=True).start()

    # Añadimos un hilo para poder enviar distintas órdenes a los CPs de forma arbitraria
    threading.Thread(target=command_loop, args=(producer, conn), daemon=True).start()

    # Hilo para refrescar la pantalla cada cierto tiempo
    threading.Thread(target=mostrar_CPs_loop, args=(conn,), daemon=True).start()

    try:
        # Bloquea el hilo ppal hasta que algo active stop_event (ej: el comando salir)
        stop_event.wait()

    finally:
        # Cierre ordenado
        print("[CENTRAL] Cerrando conexiones...")
        for c in consumers.values():
            c.close()
        producer.close()
        conn.close()
        print("[CENTRAL] Todos los recursos cerrados correctamente.")
        print("[CENTRAL] Apagando CENTRAL. ¡Hasta luego!")


# Comprobar si hay CPs conectados y mostrarlos por pantalla
def CPs_conectados(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT idCP, precio, ubicacion FROM CP;")
    cps = cursor.fetchall()

    if cps:
        print("[CENTRAL] CPs registrados:")
        for cp in cps:
            idCP, precio, ubicacion = cp
            color = COLORS.get("DESCONECTADO", "")
            print(color + f"  - CP {idCP} : en {ubicacion} (precio {precio} €/kWh) -> Estado: DESCONECTADO")
    else:
        print("[CENTRAL] No hay CPs registrados todavía.")


# Se muestra el estado de todos los CPs cada cierto tiempo
def mostrar_CPs(conn):
    limpiar_pantalla()
    cursor = conn.cursor()
    cursor.execute("SELECT idCP, estado, precio, ubicacion FROM CP;")
    cps = cursor.fetchall()

    print("[CENTRAL] Monitorización de CPs:")
    for idCP, estado, precio, ubicacion in cps:
        color = COLORS.get(estado, "")
        print(color + f"  - CP {idCP} : en {ubicacion} (precio {precio} €/kWh) -> Estado: {estado}")

        # Si CP está suministrando, muestra también el consumo actual
        if estado == "SUMINISTRANDO" and idCP in CP_CONSUMPTION_DATA:
            data = CP_CONSUMPTION_DATA[idCP]
            print(
                f"  Consumo: {data['kwh']:.2f} kWh | "
                f"Importe: {data['importe']:.2f} € | "
                f"Conductor: {data['conductor']}"
            )


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
    """, (id, "ACTIVADO", precio, ubicacion))
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
    actualizar_pantalla.set()  # Señal para refrescar pantalla


# Cambiar el estado del CP
def cambiar_estado_CP(event, conn):
    id = event["idCP"]
    estado = event["estado"]
    cursor = conn.cursor()

    # Actualizar estado en BD
    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (estado, id))
    conn.commit()
    print(f"[CENTRAL] Estado actualizado CP {id} a: {estado}")

    # Refrescar pantalla
    actualizar_pantalla.set()


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
    conductor = event["conductor"]

    CP_CONSUMPTION_DATA[id_cp] = {
        "kwh": kwh,
        "importe": importe,
        "conductor": conductor
    }

    actualizar_pantalla.set()


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
    conn.commit()
    print(f"[CENTRAL] Estado actualizado CP {id} a: ACTIVADO")

    # Eliminar datos de consumo del CP y actualizar pantalla
    CP_CONSUMPTION_DATA.pop(id, None)
    actualizar_pantalla.set()


# Para evitar que se ejecute de nuevo el main cuando se llame desde otros módulos
if __name__ == "__main__":
    main()
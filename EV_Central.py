import sys
import os
import threading
import json
import time
import msvcrt
from kafka import KafkaConsumer, KafkaProducer
from EV_DB import *
from EV_Topics import *
from colorama import init, Fore, Back, Style
init(autoreset=True)

# Evento global para detener todos los hilos
stop_event = threading.Event()

# Evento para avisar que se debe refrescar la pantalla
actualizar_pantalla = threading.Event()

# Colores según el estado de los CPs
COLORS = {
    "ACTIVADO": Back.GREEN,
    "SUMINISTRANDO": Back.GREEN,
    "PARADO": Back.YELLOW,
    "AVERIADO": Back.RED,
    "DESCONECTADO": Back.WHITE + Fore.BLACK,
}

# Diccionario para guardar el consumo en tiempo real
CP_CONSUMPTION_DATA = {}


# ---- Función para limpiar pantalla ----
def limpiar_pantalla():
    os.system('cls' if os.name == 'nt' else 'clear')


# ---- Hilo que consume mensajes de Kafka ----
def consume_loop(topic, producer, consumer, db_host):
    try:
        # Cada hilo crea su propia conexión a la base de datos
        conn = get_connection(db_host)

        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            # Procesar cada mensaje recibido
            for tp, msgs in records.items():
                for msg in msgs:
                    event = msg.value
                    print(f"[CENTRAL] Mensaje en {topic}: {event}")

                    # Según el topic, se llama a la función correspondiente
                    if topic == CP_REGISTER:
                        registrar_CP(event, conn)
                    elif topic == CP_HEALTH:
                        comprobar_salud_CP(event, conn)
                    elif topic == CP_STATUS:
                        cambiar_estado_CP(event, conn)
                    elif topic == SUPPLY_REQUEST_TO_CENTRAL:
                        procesar_peticion_suministro(event, conn, producer)
                    elif topic == CP_CONSUMPTION:
                        monitorizar_CP(event)
                    elif topic == CP_SUPPLY_COMPLETE:
                        enviar_ticket(event, conn, producer)

    except Exception as e:
        print(f"[CENTRAL] Excepción en hilo de consumo ({topic}): {e}")

    finally:
        # Cerrar la conexión a la base de datos y el consumer
        try:
            conn.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        print(f"[CENTRAL] Hilo de {topic} terminado.")


# ---- Hilo interactivo que permite escribir comandos ----
def command_loop(producer, conn):
    buffer = ""
    try:
        while not stop_event.is_set():
            # Si hay una tecla pulsada
            if msvcrt.kbhit():
                char = msvcrt.getwch()

                # Si se pulsa ENTER -> procesar comando
                if char == "\r":
                    print()
                    cmd = buffer.strip().lower()
                    buffer = ""

                    if not cmd:
                        print("> ", end="", flush=True)
                        continue

                    # Comando para cerrar la central
                    if cmd == "salir":
                        print("\n[CENTRAL] Cerrando CENTRAL...")
                        stop_event.set()
                        break

                    # Mostrar lista de CPs
                    elif cmd == "listar":
                        cursor = conn.cursor()
                        cursor.execute("SELECT idCP, estado FROM CP;")
                        cps = cursor.fetchall()
                        print("\n[CENTRAL] Lista de CPs:")
                        for idCP, estado in cps:
                            print(f"  - CP {idCP}: {estado}")
                        print()

                    # Enviar orden PARAR
                    elif cmd.startswith("parar"):
                        parts = cmd.split()

                        if len(parts) == 2:
                            target = parts[1]
                            cursor = conn.cursor()

                            if target == "todos":
                                print(target)
                                cursor.execute("UPDATE CP SET estado=%s;", ("PARADO",))
                                print("[CENTRAL] Enviada orden PARAR a todos los CPs")
                            else:
                                cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("PARADO", target))
                                print(f"[CENTRAL] Enviada orden PARAR a CP {target}")

                            conn.commit()
                            producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": target})
                            producer.flush()                            
                            actualizar_pantalla.set()

                    # Enviar orden REANUDAR
                    elif cmd.startswith("reanudar"):
                        parts = cmd.split()

                        if len(parts) == 2:
                            target = parts[1]
                            cursor = conn.cursor()

                            if target == "todos":
                                cursor.execute("UPDATE CP SET estado=%s;", ("ACTIVADO",))
                                print("[CENTRAL] Enviada orden REANUDAR a todos los CPs")
                            else:
                                cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("ACTIVADO", target))
                                print(f"[CENTRAL] Enviada orden REANUDAR a CP {target}")

                            conn.commit()
                            producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": target})
                            producer.flush()                            
                            actualizar_pantalla.set()

                    # Otra orden
                    else:
                        print(f"[CENTRAL] Comando no reconocido: {cmd}")

                    print("> ", end="", flush=True)

                # Retroceso (borrar letra)
                elif char == "\b":
                    if buffer:
                        buffer = buffer[:-1]
                        print("\b \b", end="", flush=True)

                # Cualquier otro carácter
                else:
                    buffer += char
                    print(char, end="", flush=True)

            else:
                time.sleep(0.1)

    except Exception as e:
        print(f"[CENTRAL] Excepción en command_loop: {e}")


# ---- Hilo que actualiza la pantalla cada vez que hay cambios ----
def mostrar_CPs_loop(conn):
    while not stop_event.is_set():
        actualizar_pantalla.wait(timeout=1)
        if stop_event.is_set():
            break
        if actualizar_pantalla.is_set():
            actualizar_pantalla.clear()
            mostrar_CPs(conn)


# ---- Mostrar todos los CPs (con sus estados) ----
def reconectar_CPs(conn):
    limpiar_pantalla()
    cursor = conn.cursor()
    cursor.execute("SELECT idCP, precio, ubicacion FROM CP;")
    cps = cursor.fetchall()

    print("[CENTRAL] Monitorización de CPs:")
    print("--------------------------------")
    for idCP, precio, ubicacion in cps:
        color = COLORS.get("DESCONECTADO", "")
        print(color + f"  - CP {idCP} : en {ubicacion} (precio {precio} €/kWh) -> Estado: DESCONECTADO")

    # Mostrar comandos disponibles
    print("\n[CENTRAL] En cualquier momento puedes ejecutar cualquiera de los siguientes comandos (escríbelo y pulsa ENTER):")
    print("  > parar <id_CP / todos>")
    print("  > reanudar <id_CP / todos>")
    print("  > listar")
    print("  > salir\n")


# ---- Mostrar todos los CPs (con sus estados) ----
def mostrar_CPs(conn):
    limpiar_pantalla()
    cursor = conn.cursor()
    cursor.execute("SELECT idCP, estado, precio, ubicacion FROM CP;")
    cps = cursor.fetchall()

    print("[CENTRAL] Monitorización de CPs:")
    print("--------------------------------")
    for idCP, estado, precio, ubicacion in cps:
        color = COLORS.get(estado, "")
        print(color + f"  - CP {idCP} : en {ubicacion} (precio {precio} €/kWh) -> Estado: {estado}")

        # Mostrar consumo si está suministrando
        if estado == "SUMINISTRANDO" and idCP in CP_CONSUMPTION_DATA:
            data = CP_CONSUMPTION_DATA[idCP]
            print(f"  Consumo: {data['kwh']:.2f} kWh | Importe: {data['importe']:.2f} € | Conductor: {data['conductor']}")

    # Mostrar comandos disponibles
    print("\n[CENTRAL] En cualquier momento puedes ejecutar cualquiera de los siguientes comandos (escríbelo y pulsa ENTER):")
    print("  > parar <id_CP / todos>")
    print("  > reanudar <id_CP / todos>")
    print("  > listar")
    print("  > salir\n")
    print("> ", end="", flush=True)


# ---- Registrar un nuevo CP en la base de datos ----
def registrar_CP(event, conn):
    id = event["idCP"]
    precio = event.get("precio", 0)
    ubicacion = event.get("ubicacion", "")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO CP (idCP, estado, precio, ubicacion)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE precio=%s, ubicacion=%s;
    """, (id, "ACTIVADO", precio, ubicacion, precio, ubicacion))
    conn.commit()
    print(f"[CENTRAL] Registrado CP {id} en BD")
    actualizar_pantalla.set()


# ---- Cambiar el estado de un CP (o de todos) ----
def cambiar_estado_CP(event, conn):
    id_cp = event["idCP"]
    estado = event["estado"]
    cursor = conn.cursor()

    if id_cp == "todos":
        cursor.execute("UPDATE CP SET estado=%s;", (estado,))
        conn.commit()
        print(f"[CENTRAL] Estado de todos los CPs actualizado a: {estado}")
    else:
        cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (estado, id_cp))
        conn.commit()
        print(f"[CENTRAL] Estado actualizado CP {id_cp} a: {estado}")

    actualizar_pantalla.set()


# ---- Comprobar si un CP se ha averiado o recuperado ----
def comprobar_salud_CP(event, conn):
    id_cp = event["idCP"]
    salud = event["salud"]
    cursor = conn.cursor()

    if salud == "KO":
        nuevo_estado = "AVERIADO"
        print(f"[CENTRAL] CP {id_cp} se ha averiado")
    else:
        nuevo_estado = "ACTIVADO"
        print(f"[CENTRAL] CP {id_cp} se ha recuperado")

    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (nuevo_estado, id_cp))
    conn.commit()
    actualizar_pantalla.set()


# ---- Petición de recarga desde un CP ----
def procesar_peticion_suministro(event, conn, producer):
    print(f"[CENTRAL] Petición de recarga recibida: {event}")
    id = event["idCP"]
    cursor = conn.cursor()

    cursor.execute("SELECT estado FROM CP WHERE idCP=%s;", (id,))
    row = cursor.fetchone()

    if row is None:
        print(f"[CENTRAL] ERROR: El CP {id} no está registrado en la BD.")
    else:
        estado = row[0]
        if estado == "ACTIVADO":
            producer.send(CP_AUTHORIZE_SUPPLY, {"idCP": id})
            producer.flush()
            print(f"[CENTRAL] CP {id} disponible. Autorizando suministro...")
        else:
            print(f"[CENTRAL] CP {id} no disponible (estado actual: {estado}).")


# ---- Actualizar datos de consumo en tiempo real ----
def monitorizar_CP(event):
    id_cp = event["idCP"]
    CP_CONSUMPTION_DATA[id_cp] = {
        "kwh": event.get("kwh", 0.0),
        "importe": event.get("importe", 0.0),
        "conductor": event.get("conductor", "desconocido")
    }
    actualizar_pantalla.set()


# ---- Enviar ticket final al conductor ----
def enviar_ticket(event, conn, producer):
    id = event["idCP"]
    ticket = event.get("ticket", {})
    producer.send(DRIVER_SUPPLY_COMPLETE, {"idCP": id, "ticket": ticket})
    producer.flush()
    print(f"[CENTRAL] Suministro finalizado CP {id}, ticket enviado")

    cursor = conn.cursor()
    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("ACTIVADO", id))
    conn.commit()
    CP_CONSUMPTION_DATA.pop(id, None)
    actualizar_pantalla.set()


# ---- Función principal ----
def main():
    if len(sys.argv) < 4:
        print("Uso: py EV_Central.py <puerto> <broker_ip:puerto> <db_ip>")
        sys.exit(1)

    listen_port = sys.argv[1]
    broker = sys.argv[2]
    db_host = sys.argv[3]

    # Conexión inicial a la base de datos (solo para algunos hilos)
    conn = get_connection(db_host)
    init_db(conn)

    # Mensaje inicial
    print("[CENTRAL] ¡Bienvenido!")
    print("[CENTRAL] Conexión a la base de datos establecida.")
    print("[CENTRAL] En cualquier momento puedes ejecutar cualquiera de los siguientes comandos (escríbelo y pulsa ENTER):")
    print("  > parar <id_CP / todos>")
    print("  > reanudar <id_CP / todos>")
    print("  > listar")
    print("  > salir\n")

    reconectar_CPs(conn)

    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumidores de Kafka (uno por topic)
    consumers = {
        CP_REGISTER: KafkaConsumer(CP_REGISTER, bootstrap_servers=[broker],
                                   value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                   group_id="central", enable_auto_commit=True, auto_offset_reset='earliest'),
        CP_HEALTH: KafkaConsumer(CP_HEALTH, bootstrap_servers=[broker],
                                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                 group_id="central", enable_auto_commit=True, auto_offset_reset='earliest'),
        CP_STATUS: KafkaConsumer(CP_STATUS, bootstrap_servers=[broker],
                                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                 group_id="central", enable_auto_commit=True, auto_offset_reset='earliest'),
        CP_CONSUMPTION: KafkaConsumer(CP_CONSUMPTION, bootstrap_servers=[broker],
                                      value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                      group_id="central", enable_auto_commit=True, auto_offset_reset='earliest'),
        CP_SUPPLY_COMPLETE: KafkaConsumer(CP_SUPPLY_COMPLETE, bootstrap_servers=[broker],
                                          value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                          group_id="central", enable_auto_commit=True, auto_offset_reset='earliest'),
        SUPPLY_REQUEST_TO_CENTRAL: KafkaConsumer(SUPPLY_REQUEST_TO_CENTRAL, bootstrap_servers=[broker],
                                                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                                 group_id="central", enable_auto_commit=True, auto_offset_reset='earliest'),
    }

    print("[CENTRAL] Escuchando en varios topics...\n")
    print("> ", end="", flush=True)

    # Lanzar hilos de consumo (cada uno con su conexión a BD)
    threads = []
    for topic, consumer in consumers.items():
        t = threading.Thread(target=consume_loop, args=(topic, producer, consumer, db_host), daemon=False)
        t.start()
        threads.append(t)

    # Hilo para mostrar CPs
    t_mostrar = threading.Thread(target=mostrar_CPs_loop, args=(conn,), daemon=False)
    t_mostrar.start()
    threads.append(t_mostrar)

    # Hilo para leer comandos
    t_cmd = threading.Thread(target=command_loop, args=(producer, conn), daemon=False)
    t_cmd.start()
    threads.append(t_cmd)

    # Esperar hasta que se pida salir
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[CENTRAL] Interrupción manual. Cerrando...")
        stop_event.set()
    finally:
        stop_event.set()
        print("[CENTRAL] Cerrando conexiones y esperando hilos...")

        for t in threads:
            t.join(timeout=3)

        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        try:
            conn.close()
        except Exception:
            pass

        print("[CENTRAL] Todos los recursos cerrados correctamente.")
        print("[CENTRAL] Apagando CENTRAL. ¡Hasta luego!")


# ---- Punto de entrada ----
if __name__ == "__main__":
    main()

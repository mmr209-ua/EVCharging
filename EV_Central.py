import sys
import os
import threading
import json
import time
import msvcrt # Para que haya una lectura de lo que se escriba por terminal pero que no resulte bloqueante y joda toda la terminal
from kafka import KafkaConsumer, KafkaProducer
from EV_DB import *
from EV_Topics import *
from colorama import init, Fore, Back, Style
init(autoreset=True)  # Para q los colores se reseteeen automaticamente después de cada print

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


# Bucle de consumo de mensajes (usa poll para no quedarse bloqueado indefinidamente)
def consume_loop(topic, producer, consumer, conn):
    try:
        while not stop_event.is_set():
            # poll devuelve mensajes pendientes; timeout corto para poder comprobar stop_event
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue
            for tp, msgs in records.items():
                for msg in msgs:
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
                    elif topic == SUPPLY_REQUEST_TO_CENTRAL:
                        procesar_peticion_suministro(event, conn, producer)

                    # Obtener info del consumo e importes del CP en tiempo real durante el suministro
                    elif topic == CP_CONSUMPTION:
                        monitorizar_CP(event)

                    # Al finalizar el suministro se envía el ticket final al conductor y se cambia el estado del CP
                    elif topic == CP_SUPPLY_COMPLETE:
                        enviar_ticket(event, conn, producer)
    except Exception as e:
        # Mostrar la excepción real para debug
        print(f"[CENTRAL] Excepción en hilo de consumo ({topic}): {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        print(f"[CENTRAL] Hilo de {topic} terminado.")


# Hilo interactivo que permite enviar órdenes manuales a los CPs o salir de la central
def command_loop(producer, conn):
    buffer = ""
    try:
        while not stop_event.is_set():
            if msvcrt.kbhit():  # hay tecla pulsada
                char = msvcrt.getwch()  # leer carácter (soporta acentos)
                
                # Si el usuario pulsa Enter → procesar comando
                if char == "\r":
                    cmd = buffer.strip().lower()
                    buffer = ""  # limpiar buffer

                    if not cmd:
                        continue

                    if cmd == "salir":
                        print("\n[CENTRAL] Cerrando CENTRAL...")
                        stop_event.set()
                        break

                    elif cmd == "listar":
                        cursor = conn.cursor()
                        cursor.execute("SELECT idCP, estado FROM CP;")
                        cps = cursor.fetchall()
                        print("\n[CENTRAL] Lista de CPs:")
                        for idCP, estado in cps:
                            print(f"  - {idCP}: {estado}")
                        print()

                    elif cmd.startswith("parar"):
                        parts = cmd.split()
                        if len(parts) == 2:
                            target = parts[1]
                            producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": target})
                            producer.flush()
                            print(f"[CENTRAL] Enviada orden PARAR a {target}")
                        else:
                            producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": "ALL"})
                            producer.flush()
                            print("[CENTRAL] Enviada orden PARAR a todos los CPs")

                    elif cmd.startswith("reanudar"):
                        parts = cmd.split()
                        if len(parts) == 2:
                            target = parts[1]
                            producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": target})
                            producer.flush()
                            print(f"[CENTRAL] Enviada orden REANUDAR a {target}")
                        else:
                            producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": "ALL"})
                            producer.flush()
                            print("[CENTRAL] Enviada orden REANUDAR a todos los CPs")

                    else:
                        print(f"[CENTRAL] Comando no reconocido: {cmd}")

                # Permitir borrar con retroceso
                elif char == "\b":
                    buffer = buffer[:-1]
                else:
                    buffer += char

            else:
                time.sleep(0.1)  # evita uso excesivo de CPU

    except Exception as e:
        print(f"[CENTRAL] Excepción en command_loop: {e}")


# Hilo que refresca la pantalla
def mostrar_CPs_loop(conn):
    while not stop_event.is_set():
        actualizar_pantalla.wait(timeout=1)  # espera, pero con timeout para poder salir si stop_event se activa
        if stop_event.is_set():
            break
        if actualizar_pantalla.is_set():
            actualizar_pantalla.clear()
            mostrar_CPs(conn)


# Comprobar si hay CPs conectados y mostrarlos por pantalla
def CPs_conectados(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT idCP, precio, ubicacion FROM CP;")
    cps = cursor.fetchall()

    if cps:
        print("[CENTRAL] Monitorización de CPs:")
        print("--------------------------------")
        for cp in cps:
            idCP, precio, ubicacion = cp
            color = COLORS.get("DESCONECTADO", "")
            print(color + f"  - CP {idCP} : en {ubicacion} (precio {precio} €/kWh) -> Estado: DESCONECTADO")
            print("\n")
    else:
        print("[CENTRAL] No hay CPs registrados todavía.\n")


# Se muestra el estado de todos los CPs cada cierto tiempo
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

        # Si CP está suministrando, muestra también el consumo actual
        if estado == "SUMINISTRANDO" and idCP in CP_CONSUMPTION_DATA:
            data = CP_CONSUMPTION_DATA[idCP]
            print(
                f"  Consumo: {data['kwh']:.2f} kWh | "
                f"Importe: {data['importe']:.2f} € | "
                f"Conductor: {data['conductor']}"
            )


# Registrar un CP
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

    conn.commit()  # <-- IMPORTANTE: confirmar cambios
    print(f"[CENTRAL] Registrado CP {id} en BD")
    actualizar_pantalla.set()


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
    actualizar_pantalla.set()


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
def procesar_peticion_suministro(event, conn, producer):
    print(f"[CENTRAL] Petición de recarga recibida: {event}")
    id = event["idCP"]
    cursor = conn.cursor()

    # Comprobar si el CP está disponible
    cursor.execute("SELECT estado FROM CP WHERE idCP=%s;", (id,))
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
            producer.flush()
            print(f"[CENTRAL] CP {id} disponible. Autorizando suministro...")

        # En caso negativo, informar de que el CP no se encuentra disponible
        else:
            print(f"[CENTRAL] CP {id} no disponible (estado actual: {estado}).")


# Obtener info del consumo e importes del CP en tiempo real durante el suministro
def monitorizar_CP(event):
    id_cp = event["idCP"]
    kwh = event.get("kwh", 0.0)
    importe = event.get("importe", 0.0)
    conductor = event.get("conductor", "desconocido")

    CP_CONSUMPTION_DATA[id_cp] = {
        "kwh": kwh,
        "importe": importe,
        "conductor": conductor
    }

    actualizar_pantalla.set()


# Al finalizar el suministro se envía el ticket final al conductor y se cambia el estado del CP
def enviar_ticket(event, conn, producer):
    id = event["idCP"]
    ticket = event.get("ticket", {})

    # Se envía ticket
    producer.send(DRIVER_SUPPLY_COMPLETE, {"idCP": id, "ticket": ticket})
    producer.flush()
    print(f"[CENTRAL] Suministro finalizado CP {id}, ticket enviado")

    # Cambiar el estado del CP para que vuelva a estar disponible para otro suministro
    cursor = conn.cursor()
    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("ACTIVADO", id))
    conn.commit()
    print(f"[CENTRAL] Estado actualizado CP {id} a: ACTIVADO")

    # Eliminar datos de consumo del CP y actualizar pantalla
    CP_CONSUMPTION_DATA.pop(id, None)
    actualizar_pantalla.set()


# Programa principal
def main():
    if len(sys.argv) < 4:
        print("Uso: py EV_Central.py <puerto> <broker_ip:puerto> <db_ip>")
        sys.exit(1)

    listen_port = sys.argv[1]
    broker = sys.argv[2]
    db_host = sys.argv[3]

    # Conexión DB
    conn = get_connection(db_host)
    init_db(conn)

    # Mostrar menú
    print("[CENTRAL] ¡Bienvenido!")
    print("[CENTRAL] Conexión a la base de datos establecida.")
    print("[CENTRAL] En cualquier momento puedes ejecutar cualquiera de los siguientes comandos:")
    print("  > parar <id_CP / todos>")
    print("  > reanudar <id_CP / todos>")
    print("  > listar")
    print("  > salir\n")
    
    # Al arrancar, comprobar si ya hay CPs conectados y mostrarlos por pantalla
    # actualizar_pantalla.set()
    CPs_conectados(conn)

    # Producer -> envía mensajes a los otros módulos
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Consumers -> reciben mensajes de otros módulos
    consumers = {
        CP_REGISTER: KafkaConsumer(CP_REGISTER, bootstrap_servers=[broker],
                                   value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                   group_id="central", auto_offset_reset='earliest'),
        CP_HEALTH: KafkaConsumer(CP_HEALTH, bootstrap_servers=[broker],
                                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                 group_id="central", auto_offset_reset='earliest'),
        CP_STATUS: KafkaConsumer(CP_STATUS, bootstrap_servers=[broker],
                                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                 group_id="central", auto_offset_reset='earliest'),
        CP_CONSUMPTION: KafkaConsumer(CP_CONSUMPTION, bootstrap_servers=[broker],
                                      value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                      group_id="central", auto_offset_reset='earliest'),
        CP_SUPPLY_COMPLETE: KafkaConsumer(CP_SUPPLY_COMPLETE, bootstrap_servers=[broker],
                                          value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                          group_id="central", auto_offset_reset='earliest'),
        SUPPLY_REQUEST_TO_CENTRAL: KafkaConsumer(SUPPLY_REQUEST_TO_CENTRAL, bootstrap_servers=[broker],
                                                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                                 group_id="central", auto_offset_reset='earliest'),
    }

    print("[CENTRAL] Escuchando en varios topics...\n")

    # Lanzar cada consumer en su propio hilo (no-daemon para permitir limpieza ordenada)
    threads = []
    for topic, consumer in consumers.items():
        t = threading.Thread(target=consume_loop, args=(topic, producer, consumer, conn), daemon=False)
        t.start()
        threads.append(t)

    # Hilo para refrescar la pantalla
    t_mostrar = threading.Thread(target=mostrar_CPs_loop, args=(conn,), daemon=False)
    t_mostrar.start()
    threads.append(t_mostrar)

    # Hilo de comandos interactivo (opcional, útil para pruebas)
    t_cmd = threading.Thread(target=command_loop, args=(producer, conn), daemon=False)
    t_cmd.start()
    threads.append(t_cmd)

    try:
        # Espera hasta que se active stop_event (por comando 'salir' o CTRL+C)
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[CENTRAL] KeyboardInterrupt recibido. Iniciando cierre...")
        stop_event.set()
    finally:
        # Señalar cierre y esperar a hilos
        stop_event.set()
        print("[CENTRAL] Cerrando conexiones y esperando threads...")

        #Cerrar consumidores y producer (ya se cierran en cada hilo, pero intentamos por si acaso)
        try:
            for c in consumers.values():
                try:
                    c.wakeup()  # intenta interrumpir bloqueos internos
                except Exception:
                    pass
        except Exception:
            pass

        # Esperar threads razonablemente
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


# Para evitar que se ejecute de nuevo el main cuando se llame desde otros módulos
if __name__ == "__main__":
    main()
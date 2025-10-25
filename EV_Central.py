import sys
import os
import threading
import json
import time
import socket
import msvcrt
from kafka import KafkaConsumer, KafkaProducer
from EV_DB import *
from EV_Topics import *
from colorama import init, Fore, Back, Style

init(autoreset=True)

# ==============================================================
# Eventos y estado global
# ==============================================================
stop_event = threading.Event()                 # Para apagar ordenadamente todos los hilos
actualizar_pantalla = threading.Event()        # Señal para refrescar el panel

# Colores según el estado de los CPs
COLORS = {
    "ACTIVADO": Back.GREEN,
    "SUMINISTRANDO": Back.GREEN,
    "PARADO": Back.YELLOW,
    "AVERIADO": Back.RED,
    "DESCONECTADO": Back.WHITE + Fore.BLACK,
}

# Datos de consumo en tiempo real por CP (solo visualización)
CP_CONSUMPTION_DATA = {}

# ==============================================================
# Utilidades
# ==============================================================

def limpiar_pantalla():
    os.system('cls' if os.name == 'nt' else 'clear')


def log_color(prefix, msg, color=Style.RESET_ALL):
    print(color + f"{prefix} {msg}" + Style.RESET_ALL)


# ==============================================================
# HILOS KAFKA (consumidores por topic)
# ==============================================================

def consume_loop(topic, producer, consumer, db_host):
    """Hilo genérico: consume de un topic y despacha a la lógica correspondiente.
    Cada hilo mantiene su propia conexión a BD para evitar contención.
    """
    conn = None
    try:
        conn = get_connection(db_host)
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue
            for tp, msgs in records.items():
                for msg in msgs:
                    event = msg.value
                    print(f"[CENTRAL][Kafka] Mensaje en {topic}: {event}")

                    try:
                        if topic == CP_REGISTER:
                            registrar_CP(event, conn)
                        elif topic == CP_HEALTH:
                            comprobar_salud_CP(event, conn)
                        elif topic == CP_STATUS:
                            cambiar_estado_CP(event, conn)
                        elif topic == CP_CONSUMPTION:
                            monitorizar_CP(event)
                        elif topic == CP_SUPPLY_COMPLETE:
                            enviar_ticket(event, conn, producer)
                        elif topic in (SUPPLY_REQUEST_TO_CENTRAL, CHARGING_REQUESTS):
                            procesar_peticion_suministro(event, conn, producer)
                    except Exception as e:
                        print(f"[CENTRAL] Excepción tramitando mensaje de {topic}: {e}")
    except Exception as e:
        print(f"[CENTRAL] Excepción en hilo de consumo ({topic}): {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass


# ==============================================================
# HILO INTERACTIVO DE COMANDOS (teclado no bloqueante)
# ==============================================================

def command_loop(producer, db_host):
    buffer = ""
    conn = None
    try:
        conn = get_connection(db_host)
        while not stop_event.is_set():
            if msvcrt.kbhit():
                char = msvcrt.getwch()

                if char == "\r":  # ENTER
                    print()
                    cmd = buffer.strip().lower()
                    buffer = ""

                    if not cmd:
                        print("> ", end="", flush=True)
                        continue

                    if cmd == "salir":
                        print("\n[CENTRAL] Cerrando CENTRAL...")
                        stop_event.set()
                        break

                    elif cmd.startswith("parar"):
                        parts = cmd.split()
                        if len(parts) == 2:
                            target = parts[1]
                            try:
                                cursor = conn.cursor()
                                if target == "todos":
                                    cursor.execute("UPDATE CP SET estado=%s;", ("PARADO",))
                                    print("[CENTRAL] Enviada orden PARAR a todos los CPs")
                                else:
                                    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("PARADO", target))
                                    print(f"[CENTRAL] Enviada orden PARAR a CP {target}")
                                conn.commit()
                                producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": target})
                                producer.flush()
                                actualizar_pantalla.set()
                            except Exception as e:
                                print(f"[CENTRAL][DB] Error al ejecutar PARAR: {e}")
                                try:
                                    conn.close()
                                except Exception:
                                    pass
                                time.sleep(1)
                                print("[CENTRAL][DB] Reconectando a la base de datos...")
                                conn = get_connection(db_host)
                                print("[CENTRAL][DB] Reconexión exitosa.")

                    elif cmd.startswith("reanudar"):
                        parts = cmd.split()
                        if len(parts) == 2:
                            target = parts[1]
                            try:
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
                            except Exception as e:
                                print(f"[CENTRAL][DB] Error al ejecutar REANUDAR: {e}")
                                try:
                                    conn.close()
                                except Exception:
                                    pass
                                time.sleep(1)
                                print("[CENTRAL][DB] Reconectando a la base de datos...")
                                conn = get_connection(db_host)
                                print("[CENTRAL][DB] Reconexión exitosa.")

                    else:
                        print(f"[CENTRAL] Comando no reconocido: {cmd}")

                    print("> ", end="", flush=True)

                elif char == "\b":  # BACKSPACE
                    if buffer:
                        buffer = buffer[:-1]
                        print("\b \b", end="", flush=True)
                else:
                    buffer += char
                    print(char, end="", flush=True)
            else:
                time.sleep(0.1)
    except Exception as e:
        print(f"[CENTRAL] Excepción general en command_loop: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ==============================================================
# PANEL DE ESTADO (impresión con color)
# ==============================================================

def reconectar_CPs(conn):
    """Se invoca al arrancar para mostrar todos como DESCONECTADO hasta que reporten."""
    limpiar_pantalla()
    cursor = conn.cursor()
    cursor.execute("SELECT idCP, precio, ubicacion FROM CP;")
    cps = cursor.fetchall()

    print("[CENTRAL] Monitorización de CPs:")
    print("--------------------------------")
    for idCP, precio, ubicacion in cps:
        color = COLORS.get("DESCONECTADO", "")
        print(color + f"  - CP {idCP} : en {ubicacion} (precio {precio} €/kWh) -> Estado: DESCONECTADO")

    print("\n[CENTRAL] Comandos (ENTER para ejecutar):")
    print("  > parar <id_CP / todos>")
    print("  > reanudar <id_CP / todos>")
    print("  > salir\n")


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
        if estado == "SUMINISTRANDO" and idCP in CP_CONSUMPTION_DATA:
            data = CP_CONSUMPTION_DATA[idCP]
            print(f"  Consumo: {data['kwh']:.2f} kWh | Importe: {data['importe']:.2f} € | Conductor: {data['conductor']}")

    print("\n[CENTRAL] Comandos (ENTER para ejecutar):")
    print("  > parar <id_CP / todos>")
    print("  > reanudar <id_CP / todos>")
    print("  > salir\n")
    print("> ", end="", flush=True)


def mostrar_CPs_loop(db_host):
    conn = None
    try:
        conn = get_connection(db_host)
        while not stop_event.is_set():
            actualizar_pantalla.wait(timeout=1)
            if stop_event.is_set():
                break
            if actualizar_pantalla.is_set():
                actualizar_pantalla.clear()
                mostrar_CPs(conn)
    except Exception as e:
        print(f"[CENTRAL][DB] Error en mostrar_CPs_loop: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ==============================================================
# LÓGICA DE NEGOCIO / ACCIONES
# ==============================================================

def registrar_CP(event, conn):
    id_cp = str(event["idCP"]) 
    precio = event.get("precio", 0)
    ubicacion = event.get("ubicacion", "")
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO CP (idCP, estado, precio, ubicacion)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE precio=%s, ubicacion=%s;
        """,
        (id_cp, "ACTIVADO", precio, ubicacion, precio, ubicacion),
    )
    conn.commit()
    print(f"[CENTRAL] Registrado CP {id_cp} en BD")
    actualizar_pantalla.set()


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


def procesar_peticion_suministro(event, conn, producer):
    """Central valida disponibilidad del CP y, si procede, autoriza.
    También notifica al Driver el resultado de la autorización.
    """
    print(f"[CENTRAL] Petición de recarga recibida: {event}")
    id_cp = str(event.get("idCP"))
    id_driver = str(event.get("idDriver")) if event.get("idDriver") is not None else None

    if not id_cp:
        print("[CENTRAL] ERROR: Petición sin idCP.")
        return

    cursor = conn.cursor()
    cursor.execute("SELECT estado FROM CP WHERE idCP=%s;", (id_cp,))
    row = cursor.fetchone()

    if row is None:
        print(f"[CENTRAL] ERROR: El CP {id_cp} no está registrado en la BD.")
        return

    estado = row[0]
    if estado == "ACTIVADO":
        # 1) Autoriza al CP
        try:
            payload_cp = {"idCP": id_cp, "action": "authorize"}
            if id_driver is not None:
                payload_cp["idDriver"] = id_driver
            producer.send(CP_AUTHORIZE_SUPPLY, payload_cp)
            producer.flush()
            print(f"[CENTRAL] CP {id_cp} disponible. Autorización enviada al CP.")
        except Exception as e:
            print(f"[CENTRAL] Error enviando autorización a CP: {e}")

        # 2) Notifica al Driver la autorización (canal DRIVER_SUPPLY_COMPLETE)
        if id_driver is not None:
            try:
                producer.send(
                    DRIVER_SUPPLY_COMPLETE,
                    {
                        "idCP": id_cp,
                        "ticket": {
                            "idDriver": id_driver,
                            "estado": "AUTORIZADO",
                            "mensaje": "Autorizado por CENTRAL. Puede comenzar el suministro cuando el CP inicie."
                        },
                    },
                )
                producer.flush()
                print(f"[CENTRAL] Notificada autorización al DRIVER {id_driver}.")
            except Exception as e:
                print(f"[CENTRAL] Error notificando al driver: {e}")
    else:
        print(f"[CENTRAL] CP {id_cp} no disponible (estado actual: {estado}).")
        # Si conocemos al driver, notificamos rechazo inmediato
        if id_driver is not None:
            try:
                producer.send(
                    DRIVER_SUPPLY_COMPLETE,
                    {
                        "idCP": id_cp,
                        "ticket": {
                            "idDriver": id_driver,
                            "estado": "RECHAZADO",
                            "motivo": f"CP_NO_DISPONIBLE_{estado}"
                        },
                    },
                )
                producer.flush()
            except Exception as e:
                print(f"[CENTRAL] Error notificando rechazo al driver: {e}")


def monitorizar_CP(event):
    id_cp = str(event["idCP"])
    CP_CONSUMPTION_DATA[id_cp] = {
        "kwh": float(event.get("consumo", 0.0)),
        "importe": float(event.get("importe", 0.0)),
        "conductor": event.get("conductor", "desconocido"),
    }
    actualizar_pantalla.set()


def enviar_ticket(event, conn, producer):
    id_cp = str(event["idCP"])
    ticket = event.get("ticket", {})
    try:
        producer.send(DRIVER_SUPPLY_COMPLETE, {"idCP": id_cp, "ticket": ticket})
        producer.flush()
        print(f"[CENTRAL] Suministro finalizado CP {id_cp}, ticket enviado al Driver")
    except Exception as e:
        print(f"[CENTRAL] Error reenviando ticket al Driver: {e}")

    cursor = conn.cursor()
    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("ACTIVADO", id_cp))
    conn.commit()
    CP_CONSUMPTION_DATA.pop(id_cp, None)
    actualizar_pantalla.set()


# ==============================================================
# SERVIDOR TCP PARA MONITORES (EV_CP_M)
# ==============================================================

def handle_tcp_client(conn_sock, addr, db_host, producer):
    """Atiende a un monitor EV_CP_M. Lee líneas JSON separadas por '\n'.
    Cada línea se procesa individualmente para tolerar múltiples mensajes por conexión.
    """
    db_conn = None
    try:
        db_conn = get_connection(db_host)
        conn_file = conn_sock.makefile('r')  # stream de texto para leer por líneas
        while not stop_event.is_set():
            line = conn_file.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                print(f"[CENTRAL][TCP] JSON inválido desde {addr}: {line}")
                continue

            msg_type = msg.get("type")
            try:
                if msg_type == "register":
                    registrar_CP(msg, db_conn)
                elif msg_type == "status":
                    # Espera {"type":"status","idCP":"X","estado":"ACTIVADO"|...}
                    cambiar_estado_CP({"idCP": msg.get("idCP"), "estado": msg.get("estado")}, db_conn)
                elif msg_type == "health":
                    # Espera {"type":"health","idCP":"X","salud":"OK"|"KO"|"RECUPERADO"}
                    salud = msg.get("salud")
                    if salud == "RECUPERADO":
                        salud = "OK"
                    comprobar_salud_CP({"idCP": msg.get("idCP"), "salud": salud}, db_conn)
                elif msg_type == "alert":
                    print(f"[CENTRAL][TCP] ALERTA de {msg.get('idCP')}: {msg.get('alerta')}")
                else:
                    print(f"[CENTRAL][TCP] Tipo desconocido desde {addr}: {msg_type}")
            except Exception as e:
                print(f"[CENTRAL][TCP] Error tramitando mensaje de {addr}: {e}")
    except Exception as e:
        print(f"[CENTRAL][TCP] Error en cliente {addr}: {e}")
    finally:
        try:
            conn_sock.close()
        except Exception:
            pass
        try:
            if db_conn:
                db_conn.close()
        except Exception:
            pass


def tcp_server(listen_port, db_host, producer):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", int(listen_port)))
    s.listen(8)
    print(f"[CENTRAL] Servidor TCP escuchando en puerto {listen_port}")

    try:
        while not stop_event.is_set():
            try:
                s.settimeout(1.0)
                conn_sock, addr = s.accept()
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[CENTRAL][TCP] Error aceptando conexión: {e}")
                continue

            threading.Thread(
                target=handle_tcp_client,
                args=(conn_sock, addr, db_host, producer),
                daemon=True,
            ).start()
    finally:
        try:
            s.close()
        except Exception:
            pass


# ==============================================================
# MAIN
# ==============================================================

def main():
    if len(sys.argv) < 4:
        print("Uso: py EV_Central.py <puerto> <broker_ip:puerto> <db_ip>")
        sys.exit(1)

    listen_port = sys.argv[1]
    broker = sys.argv[2]
    db_host = sys.argv[3]

    # Conexión inicial a BD e inicialización de esquema
    conn = get_connection(db_host)
    init_db(conn)

    print("[CENTRAL] Bienvenido")
    print("[CENTRAL] Conexión a BD establecida.")
    print("[CENTRAL] Comandos disponibles:")
    print("  > parar <id_CP / todos>")
    print("  > reanudar <id_CP / todos>")
    print("  > salir\n")

    # Mostrar todos como DESCONECTADOS hasta que reporten
    reconectar_CPs(conn)

    # Producer Kafka
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Consumidores Kafka
    consumers = {
        CP_REGISTER: KafkaConsumer(
            CP_REGISTER,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_register",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        CP_HEALTH: KafkaConsumer(
            CP_HEALTH,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_health",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        CP_STATUS: KafkaConsumer(
            CP_STATUS,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_status",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        CP_CONSUMPTION: KafkaConsumer(
            CP_CONSUMPTION,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_consumption",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        CP_SUPPLY_COMPLETE: KafkaConsumer(
            CP_SUPPLY_COMPLETE,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_supply_complete",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        # Importante: EV_Driver y EV_CP_E envían CHARGING_REQUESTS
        CHARGING_REQUESTS: KafkaConsumer(
            CHARGING_REQUESTS,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_requests",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        # Mantengo el topic SUPPLY_REQUEST_TO_CENTRAL por compatibilidad
        SUPPLY_REQUEST_TO_CENTRAL: KafkaConsumer(
            SUPPLY_REQUEST_TO_CENTRAL,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_request_legacy",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
    }

    print("[CENTRAL] Escuchando topics Kafka y conexiones TCP...\n")
    print("> ", end="", flush=True)

    # Hilo servidor TCP (monitores)
    t_tcp = threading.Thread(target=tcp_server, args=(listen_port, db_host, producer), daemon=True)
    t_tcp.start()

    # Hilos consumidores Kafka
    threads = []
    for topic, consumer in consumers.items():
        t = threading.Thread(target=consume_loop, args=(topic, producer, consumer, db_host), daemon=True)
        t.start()
        threads.append(t)

    # Hilo pantalla
    t_mostrar = threading.Thread(target=mostrar_CPs_loop, args=(db_host,), daemon=True)
    t_mostrar.start()

    # Hilo comandos
    t_cmd = threading.Thread(target=command_loop, args=(producer, db_host), daemon=True)
    t_cmd.start()

    # Bucle de vida del proceso
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
            t.join(timeout=2)

        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        try:
            conn.close()
        except Exception:
            pass

        print("[CENTRAL] Recursos cerrados. Hasta luego.")


if __name__ == "__main__":
    main()

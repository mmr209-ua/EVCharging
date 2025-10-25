# EV_Central.py
# Central de control. Sockets para monitores + Kafka para Drivers/CP_Engine.
import argparse, json, socket, socketserver, threading, time, sys
from collections import defaultdict
from typing import Dict, Tuple
from colorama import init as colorama_init, Fore, Back, Style
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from EV_Topics import *
from EV_DB import get_connection, init_db
import msvcrt
stop_event = threading.Event()
actualizar_pantalla = threading.Event()

# ======== Consola a color =========
colorama_init(autoreset=True)

def c_state_bg(state: str) -> str:
    return {
        ST_ACTIVADO: Back.GREEN,
        ST_PARADO: Back.YELLOW,
        ST_SUMINISTRANDO: Back.GREEN,
        ST_AVERIADO: Back.RED,
        ST_DESC: Back.WHITE + Fore.BLACK,
    }.get(state, Back.RESET)

# ======== Gestión de monitores por socket TCP =========
# Protocolo sencillo: JSON por línea. Mensajes del monitor:
# {"type":"HELLO","cp_id":"CP01","price":0.45,"location":"Alicante"}
# {"type":"HEARTBEAT","cp_id":"CP01"}
# {"type":"STATUS","cp_id":"CP01","state":"AVERIADO"}

class CentralState:
    def __init__(self):
        # cp_id -> dict(info)
        self.cps: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        self.last_hb: Dict[str, float] = defaultdict(lambda: 0.0)

CENTRAL = CentralState()

class MonitorTCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            for line in self.rfile:
                try:
                    msg = json.loads(line.decode('utf-8').strip())
                except json.JSONDecodeError:
                    continue
                t = msg.get("type")
                cp_id = msg.get("cp_id")
                if not cp_id:
                    continue
                if t == "HELLO":
                    with CENTRAL.lock:
                        info = CENTRAL.cps.get(cp_id, {"state": ST_DESC})
                        info["cp_id"] = cp_id
                        info["price"] = float(msg.get("price", info.get("price", 0.30)))
                        info["location"] = msg.get("location", info.get("location", "N/A"))
                        info["state"] = info.get("state", ST_DESC)
                        CENTRAL.cps[cp_id] = info
                        CENTRAL.last_hb[cp_id] = time.time()
                    self.wfile.write(b'{"ok":true}\n')
                    print(Style.BRIGHT + f"[MONITOR] HELLO de {cp_id} {info['location']} precio {info['price']}€")
                elif t == "HEARTBEAT":
                    with CENTRAL.lock:
                        CENTRAL.last_hb[cp_id] = time.time()
                    self.wfile.write(b'{"ok":true}\n')
                elif t == "STATUS":
                    new_state = msg.get("state")
                    with CENTRAL.lock:
                        info = CENTRAL.cps.setdefault(cp_id, {"cp_id":cp_id, "price":0.30,"location":"N/A"})
                        info["state"] = new_state
                    self.wfile.write(b'{"ok":true}\n')
                    print(Style.BRIGHT + f"[MONITOR] STATUS {cp_id} -> {new_state}")
        except Exception as e:
            print(f"[MONITOR] Error de socket: {e}")

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

def start_monitor_server(host, port):
    server = ThreadedTCPServer((host, port), MonitorTCPHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(Style.BRIGHT + f"[CENTRAL] Servidor monitores en {host}:{port}")
    return server

# ======== Kafka handlers =========
def make_producer(bootstrap):
    for _ in range(5):
        try:
            return KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except NoBrokersAvailable:
            print("[CENTRAL] Esperando broker Kafka...")
            time.sleep(2)
    raise RuntimeError("No se pudo conectar a Kafka")

# ---- Hilo interactivo que permite escribir comandos ----
def command_loop(bootstrap, db_host):
    buffer = ""
    conn = None
    try:
        print("\n[CENTRAL] ¡Bienvenido!")
        print("[CENTRAL] Conexión a la base de datos establecida.")
        print("[CENTRAL] En cualquier momento puedes ejecutar cualquiera de los siguientes comandos (escríbelo y pulsa ENTER):")
        print("  > parar <id_CP / todos>")
        print("  > reanudar <id_CP / todos>")
        print("  > salir\n")
        print("> ", end="", flush=True)

        # Intentar conexión inicial
        conn = get_connection(db_host)
        producer = make_producer(bootstrap)

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

                    # ---- SALIR ----
                    if cmd == "salir":
                        print("\n[CENTRAL] Cerrando CENTRAL...")
                        stop_event.set()
                        break

                    # ---- PARAR ----
                    elif cmd.startswith("parar"):
                        parts = cmd.split()
                        if len(parts) == 2:
                            target = parts[1]
                            try:
                                cur = conn.cursor()
                                if target == "todos":
                                    cur.execute("UPDATE CP SET estado=%s;", ("PARADO",))
                                    print("[CENTRAL] Enviada orden PARAR a todos los CPs")
                                    for cp_id in list(CENTRAL.cps.keys()):
                                        producer.send(CP_COMMANDS_FMT.format(cp_id=cp_id), {"type": MSG_CMD_OUTOFORDER, "cp_id": cp_id})
                                else:
                                    cur.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("PARADO", target))
                                    print(f"[CENTRAL] Enviada orden PARAR a CP {target}")
                                    producer.send(CP_COMMANDS_FMT.format(cp_id=target), {"type": MSG_CMD_OUTOFORDER, "cp_id": target})
                                conn.commit()
                            except Exception as e:
                                print(f"[CENTRAL][DB] Error al ejecutar PARAR: {e}")
                                try:
                                    conn.close()
                                except:
                                    pass
                                time.sleep(2)
                                print("[CENTRAL][DB] Reconectando a la base de datos...")
                                conn = get_connection(db_host)
                                print("[CENTRAL][DB] Reconexión exitosa.")

                    # ---- REANUDAR ----
                    elif cmd.startswith("reanudar"):
                        parts = cmd.split()
                        if len(parts) == 2:
                            target = parts[1]
                            try:
                                cur = conn.cursor()
                                if target == "todos":
                                    cur.execute("UPDATE CP SET estado=%s;", ("ACTIVADO",))
                                    print("[CENTRAL] Enviada orden REANUDAR a todos los CPs")
                                    for cp_id in list(CENTRAL.cps.keys()):
                                        producer.send(CP_COMMANDS_FMT.format(cp_id=cp_id), {"type": MSG_CMD_RESUME, "cp_id": cp_id})
                                else:
                                    cur.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("ACTIVADO", target))
                                    print(f"[CENTRAL] Enviada orden REANUDAR a CP {target}")
                                    producer.send(CP_COMMANDS_FMT.format(cp_id=target), {"type": MSG_CMD_RESUME, "cp_id": target})
                                conn.commit()
                            except Exception as e:
                                print(f"[CENTRAL][DB] Error al ejecutar REANUDAR: {e}")
                                try:
                                    conn.close()
                                except:
                                    pass
                                time.sleep(2)
                                print("[CENTRAL][DB] Reconectando a la base de datos...")
                                conn = get_connection(db_host)
                                print("[CENTRAL][DB] Reconexión exitosa.")

                    else:
                        print(f"[CENTRAL] Comando no reconocido: {cmd}")
                    print("> ", end="", flush=True)

                elif char == "\b":  # Retroceso
                    if buffer:
                        buffer = buffer[:-1]
                        print("\b \b", end="", flush=True)
                else:
                    buffer += char
                    print(char, end="", flush=True)
            else:
                time.sleep(0.1)

    except Exception as e:
        print(f"[CENTRAL] Excepción en command_loop: {e}")
    finally:
        if conn:
            try:
                conn.close()
                print("[CENTRAL] Conexión de command_loop cerrada correctamente.")
            except Exception:
                pass

def consumer_thread(bootstrap, db_host):
    # DB init
    conn = get_connection(db_host)
    init_db(conn)
    producer = make_producer(bootstrap)

    consumer = KafkaConsumer(
        CENTRAL_TELEMETRY, CENTRAL_EVENTS, DRIVER_REQUESTS, CP_EVENTS,
        bootstrap_servers=bootstrap,
        group_id="central",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    print(Style.DIM + "[CENTRAL] Consumidor Kafka iniciado.")
    for msg in consumer:
        val = msg.value
        mtype = val.get("type")
        if mtype == MSG_CP_REGISTER:
            cp_id = val["cp_id"]
            with CENTRAL.lock:
                info = CENTRAL.cps.get(cp_id, {"state": ST_DESC})
                info["cp_id"] = cp_id
                info["price"] = float(val.get("price", info.get("price", 0.30)))
                info["location"] = val.get("location", info.get("location","N/A"))
                info["state"] = info.get("state", ST_ACTIVADO)
                CENTRAL.cps[cp_id] = info
            print(Style.BRIGHT + f"[CENTRAL] CP registrado {cp_id} {info['location']}")
        elif mtype == MSG_CP_TELEMETRY:
            cp_id = val["cp_id"]
            kw = val.get("kw", 0.0)
            euros = val.get("euros", 0.0)
            driver = val.get("driver_id","?")
            with CENTRAL.lock:
                info = CENTRAL.cps.setdefault(cp_id, {"cp_id":cp_id,"price":0.30,"location":"N/A","state":ST_ACTIVADO})
                info["state"] = ST_SUMINISTRANDO
            print(c_state_bg(ST_SUMINISTRANDO) + f" [TEL] {cp_id} -> {kw:.2f} kW, {euros:.2f} €, driver {driver} " + Style.RESET_ALL)
        elif mtype == MSG_CP_TICKET:
            # guardar en BD
            cp_id = val["cp_id"]; driver = int(val["driver_id"])
            consumo = float(val["kw"])
            importe = float(val["euros"])
            with conn.cursor() as cur:
                try:
                    cur.execute("INSERT IGNORE INTO CONDUCTOR(idConductor) VALUES(%s)", (driver,))
                    cur.execute("INSERT INTO CONSUMO(conductor, cp, consumo, importe) VALUES(%s,%s,%s,%s)",
                                (driver, cp_id, consumo, importe))
                    conn.commit()
                except Exception as e:
                    print(f"[DB] Error insert: {e}")
            # reenviar ticket al driver
            topic = DRIVER_EVENTS_FMT.format(driver_id=driver)
            producer.send(topic, {"type": MSG_DRV_TICKET, "cp_id": cp_id, "kw": consumo, "euros": importe})
            with CENTRAL.lock:
                info = CENTRAL.cps.setdefault(cp_id, {"cp_id":cp_id,"price":0.30,"location":"N/A"})
                info["state"] = ST_ACTIVADO
            print(Style.BRIGHT + f"[CENTRAL] Ticket enviado a Driver {driver}. CP {cp_id} listo.")
        elif mtype == MSG_CP_STATUS:
            cp_id = val["cp_id"]; st = val["state"]
            with CENTRAL.lock:
                info = CENTRAL.cps.setdefault(cp_id, {"cp_id":cp_id,"price":0.30,"location":"N/A"})
                info["state"] = st
            print(Style.BRIGHT + f"[CENTRAL] Estado {cp_id} -> {st}")
        elif mtype == MSG_DRV_REQUEST:
            driver = val["driver_id"]; cp_id = val["cp_id"]
            # autorizar si el CP está activado
            with CENTRAL.lock:
                st = CENTRAL.cps.get(cp_id, {}).get("state", ST_DESC)
                price = CENTRAL.cps.get(cp_id, {}).get("price", 0.30)
            topic = DRIVER_EVENTS_FMT.format(driver_id=driver)
            if st == ST_ACTIVADO:
                # enviar start al CP
                cmd_topic = CP_COMMANDS_FMT.format(cp_id=cp_id)
                payload = {"type": MSG_CMD_START, "cp_id": cp_id, "driver_id": driver, "price": price}
                producer.send(cmd_topic, payload)
                producer.send(topic, {"type": MSG_DRV_UPDATE, "status":"AUTORIZADO","cp_id":cp_id})
                print(Style.BRIGHT + f"[CENTRAL] Autorizado servicio Driver {driver} en {cp_id}")
            else:
                producer.send(topic, {"type": MSG_DRV_UPDATE, "status":"DENEGADO","reason": f"CP {cp_id} en estado {st}"})
                print(Style.BRIGHT + f"[CENTRAL] Denegado servicio Driver {driver} en {cp_id}: {st}")

def watchdog_disconnected(timeout=5.0):
    while True:
        time.sleep(1.0)
        now = time.time()
        with CENTRAL.lock:
            for cp_id, last in list(CENTRAL.last_hb.items()):
                if now - last > timeout:
                    info = CENTRAL.cps.setdefault(cp_id, {"cp_id":cp_id,"price":0.30,"location":"N/A"})
                    if info.get("state") != ST_DESC:
                        info["state"] = ST_DESC
                        print(Style.DIM + f"[CENTRAL] CP {cp_id} marcado DESCONECTADO (timeout heartbeat)")

def panel_thread():
    # Panel simple en consola cada 2s
    while True:
        time.sleep(2)
        with CENTRAL.lock:
            cps = list(CENTRAL.cps.values())
        sys.stdout.write("\x1b[2J\x1b[H")  # clear
        print(Style.BRIGHT + "=== EV_Central - Panel ===")
        for info in sorted(cps, key=lambda x: x["cp_id"]):
            bg = c_state_bg(info.get("state", ST_DESC))
            txt = f"{info['cp_id']:>6} | {info.get('location','N/A'):<15} | {info.get('price',0.0):>5.2f} €/kWh | {info.get('state', ST_DESC)}"
            print(bg + " " + txt + " " + Style.RESET_ALL)
        print(Style.DIM + "Ctrl+C para salir")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True, help="Puerto TCP para monitores (EV_CP_M)")
    ap.add_argument("--bootstrap", required=True, help="IP:puerto del bootstrap-server Kafka")
    ap.add_argument("--dbhost", required=True, help="Host/IP de la BD MySQL (debe existir BBDD EVCharging)")
    args = ap.parse_args()

    # Arranques
    start_monitor_server(args.host, args.port)
    threading.Thread(target=watchdog_disconnected, daemon=True).start()
    threading.Thread(target=panel_thread, daemon=True).start()
    threading.Thread(target=consumer_thread, args=(args.bootstrap, args.dbhost), daemon=True).start()
    threading.Thread(target=command_loop, args=(args.bootstrap, args.dbhost), daemon=True).start()

    # Bloqueo principal
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[CENTRAL] Saliendo...")

if __name__ == "__main__":
    main()
# EV_CP_E.py
# Engine del CP. Recibe comandos de Central por Kafka. Atiende pings del Monitor por TCP.
import argparse, json, socketserver, threading, time, sys, random
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from EV_Topics import *

class State:
    def __init__(self, cp_id, price):
        self.cp_id = cp_id
        self.state = ST_ACTIVADO
        self.price = price
        self.ko_mode = False         # alternable desde consola: simula avería local
        self.serving_driver = None
        self.lock = threading.Lock()

ENGINE: State = None

# ===== TCP server para Monitor =====
# Protocolo simple línea JSON:
#   {"op":"PING"} -> {"ok":true,"status":"OK"} o {"ok":false,"status":"KO"}
class MonitorHandler(socketserver.StreamRequestHandler):
    def handle(self):
        global ENGINE
        for line in self.rfile:
            try:
                msg = json.loads(line.decode('utf-8').strip())
            except:
                continue
            if msg.get("op") == "PING":
                with ENGINE.lock:
                    ok = not ENGINE.ko_mode
                status = "OK" if ok else "KO"
                self.wfile.write(json.dumps({"ok": ok, "status": status}).encode('utf-8') + b"\n")

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

def make_producer(bootstrap):
    for _ in range(5):
        try:
            return KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except NoBrokersAvailable:
            print("[CP_E] Esperando broker Kafka...")
            time.sleep(2)
    raise RuntimeError("No se pudo conectar a Kafka")

def kafka_thread(bootstrap):
    global ENGINE
    prod = make_producer(bootstrap)
    consumer = KafkaConsumer(
        CP_COMMANDS_FMT.format(cp_id=ENGINE.cp_id),
        bootstrap_servers=bootstrap,
        group_id=f"cp_{ENGINE.cp_id}",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    print(f"[CP_E {ENGINE.cp_id}] Consumidor Kafka listo.")
    for msg in consumer:
        val = msg.value
        t = val.get("type")
        if t == MSG_CMD_START and val.get("cp_id")==ENGINE.cp_id:
            with ENGINE.lock:
                if ENGINE.state == ST_ACTIVADO and ENGINE.serving_driver is None:
                    ENGINE.serving_driver = val["driver_id"]
                    ENGINE.state = ST_SUMINISTRANDO
                    print(f"[CP_E {ENGINE.cp_id}] Solicitud de suministro para Driver {ENGINE.serving_driver}.")
                    print("    >> Pulsa 'p'+Enter para simular enchufado y comenzar.")
                else:
                    print(f"[CP_E {ENGINE.cp_id}] No puedo iniciar (estado {ENGINE.state}).")
        elif t in (MSG_CMD_STOP, MSG_CMD_OUTOFORDER):
            with ENGINE.lock:
                ENGINE.state = ST_PARADO if t==MSG_CMD_OUTOFORDER else ST_ACTIVADO
                print(f"[CP_E {ENGINE.cp_id}] Comando recibido: {t}. Nuevo estado: {ENGINE.state}")

def operator_thread(bootstrap):
    global ENGINE
    prod = make_producer(bootstrap)
    while True:
        try:
            s = input().strip().lower()
        except EOFError:
            break
        if s == "k":
            with ENGINE.lock:
                ENGINE.ko_mode = not ENGINE.ko_mode
            print(f"[CP_E {ENGINE.cp_id}] KO mode -> {ENGINE.ko_mode}")
        elif s == "p":
            with ENGINE.lock:
                if ENGINE.state == ST_SUMINISTRANDO and ENGINE.serving_driver is not None:
                    driver = ENGINE.serving_driver
                else:
                    print("No hay servicio pendiente.")
                    continue
            # Simular suministro ~5-10s
            kw = 0.0; euros = 0.0
            for _ in range(random.randint(5,10)):
                time.sleep(1)
                with ENGINE.lock:
                    if ENGINE.ko_mode:
                        print("[CP_E] Avería durante suministro. Cortando...")
                        ENGINE.state = ST_AVERIADO
                        ENGINE.serving_driver = None
                        prod.send(CENTRAL_EVENTS, {"type": MSG_CP_STATUS,"cp_id":ENGINE.cp_id,"state":ST_AVERIADO})
                        break
                kw += random.uniform(1.2, 3.0)
                euros = kw * ENGINE.price
                prod.send(CENTRAL_TELEMETRY, {"type": MSG_CP_TELEMETRY, "cp_id": ENGINE.cp_id,
                                              "kw": kw, "euros": euros, "driver_id": driver})
            else:
                # fin normal
                prod.send(CENTRAL_EVENTS, {"type": MSG_CP_TICKET, "cp_id": ENGINE.cp_id,
                                           "driver_id": driver, "kw": kw, "euros": euros})
                with ENGINE.lock:
                    ENGINE.state = ST_ACTIVADO
                    ENGINE.serving_driver = None
        else:
            print("Comandos: 'p' iniciar/continuar suministro, 'k' alterna KO.")

def main():
    global ENGINE
    ap = argparse.ArgumentParser()
    ap.add_argument("--cp_id", required=True)
    ap.add_argument("--price", type=float, default=0.35)
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True, help="Puerto TCP para pings del Monitor")
    args = ap.parse_args()
    ENGINE = State(args.cp_id, args.price)

    # TCP para Monitor
    server = ThreadedTCPServer((args.host, args.port), MonitorHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"[CP_E {args.cp_id}] Escuchando monitor en {args.host}:{args.port}")

    # Kafka register
    prod = make_producer(args.bootstrap)
    prod.send(CENTRAL_EVENTS, {"type": MSG_CP_REGISTER, "cp_id": args.cp_id, "price": args.price, "location": "N/A"})

    threading.Thread(target=kafka_thread, args=(args.bootstrap,), daemon=True).start()
    threading.Thread(target=operator_thread, args=(args.bootstrap,), daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[CP_E] Saliendo...")

if __name__ == "__main__":
    main()
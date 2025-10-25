# EV_CP_M.py
# Monitor de un CP. Autentica/Registra contra Central por socket, y vigila Engine por socket.
import argparse, json, socket, threading, time

def send_json_line(sock, obj):
    data = (json.dumps(obj) + "\n").encode('utf-8')
    sock.sendall(data)
    # leer respuesta si la hay (no bloqueante estricto)
    sock.settimeout(2.0)
    try:
        resp = sock.recv(4096)
        if resp:
            return json.loads(resp.decode('utf-8').splitlines()[-1])
    except Exception:
        return None

def monitor_engine_loop(engine_host, engine_port, report_fn, stop_evt: threading.Event):
    while not stop_evt.is_set():
        try:
            with socket.create_connection((engine_host, engine_port), timeout=2.0) as esock:
                # ping rápido
                esock.sendall(b'{"op":"PING"}\n')
                esock.settimeout(2.0)
                resp = esock.recv(4096)
                ok = False
                if resp:
                    try:
                        ok = json.loads(resp.decode('utf-8').splitlines()[-1]).get("ok", False)
                    except:
                        ok = False
                report_fn(ok)
        except Exception:
            report_fn(False)
        time.sleep(1.0)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--central_host", required=True)
    ap.add_argument("--central_port", type=int, required=True)
    ap.add_argument("--engine_host", required=True)
    ap.add_argument("--engine_port", type=int, required=True)
    ap.add_argument("--cp_id", required=True)
    ap.add_argument("--price", type=float, default=0.35)
    ap.add_argument("--location", default="N/A")
    args = ap.parse_args()

    # Conectar a CENTRAL por TCP
    csock = socket.create_connection((args.central_host, args.central_port), timeout=5.0)
    # HELLO
    send_json_line(csock, {"type":"HELLO","cp_id":args.cp_id,"price":args.price,"location":args.location})

    last_state = "INIT"
    def report(ok):
        nonlocal last_state
        st = "ACTIVADO" if ok else "AVERIADO"
        if st != last_state:
            send_json_line(csock, {"type":"STATUS","cp_id":args.cp_id,"state":st})
            last_state = st
        # heartbeat
        send_json_line(csock, {"type":"HEARTBEAT","cp_id":args.cp_id})

    stop_evt = threading.Event()
    try:
        t = threading.Thread(target=monitor_engine_loop, args=(args.engine_host,args.engine_port,report,stop_evt), daemon=True)
        t.start()
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        stop_evt.set()
        csock.close()
        print("\n[CP_M] Saliendo...")

if __name__ == "__main__":
    main()
# EV_Driver.py
# Cliente Driver. Envía peticiones a Central por Kafka y recibe updates/ticket.
import argparse, json, time
from kafka import KafkaProducer, KafkaConsumer
from EV_Topics import *

def make_producer(bootstrap):
    from kafka.errors import NoBrokersAvailable
    import time
    for _ in range(5):
        try:
            return KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except NoBrokersAvailable:
            print("[DRIVER] Esperando broker Kafka..."); time.sleep(2)
    raise RuntimeError("No se pudo conectar a Kafka")

def run_sequence(bootstrap, driver_id, services):
    prod = make_producer(bootstrap)
    topic_events = DRIVER_EVENTS_FMT.format(driver_id=driver_id)
    cons = KafkaConsumer(topic_events, bootstrap_servers=bootstrap, group_id=f"drv_{driver_id}",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')), auto_offset_reset="earliest")
    print(f"[DRIVER {driver_id}] Escuchando respuestas en {topic_events}")

    for cp_id in services:
        print(f"[DRIVER {driver_id}] Solicitando servicio en CP {cp_id}...")
        prod.send(DRIVER_REQUESTS, {"type": MSG_DRV_REQUEST, "driver_id": int(driver_id), "cp_id": cp_id})

        ticket_received = False
        t0 = time.time()
        while time.time() - t0 < 120:  # timeout 2 min por servicio
            for msg in cons.poll(timeout_ms=500).values():
                for record in msg:
                    val = record.value
                    if val.get("type")==MSG_DRV_UPDATE:
                        print(f"  >> Update: {val.get('status')} {val.get('reason','')}")
                    elif val.get("type")==MSG_DRV_TICKET:
                        if val.get("cp_id")==cp_id:
                            print(f"  >> Ticket: CP {cp_id} {val['kw']:.2f} kWh, {val['euros']:.2f} €")
                            ticket_received = True
                            break
            if ticket_received:
                break
        print("[DRIVER] Esperando 4s antes del siguiente servicio...")
        time.sleep(4)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--driver_id", required=True, type=int)
    ap.add_argument("--services_file", help="Fichero con lista de CP_ID (uno por línea)")
    ap.add_argument("--cp_id", help="Solicitar 1 servicio inmediato para este CP")
    args = ap.parse_args()
    services = []
    if args.services_file:
        with open(args.services_file, "r", encoding="utf-8") as f:
            services = [line.strip() for line in f if line.strip()]
    if args.cp_id:
        services.append(args.cp_id)
    if not services:
        print("Debes indicar --cp_id o --services_file"); return
    run_sequence(args.bootstrap, args.driver_id, services)

if __name__ == "__main__":
    main()
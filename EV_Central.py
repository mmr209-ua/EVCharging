#EV_Central
import sys
import threading
import json
import socket
import os
from kafka import KafkaConsumer, KafkaProducer
from EV_DB import *
from EV_Topics import *
from colorama import init, Fore, Back, Style

def main():
    if len(sys.argv) < 4:
        print("Uso: python central.py <puerto_tcp_monitores> <broker_ip:puerto> <db_ip>")
        sys.exit(1)

    listen_port = int(sys.argv[1])
    broker = sys.argv[2]
    db_host = sys.argv[3]

    # ================================
    # Conexión inicial (solo para crear tablas)
    # ================================
    print("Inicializando base de datos...")
    conn = get_connection(db_host)
    init_db(conn)
    
    # CORRECCIÓN ERROR 1: Marcar todos los CPs como DESCONECTADO al inicio
    cursor = conn.cursor()
    cursor.execute("UPDATE CP SET estado='DESCONECTADO' WHERE estado != 'DESCONECTADO' OR estado IS NULL;")
    conn.commit()
    conn.close()
    print("[DB] Tablas inicializadas correctamente. Todos los CPs marcados como DESCONECTADO.")

    # ================================
    # Kafka Producer y Consumers
    # ================================
    kafka_server = [broker]

    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    consumers = {
        CP_STATUS: KafkaConsumer(CP_STATUS, bootstrap_servers=kafka_server, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_ALERTS: KafkaConsumer(CP_ALERTS, bootstrap_servers=kafka_server, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_HEALTH: KafkaConsumer(CP_HEALTH, bootstrap_servers=kafka_server, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_CONSUMPTION: KafkaConsumer(CP_CONSUMPTION, bootstrap_servers=kafka_server, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CP_SUPPLY_COMPLETE: KafkaConsumer(CP_SUPPLY_COMPLETE, bootstrap_servers=kafka_server, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central"),
        CHARGING_REQUESTS: KafkaConsumer(CHARGING_REQUESTS, bootstrap_servers=kafka_server, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="central")
    }

    # ==========================================================
    # Servidor TCP para monitores
    # ==========================================================
    def monitor_server():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        s.bind(("0.0.0.0", listen_port))
        s.listen(10)
        print(f"[CENTRAL] Servidor TCP de monitores escuchando en puerto {listen_port}")

        while True:
            conn_tcp, addr = s.accept()
            threading.Thread(target=handle_monitor, args=(conn_tcp, addr), daemon=True).start()

    def handle_monitor(conn_tcp, addr):
        buffer = ""
        local_conn = get_connection(db_host)
        try:
            while True:
                data = conn_tcp.recv(1024).decode()
                if not data:
                    break
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    msg = json.loads(line)
                    tipo = msg.get("type")
                    idCP = msg.get("idCP")

                    if tipo == "register":
                        precio = msg["precio"]
                        ubicacion = msg["ubicacion"]
                        cursor = local_conn.cursor()
                        cursor.execute("""
                            INSERT INTO CP (idCP, estado, precio, ubicacion)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE precio=%s, ubicacion=%s;
                        """, (idCP, "ACTIVADO", precio, ubicacion, precio, ubicacion))
                        local_conn.commit()
                        print(f"[CENTRAL] CP {idCP} registrado por socket desde {addr}")
                        conn_tcp.sendall(json.dumps({"status": "OK"}).encode("utf-8") + b"\n")

                    elif tipo == "status":
                        estado = msg["estado"]
                        cursor = local_conn.cursor()
                        cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (estado, idCP))
                        local_conn.commit()
                        print(f"[CENTRAL] Estado actualizado CP {idCP}: {estado}")

                    elif tipo == "health":
                        salud = msg["salud"]
                        print(f"[CENTRAL] Salud CP {idCP}: {salud}")

                    elif tipo == "alert":
                        alerta = msg["alerta"]
                        print(f"[CENTRAL] ALERTA de CP {idCP}: {alerta}")

                    else:
                        print(f"[CENTRAL] Mensaje desconocido desde CP {idCP}: {msg}")

        except Exception as e:
            print(f"[CENTRAL] Error con monitor {addr}: {e}")
        finally:
            conn_tcp.close()
            local_conn.close()

    threading.Thread(target=monitor_server, daemon=True).start()

    # ==========================================================
    # Kafka Consumers (Engine / Drivers)
    # ==========================================================
    def consume_loop(topic, consumer):
        # Cada hilo usa su propia conexión a MySQL
        local_conn = get_connection(db_host)

        for msg in consumer:
            try:
                event = msg.value
                print(f"[CENTRAL] Kafka {topic}: {event}")
                cursor = local_conn.cursor()

                # --- Peticiones de recarga (Driver o Engine) ---
                # En la función consume_loop, dentro del caso CHARGING_REQUESTS, agregar esta validación:

                if topic == CHARGING_REQUESTS:
                    idCP = str(event.get("idCP"))
                    idDriver = str(event.get("idDriver"))
                    print(f"[CENTRAL] 🚗 Petición de recarga -> CP {idCP}, Driver {idDriver}")

                    cursor.execute("SELECT estado FROM CP WHERE idCP=%s;", (idCP,))
                    row = cursor.fetchone()
                    estado_cp = row[0] if row else None

                    if estado_cp is None:
                        cursor.execute("INSERT INTO CP (idCP, estado, precio, ubicacion) VALUES (%s, %s, %s, %s);",
                                    (idCP, "ACTIVADO", 0.25, "DESCONOCIDA"))
                        local_conn.commit()
                        estado_cp = "ACTIVADO"
                        print(f"[CENTRAL] CP {idCP} no estaba en BD. Insertado y marcado ACTIVADO.")

                    # CORRECCIÓN ERROR 1: Rechazar si está SUMINISTRANDO
                    if estado_cp == "SUMINISTRANDO":
                        print(f"[CENTRAL] ❌ RECHAZO INMEDIATO: CP {idCP} está SUMINISTRANDO a otro vehículo")
                        producer.send(DRIVER_SUPPLY_COMPLETE, {
                            "idCP": idCP, 
                            "ticket": {
                                "idDriver": idDriver,
                                "motivo": "RECHAZADO_CP_OCUPADO",
                                "estado": "RECHAZADO"
                            }
                        })
                        producer.flush()
                    elif estado_cp == "AVERIADO":
                        print(f"[CENTRAL] ❌ RECHAZO INMEDIATO: CP {idCP} está AVERIADO")
                        producer.send(DRIVER_SUPPLY_COMPLETE, {
                            "idCP": idCP, 
                            "ticket": {
                                "idDriver": idDriver,
                                "motivo": "RECHAZADO_CP_AVERIADO",
                                "estado": "RECHAZADO"
                            }
                        })
                        producer.flush()
                    elif estado_cp in ("ACTIVADO", "DESCONECTADO"):
                        cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", ("SUMINISTRANDO", idCP))
                        local_conn.commit()
                        producer.send(CP_AUTHORIZE_SUPPLY, {
                            "idCP": idCP,
                            "idDriver": idDriver,
                            "action": "authorize"
                        })
                        producer.flush()
                        print(f"[CENTRAL] ✅ Autorizado CP {idCP} para Driver {idDriver}")
                        
                        # CORRECCIÓN ERROR 3: Notificar al driver que fue autorizado
                        producer.send(DRIVER_SUPPLY_COMPLETE, {
                            "idCP": idCP,
                            "ticket": {
                                "idDriver": idDriver,
                                "estado": "AUTORIZADO",
                                "mensaje": "Puede comenzar el suministro"
                            }
                        })
                        producer.flush()
                    else:
                        print(f"[CENTRAL] ⚠️ No autorizado: CP {idCP} estado actual = {estado_cp}")


                # --- CORRECCIÓN ERROR 3: Mostrar consumo durante repostaje ---
                elif topic == CP_CONSUMPTION:
                    idCP = str(event.get("idCP"))
                    consumo = event.get("consumo")
                    importe = event.get("importe", None)
                    cursor.execute("UPDATE CP SET consumo_actual=%s WHERE idCP=%s;", (consumo, idCP))
                    local_conn.commit()
                    print(f"[CENTRAL] 🔋 Consumo actualizado CP {idCP}: {consumo} kWh / {importe} €")

                # --- Estado CP (vía Kafka) ---
                elif topic == CP_STATUS:
                    idCP = str(event.get("idCP"))
                    estado_nuevo = event.get("estado")
                    cursor.execute("UPDATE CP SET estado=%s WHERE idCP=%s;", (estado_nuevo, idCP))
                    local_conn.commit()
                    print(f"[CENTRAL] Estado (kafka) actualizado CP {idCP}: {estado_nuevo}")

                # --- CORRECCIÓN ERROR 4 y 5: Fin del suministro y guardar en BD ---
                elif topic == CP_SUPPLY_COMPLETE:
                    idCP = str(event.get("idCP"))
                    ticket = event.get("ticket", {})
                    
                    # CORRECCIÓN: Solo cambiar a ACTIVADO si no está AVERIADO
                    cursor.execute("SELECT estado FROM CP WHERE idCP=%s;", (idCP,))
                    row = cursor.fetchone()
                    estado_actual = row[0] if row else "ACTIVADO"
                    
                    if estado_actual != "AVERIADO":
                        cursor.execute("UPDATE CP SET estado=%s, consumo_actual=0 WHERE idCP=%s;", ("ACTIVADO", idCP))
                        print(f"[CENTRAL] ✅ CP {idCP} vuelve a estado ACTIVADO tras suministro")
                    else:
                        cursor.execute("UPDATE CP SET consumo_actual=0 WHERE idCP=%s;", (idCP,))
                        print(f"[CENTRAL] ⚠️ CP {idCP} mantiene estado AVERIADO tras suministro interrumpido")
                    
                    local_conn.commit()
                    
                    # Guardar en tabla CONSUMO
                    idDriver = ticket.get("idDriver")
                    energia = ticket.get("energia", 0)
                    precio_total = ticket.get("precio_total", 0)
                    
                    if idDriver and energia > 0:  # Solo guardar si hubo consumo real
                        cursor.execute("INSERT IGNORE INTO CONDUCTOR (idConductor) VALUES (%s);", (idDriver,))
                        cursor.execute("""
                            INSERT INTO CONSUMO (conductor, cp, consumo, importe) 
                            VALUES (%s, %s, %s, %s);
                        """, (idDriver, idCP, energia, precio_total))
                        local_conn.commit()
                    
                    producer.send(DRIVER_SUPPLY_COMPLETE, {"idCP": idCP, "ticket": ticket})
                    producer.flush()
                    print(f"[CENTRAL] 🧾 Ticket enviado al driver {ticket.get('idDriver')} (CP {idCP}): {energia} kWh / {precio_total} €")

                # --- Otros topics ---
                elif topic == CP_HEALTH:
                    idCP = str(event.get("idCP"))
                    print(f"[CENTRAL] Health check CP {idCP}: {event}")
                elif topic == CP_ALERTS:
                    idCP = str(event.get("idCP"))
                    print(f"[CENTRAL] ALERTA recibida de CP {idCP}: {event}")

            except Exception as e:
                print(f"[CENTRAL] Error procesando mensaje Kafka: {e}")
                try:
                    local_conn = get_connection(db_host)
                except:
                    pass

    # Lanzar un hilo por cada consumer
    for t, c in consumers.items():
        threading.Thread(target=consume_loop, args=(t, c), daemon=True).start()

    print("[CENTRAL] Sistema iniciado correctamente. Esperando eventos...")
    try:
        while True:
            threading.Event().wait(1)
    except KeyboardInterrupt:
        print("[CENTRAL] Terminando por KeyboardInterrupt.")

if __name__ == "__main__":
    main()
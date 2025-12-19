# EV_CP_E.py - Release 2 con cifrado de mensajes Kafka
import sys
import json
import time
import datetime
import threading
import socket
import os
from kafka import KafkaProducer, KafkaConsumer
from EV_Topics import *

# Archivo de configuracion compartido con CP_M
CONFIG_FILE = None

# Carga la configuración del CP desde su archivo
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def main():
    if len(sys.argv) < 4:
        print("Uso: python EV_CP_E.py <broker_ip:puerto> <cp_id> <listen_port>")
        sys.exit(1)

    broker = sys.argv[1]
    cp_id = str(sys.argv[2])
    listen_port = int(sys.argv[3])

    # Cargar configuración (encryption_key del CP_M)
    global CONFIG_FILE
    CONFIG_FILE = f"cp_config_{cp_id}.json"
    config = load_config()
    encryption_key = config.get("encryption_key")

    if encryption_key:
        print(f"[ENGINE {cp_id}] Clave de cifrado cargada de {CONFIG_FILE}")
    else:
        print(f"[ENGINE {cp_id}] Sin clave de cifrado - modo legacy (sin cifrar)")

    # Importar módulo de cifrado si hay clave
    encrypt_message = None
    if encryption_key:
        try:
            from crypto_utils import encrypt_message as _encrypt
            encrypt_message = _encrypt
            print(f"[ENGINE {cp_id}] Cifrado HABILITADO")
        except ImportError:
            print(f"[ENGINE {cp_id}] ADVERTENCIA: crypto_utils no encontrado, cifrado deshabilitado")
            encryption_key = None

    # Funcion para enviar mensajes (con o sin cifrado)
    def send_kafka_message(producer, topic, message):
        # Envia mensaje cifrado a Kafka
        if encryption_key and encrypt_message:
            encrypted = encrypt_message(encryption_key, message) # Cifrar mensaje
            payload = {"encrypted": encrypted, "idCP": cp_id}
        # En teoria no deberia entrar aqui
        else:
            payload = message

        producer.send(topic, payload)
        producer.flush()

    # PRODUCTOR KAFKA
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # CONSUMIDORES KAFKA
    consumer_authorize = KafkaConsumer(
        AUTHORIZE_SUPPLY,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}",
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    consumer_control = KafkaConsumer(
        CP_CONTROL,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_control_{cp_id}",
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    # ESTADO INTERNO DEL CP
    estado = "ACTIVADO"
    estado_real = "ACTIVADO"
    health_ok = True
    en_suministro = False
    autorizado = False
    driver_id = None
    consumo_total = 0.0
    precio_total = 0.0
    hora_inicio = None
    menu_activo = True
    lock = threading.Lock()

    # Servidor TCP para el monitor
    def health_server():
        nonlocal health_ok
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", listen_port))
        s.listen(1)
        print(f"[ENGINE {cp_id}] Health server escuchando en puerto {listen_port}")

        while True:
            conn, _ = s.accept()
            try:
                data = conn.recv(1024).decode().strip()
                if data == "PING":
                    if health_ok:
                        conn.sendall(b"PONG")
                    else:
                        conn.sendall(b"KO")
            except:
                pass
            finally:
                conn.close()

    threading.Thread(target=health_server, daemon=True).start()

    # FUNCIONES DE SUMINISTRO
    def start_supply():
        nonlocal en_suministro, consumo_total, precio_total, hora_inicio, estado, estado_real, driver_id

        with lock:
            if not autorizado or driver_id is None:
                print("[ENGINE] No hay autorizacion valida.")
                return
            if estado_real in ("AVERIADO", "PARADO"):
                print(f"[ENGINE] No puede suministrar: CP en estado {estado_real}")
                return
            if en_suministro:
                print(f"[ENGINE] Ya hay un suministro en curso")
                return
            if estado_real != "ACTIVADO":
                print(f"[ENGINE] No puede suministrar: estado {estado_real}")
                return

            print(f"[ENGINE {cp_id}] INICIANDO SUMINISTRO para Driver {driver_id}...")
            en_suministro = True
            estado = "SUMINISTRANDO"
            estado_real = "SUMINISTRANDO"
            hora_inicio = datetime.datetime.now().isoformat()
            consumo_total = 0.0
            precio_total = 0.0

        try:
            send_kafka_message(producer, CP_STATUS, {"idCP": cp_id, "estado": "SUMINISTRANDO"})
        except Exception as e:
            print(f"[ENGINE {cp_id}] Error enviando estado a CENTRAL: {e}")

        while True:
            with lock:
                if not en_suministro or not health_ok:
                    break

                consumo_total += 0.5
                precio_total = round(consumo_total * 0.25, 2)
                consumo_actual_envio = consumo_total
                precio_actual_envio = precio_total
                driver_actual_envio = driver_id

            try:
                send_kafka_message(producer, CP_CONSUMPTION, {
                    "idCP": cp_id,
                    "consumo": consumo_actual_envio,
                    "importe": precio_actual_envio,
                    "conductor": driver_actual_envio
                })
                print(f"[ENGINE {cp_id}] +0.5 kWh -> Total {consumo_actual_envio} kWh / {precio_actual_envio} EUR")
            except Exception as e:
                print(f"[ENGINE {cp_id}] Error enviando consumo en tiempo real: {e}")

            time.sleep(1)

        debo_cortar = False
        with lock:
            if not health_ok and en_suministro:
                debo_cortar = True
                print(f"[ENGINE {cp_id}] AVERIA DETECTADA DURANTE SUMINISTRO!")
        if debo_cortar:
            stop_supply_emergencia()

    def stop_supply():
        nonlocal en_suministro, estado, estado_real, autorizado, consumo_total, precio_total, driver_id

        with lock:
            if not en_suministro:
                print("[ENGINE] No hay suministro activo.")
                return

            if driver_id is None:
                print("[ENGINE] Error: No hay driver_id asociado al suministro")
                return

            en_suministro = False
            estado = "ACTIVADO"
            estado_real = "ACTIVADO"
            hora_fin = datetime.datetime.now().isoformat()

            current_driver_id = driver_id
            current_consumo_total = consumo_total
            current_precio_total = precio_total

            ticket = {
                "energia": round(current_consumo_total, 2),
                "precio_total": round(current_precio_total, 2),
                "hora_inicio": hora_inicio,
                "hora_fin": hora_fin,
                "idCP": cp_id,
                "idDriver": current_driver_id,
                "estado": "COMPLETADO",
                "mensaje": "Suministro completado correctamente"
            }

        try:
            send_kafka_message(producer, CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
            send_kafka_message(producer, CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})

            print(f"[ENGINE {cp_id}] SUMINISTRO FINALIZADO PARA CONDUCTOR {current_driver_id}")
            print(f"    Energia: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} EUR")
            print(f"[ENGINE {cp_id}] Estado cambiado a ACTIVADO y notificado a CENTRAL")
        except Exception as e:
            print(f"[ENGINE {cp_id}] Error enviando ticket a CENTRAL: {e}")

        with lock:
            autorizado = False
            driver_id = None
            consumo_total = 0.0
            precio_total = 0.0

    def stop_supply_emergencia():
        nonlocal en_suministro, estado, estado_real, autorizado, consumo_total, precio_total, driver_id

        with lock:
            if not en_suministro:
                return
            if driver_id is None:
                print("[ENGINE] Error: No hay driver_id asociado al suministro de emergencia")
                return

            en_suministro = False
            estado = "AVERIADO"
            estado_real = "AVERIADO"
            hora_fin = datetime.datetime.now().isoformat()

            current_driver_id = driver_id
            current_consumo_total = consumo_total
            current_precio_total = precio_total

            ticket = {
                "energia": round(current_consumo_total, 2),
                "precio_total": round(current_precio_total, 2),
                "hora_inicio": hora_inicio,
                "hora_fin": hora_fin,
                "idCP": cp_id,
                "idDriver": current_driver_id,
                "motivo": "AVERIADO",
                "estado": "INTERRUMPIDO",
                "mensaje": f"Suministro interrumpido por averia. Consumo hasta el momento: {current_consumo_total} kWh / {current_precio_total} EUR"
            }

        try:
            send_kafka_message(producer, CP_STATUS, {"idCP": cp_id, "estado": "AVERIADO"})
            send_kafka_message(producer, CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})

            print(f"[ENGINE {cp_id}] SUMINISTRO INTERRUMPIDO POR AVERIA!")
            print(f"    Consumo hasta el corte: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} EUR")
            print(f"[ENGINE {cp_id}] Estado cambiado a AVERIADO y notificado a CENTRAL")
        except Exception as e:
            print(f"[ENGINE {cp_id}] Error enviando ticket de emergencia a CENTRAL: {e}")

        with lock:
            autorizado = False
            driver_id = None
            consumo_total = 0.0
            precio_total = 0.0

    def stop_supply_forzado_por_central():
        nonlocal en_suministro, estado, estado_real, autorizado, consumo_total, precio_total, driver_id

        with lock:
            if not en_suministro:
                return

            if driver_id is None:
                print("[ENGINE] Error: No hay driver_id asociado al suministro forzado por CENTRAL")
                return

            en_suministro = False
            estado = "PARADO"
            estado_real = "PARADO"
            hora_fin = datetime.datetime.now().isoformat()

            current_driver_id = driver_id
            current_consumo_total = consumo_total
            current_precio_total = precio_total

            ticket = {
                "energia": round(current_consumo_total, 2),
                "precio_total": round(current_precio_total, 2),
                "hora_inicio": hora_inicio,
                "hora_fin": hora_fin,
                "idCP": cp_id,
                "idDriver": current_driver_id,
                "motivo": "PARADO",
                "estado": "INTERRUMPIDO",
                "mensaje": f"Suministro interrumpido por orden de la central. Consumo hasta el momento: {current_consumo_total} kWh / {current_precio_total} EUR"
            }

        try:
            send_kafka_message(producer, CP_STATUS, {"idCP": cp_id, "estado": "PARADO"})
            send_kafka_message(producer, CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})

            print(f"[ENGINE {cp_id}] SUMINISTRO CORTADO POR ORDEN DE CENTRAL!")
            print(f"    Consumo hasta el corte: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} EUR")
            print(f"[ENGINE {cp_id}] Estado cambiado a PARADO y notificado a CENTRAL")
        except Exception as e:
            print(f"[ENGINE {cp_id}] Error enviando ticket forzado a CENTRAL: {e}")

        with lock:
            autorizado = False
            driver_id = None
            consumo_total = 0.0
            precio_total = 0.0

    # CONSUMO DE AUTORIZACIONES DESDE CENTRAL
    def consume_authorize_loop():
        nonlocal autorizado, driver_id, estado_real
        for msg in consumer_authorize:
            event = msg.value
            if str(event.get("idCP")) != cp_id:
                continue

            authorize = event.get("authorize")

            if authorize == "YES":
                with lock:
                    if estado_real == "ACTIVADO" and not en_suministro:
                        autorizado = True
                        driver_id = event.get("idDriver")
                        print(f"\n[ENGINE {cp_id}] AUTORIZACION CONCEDIDA para Driver {driver_id}")
                        print(f"[ENGINE {cp_id}] Use la opcion 4 del menu para INICIAR suministro")
                        print(f"[ENGINE {cp_id}] Driver {driver_id} esta esperando en su terminal...")
                    elif estado_real == "AVERIADO":
                        print(f"[ENGINE {cp_id}] Autorizacion rechazada: CP en estado AVERIADO")
                    elif estado_real == "PARADO":
                        print(f"[ENGINE {cp_id}] Autorizacion rechazada: CP en estado PARADO (orden CENTRAL)")
                    elif estado_real == "SUMINISTRANDO" or en_suministro:
                        print(f"[ENGINE {cp_id}] Autorizacion rechazada: Ya esta SUMINISTRANDO")
                    else:
                        print(f"[ENGINE {cp_id}] Autorizacion rechazada: Estado {estado_real}")

            elif authorize == "NO":
                motivo = event.get("motivo", "DESCONOCIDO")
                mensaje = event.get("mensaje", "Solicitud rechazada")
                driver_rechazado = event.get("idDriver")

                print(f"\n[ENGINE {cp_id}] AUTORIZACION RECHAZADA para Driver {driver_rechazado}")
                print(f"[ENGINE {cp_id}] Motivo: {motivo}")
                print(f"[ENGINE {cp_id}] {mensaje}")

                with lock:
                    if driver_id == driver_rechazado:
                        autorizado = False
                        driver_id = None

    threading.Thread(target=consume_authorize_loop, daemon=True).start()

    # CONSUMIR ORDENES DE CONTROL DESDE CENTRAL
    def consume_control_loop():
        nonlocal estado, estado_real, en_suministro, health_ok, autorizado, driver_id

        for msg in consumer_control:
            event = msg.value
            accion = event.get("accion")
            target = str(event.get("idCP"))

            if target != cp_id and target != "todos":
                continue

            if accion == "PARAR":
                ejecutar_corte = False
                with lock:
                    print(f"\n[ENGINE {cp_id}] ORDEN CENTRAL: PARAR")

                    if en_suministro:
                        ejecutar_corte = True
                        print(f"[ENGINE {cp_id}] Cortando suministro activo por orden CENTRAL...")
                    else:
                        estado = "PARADO"
                        estado_real = "PARADO"
                        autorizado = False
                        driver_id = None

                if ejecutar_corte:
                    stop_supply_forzado_por_central()
                    send_kafka_message(producer, CP_STATUS, {"idCP": cp_id, "estado": "PARADO"})

            elif accion == "REANUDAR":
                with lock:
                    print(f"\n[ENGINE {cp_id}] ORDEN CENTRAL: REANUDAR")

                    if estado_real == "AVERIADO" or not health_ok:
                        print(f"[ENGINE {cp_id}] No puedo reanudar: sigo en AVERIADO fisico")
                    else:
                        en_suministro = False
                        autorizado = False
                        driver_id = None
                        estado = "ACTIVADO"
                        estado_real = "ACTIVADO"

                        try:
                            send_kafka_message(producer, CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
                            print(f"[ENGINE {cp_id}] Estado reanudado a ACTIVADO, notificado a CENTRAL")
                        except Exception as e:
                            print(f"[ENGINE {cp_id}] Error notificando ACTIVADO a CENTRAL: {e}")

    threading.Thread(target=consume_control_loop, daemon=True).start()

    # Hilo para recargar configuracion periodicamente
    def reload_config_loop():
        nonlocal encryption_key, encrypt_message
        while menu_activo:
            time.sleep(10)  # Cada 10 segundos
            new_config = load_config()
            new_key = new_config.get("encryption_key")
            if new_key and new_key != encryption_key:
                encryption_key = new_key
                try:
                    from crypto_utils import encrypt_message as _encrypt
                    encrypt_message = _encrypt
                    print(f"\n[ENGINE {cp_id}] Nueva clave de cifrado cargada")
                except:
                    pass

    threading.Thread(target=reload_config_loop, daemon=True).start()

    def menu_thread():
        nonlocal health_ok, estado, estado_real, autorizado, driver_id, en_suministro, menu_activo, encryption_key

        while menu_activo:
            with lock:
                current_estado = estado_real
                current_autorizado = autorizado
                current_en_suministro = en_suministro
                current_driver_id = driver_id

            print(f"\n--- MENU ENGINE {cp_id} ---")
            if current_autorizado and not current_en_suministro:
                print(f" Driver {current_driver_id} AUTORIZADO - Use opcion 4")
                print(f" Driver esta esperando en su terminal...")
            elif current_en_suministro:
                print(f" SUMINISTRANDO a Driver {current_driver_id} - Use opcion 5 para finalizar")

            print("1 - Simular AVERIA (fisica)")
            print("2 - Simular REPARACION")
            print("3 - Peticion de SUMINISTRO desde CP (introducir ID de driver)")
            print("4 - Empezar SUMINISTRO (si esta autorizado)")
            print("5 - Terminar SUMINISTRO (envia ticket)")
            print("6 - Estado interno (debug)")
            print("7 - Recargar clave de cifrado")
            print("8 - Salir del menu")
            print(f"Estado actual: {current_estado} | Cifrado: {'SI' if encryption_key else 'NO'}")

            try:
                choice = input("Seleccione opcion: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaliendo del menu...")
                break

            if choice == "1":
                with lock:
                    estado_anterior = estado_real
                    health_ok = False
                    estado = "AVERIADO"
                    estado_real = "AVERIADO"

                print(f"[ENGINE {cp_id}] Simulando averia fisica.")
                print(f"[ENGINE {cp_id}] Monitor detectara la averia y notificara a CENTRAL via socket")

                if estado_anterior == "SUMINISTRANDO":
                    print(f"[ENGINE {cp_id}] AVERIA DURANTE SUMINISTRO!")
                    stop_supply_emergencia()

            elif choice == "2":
                with lock:
                    if estado_real != "AVERIADO":
                        print(f"[ENGINE {cp_id}] No necesita reparacion: estado actual {estado_real}")
                        continue
                    health_ok = True
                    estado = "ACTIVADO"
                    estado_real = "ACTIVADO"

                print(f"[ENGINE {cp_id}] Reparacion completada. Estado: ACTIVADO")
                print(f"[ENGINE {cp_id}] Monitor notificara recuperacion a CENTRAL via socket")

            elif choice == "3":
                with lock:
                    if estado_real in ("AVERIADO", "PARADO"):
                        print(f"[ENGINE {cp_id}] No se puede solicitar suministro: CP en estado {estado_real}")
                        continue
                    if estado_real == "SUMINISTRANDO":
                        print(f"[ENGINE {cp_id}] Ya esta SUMINISTRANDO")
                        continue

                driver_id_input = input("Introduce ID del driver: ").strip()
                if not driver_id_input:
                    print("ID invalido.")
                    continue

                print(f"[ENGINE {cp_id}] Enviando peticion de suministro para driver {driver_id_input} (desde CP)")
                try:
                    send_kafka_message(producer, SUPPLY_REQUEST_TO_CENTRAL, {
                        "idCP": cp_id,
                        "idDriver": driver_id_input
                    })
                    print(f"[ENGINE {cp_id}] Peticion enviada. Esperando autorizacion de CENTRAL...")
                except Exception as e:
                    print(f"[ENGINE {cp_id}] Error enviando peticion: {e}")

            elif choice == "4":
                with lock:
                    current_estado = estado_real
                    current_aut = autorizado
                    current_en = en_suministro
                    current_drv = driver_id

                if current_estado in ("AVERIADO", "PARADO"):
                    print(f"[ENGINE {cp_id}] No se puede suministrar: CP en estado {current_estado}")
                elif current_aut and not current_en:
                    supply_thread = threading.Thread(target=start_supply, daemon=True)
                    supply_thread.start()
                    print(f"[ENGINE {cp_id}] Iniciando suministro en segundo plano...")
                    with lock:
                        en_suministro = True
                elif current_en:
                    print(f"[ENGINE {cp_id}] Ya esta suministrando a Driver {current_drv}")
                else:
                    print(f"[ENGINE {cp_id}] No hay autorizacion para suministrar")
                    print(f"[ENGINE {cp_id}] Use la opcion 3 para solicitar autorizacion primero")

            elif choice == "5":
                stop_supply()

            elif choice == "6":
                with lock:
                    print(f"\n[ENGINE {cp_id}] ESTADO INTERNO:")
                    print(f"   Estado real: {estado_real}")
                    print(f"   Autorizado: {autorizado}")
                    print(f"   En suministro: {en_suministro}")
                    print(f"   Driver ID: {driver_id}")
                    print(f"   Health OK: {health_ok}")
                    print(f"   Consumo actual: {consumo_total} kWh")
                    print(f"   Precio actual: {precio_total} EUR")
                    print(f"   Cifrado: {'HABILITADO' if encryption_key else 'DESHABILITADO'}")

            elif choice == "7":
                new_config = load_config()
                new_key = new_config.get("encryption_key")
                if new_key:
                    encryption_key = new_key
                    print(f"[ENGINE {cp_id}] Clave de cifrado recargada desde {CONFIG_FILE}")
                else:
                    print(f"[ENGINE {cp_id}] No hay clave de cifrado en {CONFIG_FILE}")

            elif choice == "8":
                print("Saliendo del menu...")
                menu_activo = False
                break
            else:
                print("Opcion no valida.")

    menu_thread_instance = threading.Thread(target=menu_thread, daemon=True)
    menu_thread_instance.start()

    print(f"[ENGINE {cp_id}] Sistema iniciado. Esperando autorizaciones, ordenes de CENTRAL y comandos locales...")
    print(f"[ENGINE {cp_id}] Estados posibles: ACTIVADO, PARADO, SUMINISTRANDO, AVERIADO")

    try:
        while menu_activo:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"[ENGINE {cp_id}] Terminando por interrupcion del usuario.")
    finally:
        menu_activo = False
        print(f"[ENGINE {cp_id}] Programa finalizado.")

if __name__ == "__main__":
    main()

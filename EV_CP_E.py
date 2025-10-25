# EV_CP_E
import sys
import json
import time
import datetime
import threading
import socket
from kafka import KafkaProducer, KafkaConsumer
from EV_Topics import *

def main():
    if len(sys.argv) < 4:
        print("Uso: py EV_CP_E.py <broker_ip:puerto> <cp_id> <listen_port>")
        sys.exit(1)

    broker = sys.argv[1]
    cp_id = str(sys.argv[2])
    listen_port = int(sys.argv[3])

    # --- PRODUCTOR KAFKA ---
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # --- CONSUMIDORES KAFKA ---
    consumer_control = KafkaConsumer(
        CP_CONTROL,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}"
    )

    consumer_authorize = KafkaConsumer(
        CP_AUTHORIZE_SUPPLY,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}"
    )

    # --- ESTADOS INTERNOS ---
    estado = "ACTIVADO"
    en_suministro = False
    autorizado = False
    health_ok = True
    driver_id = None
    consumo_total = 0.0
    precio_total = 0.0
    hora_inicio = None
    menu_activo = True
    # CORRECCIÓN ERROR 4: Variable para controlar estado real
    estado_real = "ACTIVADO"

    # ==========================================================
    # Servidor TCP para el monitor
    # ==========================================================
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

    # ==========================================================
    # Procesar mensajes de autorización (CENTRAL -> CP_AUTHORIZE_SUPPLY)
    # ==========================================================
    def consume_authorize_loop():
        nonlocal autorizado, driver_id, estado_real, en_suministro
        for msg in consumer_authorize:
            event = msg.value
            if str(event.get("idCP")) != cp_id:
                continue
            
            # CORRECCIÓN ERROR 1: No autorizar si ya está suministrando
            if estado_real == "SUMINISTRANDO":
                print(f"[ENGINE {cp_id}] ❌ Autorización rechazada: Ya está SUMINISTRANDO a otro vehículo")
                try:
                    producer.send(DRIVER_SUPPLY_COMPLETE, {
                        "idCP": cp_id,
                        "ticket": {
                            "idDriver": event.get("idDriver"),
                            "motivo": "RECHAZADO_CP_OCUPADO",
                            "estado": "RECHAZADO"
                        }
                    })
                    producer.flush()
                except Exception as e:
                    print(f"[ENGINE {cp_id}] ❌ Error enviando rechazo: {e}")
            elif estado_real == "AVERIADO":
                print(f"[ENGINE {cp_id}] ❌ Autorización rechazada: CP en estado AVERIADO")
                try:
                    producer.send(DRIVER_SUPPLY_COMPLETE, {
                        "idCP": cp_id,
                        "ticket": {
                            "idDriver": event.get("idDriver"),
                            "motivo": "RECHAZADO_CP_AVERIADO",
                            "estado": "RECHAZADO"
                        }
                    })
                    producer.flush()
                except Exception as e:
                    print(f"[ENGINE {cp_id}] ❌ Error enviando rechazo: {e}")
            elif event.get("action") == "authorize" and estado_real == "ACTIVADO":
                # CORRECCIÓN: Verificar que no esté ya suministrando
                if en_suministro:
                    print(f"[ENGINE {cp_id}] ❌ Autorización rechazada: Ya en suministro")
                    return
                    
                autorizado = True
                driver_id = event.get("idDriver")
                print(f"[ENGINE {cp_id}] 🚗 Autorizado para suministrar al driver {driver_id}")
    

    threading.Thread(target=consume_authorize_loop, daemon=True).start()

    # ==========================================================
    # Simular suministro (envío periódico a CENTRAL)
    # ==========================================================
    def start_supply():
        nonlocal en_suministro, consumo_total, precio_total, hora_inicio, estado, estado_real
        # CORRECCIÓN ERROR 4: Usar estado_real para validación
        if not autorizado:
            print("[ENGINE] ❌ No hay autorización. Solicítela primero.")
            return
        if estado_real == "AVERIADO":
            print(f"[ENGINE] ❌ No puede suministrar: CP en estado AVERIADO")
            return
        if estado_real != "ACTIVADO":
            print(f"[ENGINE] ⚠️ No puede suministrar: estado {estado_real}")
            return

        print(f"[ENGINE {cp_id}] ⛽ Iniciando suministro...")
        en_suministro = True
        estado = "SUMINISTRANDO"
        estado_real = "SUMINISTRANDO"
        hora_inicio = datetime.datetime.now().isoformat()
        consumo_total = 0.0
        precio_total = 0.0
        
        try:
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": estado})
            producer.flush()
        except Exception as e:
            print(f"[ENGINE {cp_id}] ❌ Error enviando estado a CENTRAL: {e}")

        # CORRECCIÓN ERROR 2: Verificar continuamente health_ok durante suministro
        while en_suministro and health_ok:
            consumo_total += 0.5
            precio_total = round(consumo_total * 0.25, 2)
            try:
                producer.send(CP_CONSUMPTION, {
                    "idCP": cp_id,
                    "consumo": consumo_total,
                    "importe": precio_total
                })
                producer.flush()
                print(f"[ENGINE {cp_id}] 🔋 +0.5 kWh -> Total {consumo_total} kWh / {precio_total} €")
            except Exception as e:
                print(f"[ENGINE {cp_id}] ❌ Error enviando consumo a CENTRAL: {e}")
            time.sleep(1)
        
        # CORRECCIÓN ERROR 2: Si se averió durante suministro, finalizar inmediatamente
        if not health_ok and en_suministro:
            print(f"[ENGINE {cp_id}] ⚠️ AVERÍA DETECTADA DURANTE SUMINISTRO!")
            stop_supply_emergencia()

    # ==========================================================
    # Terminar suministro normal
    # ==========================================================
    def stop_supply():
        nonlocal en_suministro, estado, estado_real, autorizado, consumo_total, precio_total
        if not en_suministro:
            print("[ENGINE] No hay suministro activo.")
            return
        en_suministro = False
        estado = "ACTIVADO"
        estado_real = "ACTIVADO"
        hora_fin = datetime.datetime.now().isoformat()
        ticket = {
            "energia": round(consumo_total, 2),
            "precio_total": round(precio_total, 2),
            "hora_inicio": hora_inicio,
            "hora_fin": hora_fin,
            "idCP": cp_id,
            "idDriver": driver_id
        }
        try:
            producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": estado})
            producer.flush()
            print(f"[ENGINE {cp_id}] ✅ Suministro finalizado. Ticket enviado: {ticket}")
        except Exception as e:
            print(f"[ENGINE {cp_id}] ❌ Error enviando ticket a CENTRAL: {e}")
        autorizado = False

    # ==========================================================
    # CORRECCIÓN ERROR 2: Terminar suministro por emergencia (avería)
    # ==========================================================
    def stop_supply_emergencia():
        nonlocal en_suministro, estado, estado_real, autorizado, consumo_total, precio_total
        if not en_suministro:
            return
        en_suministro = False
        estado = "AVERIADO"
        estado_real = "AVERIADO"
        hora_fin = datetime.datetime.now().isoformat()
        ticket = {
            "energia": round(consumo_total, 2),
            "precio_total": round(precio_total, 2),
            "hora_inicio": hora_inicio,
            "hora_fin": hora_fin,
            "idCP": cp_id,
            "idDriver": driver_id,
            "motivo": "INTERRUMPIDO_POR_AVERIA"
        }
        try:
            producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": estado})
            producer.flush()
            print(f"[ENGINE {cp_id}] ⚠️ SUMINISTRO INTERRUMPIDO POR AVERÍA! Ticket enviado: {ticket}")
        except Exception as e:
            print(f"[ENGINE {cp_id}] ❌ Error enviando ticket de emergencia a CENTRAL: {e}")
        autorizado = False

    # ==========================================================
    # Menú interactivo
    # ==========================================================
    def menu_thread():
        nonlocal health_ok, estado, estado_real, autorizado, driver_id, en_suministro, menu_activo
        
        while menu_activo:
            print(f"\n--- MENÚ ENGINE (Estado actual: {estado_real}) ---")
            print("1 - Simular AVERÍA")
            print("2 - Simular REPARACIÓN") 
            print("3 - Petición de SUMINISTRO (introducir ID de driver)")
            print("4 - Empezar SUMINISTRO (si está autorizado)")
            print("5 - Terminar SUMINISTRO (envía ticket)")
            print("6 - Salir del menú")
            
            try:
                choice = input("Seleccione opción: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaliendo del menú...")
                break
            
            if choice == "1":
                health_ok = False
                estado_anterior = estado_real
                estado = "AVERIADO"
                estado_real = "AVERIADO"
                try:
                    producer.send(CP_STATUS, {"idCP": cp_id, "estado": estado})
                    producer.flush()
                    print(f"[ENGINE {cp_id}] ⚠️ Simulando avería.")
                    
                    # CORRECCIÓN ERROR 2: Si estaba suministrando, parar inmediatamente
                    if estado_anterior == "SUMINISTRANDO":
                        print(f"[ENGINE {cp_id}] ⚠️ AVERÍA DETECTADA DURANTE SUMINISTRO!")
                        stop_supply_emergencia()
                        
                except Exception as e:
                    print(f"[ENGINE {cp_id}] ❌ Error de conexión con CENTRAL: {e}")
                
            elif choice == "2":
                health_ok = True
                estado = "ACTIVADO"
                estado_real = "ACTIVADO" 
                try:
                    producer.send(CP_STATUS, {"idCP": cp_id, "estado": estado})
                    producer.flush()
                    print(f"[ENGINE {cp_id}] ✅ Reparación completada. Estado: ACTIVADO")
                except Exception as e:
                    print(f"[ENGINE {cp_id}] ❌ Error de conexión con CENTRAL: {e}")
                    
            elif choice == "3":
                if estado_real == "AVERIADO":
                    print(f"[ENGINE {cp_id}] ❌ No se puede solicitar suministro: CP en estado AVERIADO")
                    continue
                    
                driver_id = input("Introduce ID del driver: ").strip()
                if not driver_id:
                    print("ID inválido.")
                    continue
                print(f"[ENGINE {cp_id}] Enviando petición de suministro para driver {driver_id}")
                try:
                    producer.send(CHARGING_REQUESTS, {"idCP": cp_id, "idDriver": driver_id})
                    producer.flush()
                    print(f"[ENGINE {cp_id}] ✅ Petición enviada correctamente")
                except Exception as e:
                    print(f"[ENGINE {cp_id}] ❌ Error enviando petición: {e}")
                    
            elif choice == "4":
                if estado_real == "AVERIADO":
                    print(f"[ENGINE {cp_id}] ❌ No se puede suministrar: CP en estado AVERIADO")
                elif autorizado:
                    threading.Thread(target=start_supply, daemon=True).start()
                else:
                    print(f"[ENGINE {cp_id}] ❌ No hay autorización para suministrar")
                    
            elif choice == "5":
                stop_supply()
                
            elif choice == "6":
                print("Saliendo del menú...")
                menu_activo = False
                break
                
            else:
                print("Opción no válida.")

    menu_thread_instance = threading.Thread(target=menu_thread, daemon=True)
    menu_thread_instance.start()

    print(f"[ENGINE {cp_id}] Esperando comandos de CENTRAL... (menú interactivo activo)")
    try:
        while menu_activo:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"[ENGINE {cp_id}] Terminando por interrupción del usuario.")
    finally:
        menu_activo = False
        print(f"[ENGINE {cp_id}] Programa finalizado.")

if __name__ == "__main__":
    main()
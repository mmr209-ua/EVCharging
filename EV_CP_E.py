# EV_CP_E.py - VERSIÓN CORREGIDA (SINCRONIZACIÓN)
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
        print("Uso: python EV_CP_E.py <broker_ip:puerto> <cp_id> <listen_port>")
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
    estado_real = "ACTIVADO"
    
    # Lock para sincronización entre hilos
    lock = threading.Lock()

    # ==========================================================
    # Servidor TCP para el monitor (health check)
    # ==========================================================
    def health_server():
        """
        Servidor TCP que escucha PING del Monitor y responde PONG o KO.
        """
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
    # Procesar mensajes de autorización (CENTRAL -> CP_AUTHORIZE_SUPPLY) - MEJORADO
    # ==========================================================
    def consume_authorize_loop():
        """
        Hilo que escucha autorizaciones de CENTRAL por Kafka.
        """
        nonlocal autorizado, driver_id, estado_real
        for msg in consumer_authorize:
            event = msg.value
            if str(event.get("idCP")) != cp_id:
                continue
            
            action = event.get("action")
            
            # MEJORADO: Manejar tanto autorizaciones como rechazos
            if action == "authorize":
                # Solo autorizar si está ACTIVADO y NO está en suministro
                with lock:
                    if estado_real == "ACTIVADO" and not en_suministro:
                        autorizado = True
                        driver_id = event.get("idDriver")
                        print(f"\n[ENGINE {cp_id}] ✅ AUTORIZACIÓN CONCEDIDA para Driver {driver_id}")
                        print(f"[ENGINE {cp_id}] 💡 Use la opción 4 del menú para INICIAR suministro")
                        print(f"[ENGINE {cp_id}] 📞 Driver {driver_id} está esperando en su terminal...")
                    elif estado_real == "AVERIADO":
                        print(f"[ENGINE {cp_id}] ❌ Autorización rechazada: CP en estado AVERIADO")
                    elif estado_real == "SUMINISTRANDO" or en_suministro:
                        print(f"[ENGINE {cp_id}] ❌ Autorización rechazada: Ya está SUMINISTRANDO")
                    else:
                        print(f"[ENGINE {cp_id}] ❌ Autorización rechazada: Estado {estado_real}")
                        
            elif action == "reject":
                # MEJORADO: Recibir y mostrar rechazos explícitamente
                motivo = event.get("motivo", "DESCONOCIDO")
                mensaje = event.get("mensaje", "Solicitud rechazada")
                driver_rechazado = event.get("idDriver")
                
                print(f"\n[ENGINE {cp_id}] ❌ AUTORIZACIÓN RECHAZADA para Driver {driver_rechazado}")
                print(f"[ENGINE {cp_id}] 📋 Motivo: {motivo}")
                print(f"[ENGINE {cp_id}] 💡 {mensaje}")
                
                # Resetear estado de autorización
                with lock:
                    if driver_id == driver_rechazado:
                        autorizado = False
                        driver_id = None

    threading.Thread(target=consume_authorize_loop, daemon=True).start()

    # ==========================================================
    # Simular suministro (envío periódico a CENTRAL)
    # ==========================================================
    def start_supply():
        """
        Inicia el suministro enviando consumo cada segundo a CENTRAL.
        """
        nonlocal en_suministro, consumo_total, precio_total, hora_inicio, estado, estado_real, driver_id
        
        # Verificar que se puede iniciar suministro
        with lock:
            if not autorizado or driver_id is None:
                print("[ENGINE] ❌ No hay autorización válida.")
                return
            if estado_real == "AVERIADO":
                print(f"[ENGINE] ❌ No puede suministrar: CP en estado AVERIADO")
                return
            if en_suministro:
                print(f"[ENGINE] ❌ Ya hay un suministro en curso")
                return
            if estado_real != "ACTIVADO":
                print(f"[ENGINE] ⚠️ No puede suministrar: estado {estado_real}")
                return

            print(f"[ENGINE {cp_id}] ⛽ INICIANDO SUMINISTRO para Driver {driver_id}...")
            en_suministro = True
            estado = "SUMINISTRANDO"
            estado_real = "SUMINISTRANDO"
            hora_inicio = datetime.datetime.now().isoformat()
            consumo_total = 0.0
            precio_total = 0.0
        
        try:
            # Notificar cambio de estado a CENTRAL (aunque ya debería estar en SUMINISTRANDO)
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "SUMINISTRANDO"})
            producer.flush()
            
            # Notificar al driver que el suministro ha comenzado
            producer.send(DRIVER_SUPPLY_COMPLETE, {
                "idCP": cp_id,
                "ticket": {
                    "idDriver": driver_id,
                    "estado": "SUMINISTRANDO",
                    "mensaje": f"Suministro iniciado en CP {cp_id}",
                    "consumo_actual": 0.0,
                    "importe_actual": 0.0
                }
            })
            producer.flush()
            
        except Exception as e:
            print(f"[ENGINE {cp_id}] ❌ Error enviando estado a CENTRAL: {e}")

        # Bucle de suministro
        while True:
            with lock:
                if not en_suministro or not health_ok:
                    break
                
                consumo_total += 0.5
                precio_total = round(consumo_total * 0.25, 2)
            
            try:
                # Enviar consumo a CENTRAL
                producer.send(CP_CONSUMPTION, {
                    "idCP": cp_id,
                    "consumo": consumo_total,
                    "importe": precio_total
                })
                
                # Enviar actualización al driver también
                producer.send(DRIVER_SUPPLY_COMPLETE, {
                    "idCP": cp_id,
                    "ticket": {
                        "idDriver": driver_id,
                        "estado": "EN_PROGRESO",
                        "consumo_actual": consumo_total,
                        "importe_actual": precio_total
                    }
                })
                producer.flush()
                
                print(f"[ENGINE {cp_id}] 🔋 +0.5 kWh -> Total {consumo_total} kWh / {precio_total} €")
            except Exception as e:
                print(f"[ENGINE {cp_id}] ❌ Error enviando consumo: {e}")
            time.sleep(1)
        
        # Verificar si fue por avería
        with lock:
            if not health_ok and en_suministro:
                print(f"[ENGINE {cp_id}] ⚠️ AVERÍA DETECTADA DURANTE SUMINISTRO!")
                stop_supply_emergencia()

    # ==========================================================
    # Terminar suministro normal - CORRECCIÓN CRÍTICA
    # ==========================================================
    def stop_supply():
        """
        Finaliza el suministro normalmente y envía ticket completo.
        """
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
            estado_real = "ACTIVADO"  # CORRECCIÓN: Cambiar estado real a ACTIVADO
            hora_fin = datetime.datetime.now().isoformat()
            
            # Guardar variables localmente
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
            # CORRECCIÓN CRÍTICA: Notificar a Central el cambio de estado a ACTIVADO
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
            producer.flush()
            
            producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
            producer.flush()
            print(f"[ENGINE {cp_id}] ✅ SUMINISTRO FINALIZADO. Ticket enviado a Driver {current_driver_id}")
            print(f"    Energía: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} €")
            print(f"[ENGINE {cp_id}] 🔄 Estado cambiado a ACTIVADO y notificado a CENTRAL")
        except Exception as e:
            print(f"[ENGINE {cp_id}] ❌ Error enviando ticket a CENTRAL: {e}")
        
        # Resetear variables
        with lock:
            autorizado = False
            driver_id = None
            consumo_total = 0.0
            precio_total = 0.0

    # ==========================================================
    # Terminar suministro por emergencia (avería) con consumo - CORREGIDO
    # ==========================================================
    def stop_supply_emergencia():
        """
        Finaliza el suministro por avería y envía ticket parcial.
        """
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
            
            # Guardar consumo e importe para el ticket
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
                "motivo": "INTERRUMPIDO_POR_AVERIA",
                "estado": "INTERRUMPIDO",
                "mensaje": f"Suministro interrumpido por avería. Consumo hasta el momento: {current_consumo_total} kWh / {current_precio_total} €"
            }
        
        try:
            # CORRECCIÓN: Notificar estado AVERIADO a Central
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "AVERIADO"})
            producer.flush()
            
            producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
            producer.flush()
            print(f"[ENGINE {cp_id}] ⚠️ SUMINISTRO INTERRUMPIDO POR AVERÍA!")
            print(f"    Consumo hasta el corte: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} €")
            print(f"    Ticket de emergencia enviado a Driver {current_driver_id}")
            print(f"[ENGINE {cp_id}] 🔄 Estado cambiado a AVERIADO y notificado a CENTRAL")
        except Exception as e:
            print(f"[ENGINE {cp_id}] ❌ Error enviando ticket de emergencia a CENTRAL: {e}")
        
        # Resetear variables
        with lock:
            autorizado = False
            driver_id = None
            consumo_total = 0.0
            precio_total = 0.0

    # ==========================================================
    # Menú interactivo - MEJORADO
    # ==========================================================
    def menu_thread():
        """
        Menú principal del Engine para simular operaciones.
        """
        nonlocal health_ok, estado, estado_real, autorizado, driver_id, en_suministro, menu_activo
        
        while menu_activo:
            # ACTUALIZAR VARIABLES ANTES DE MOSTRAR MENÚ
            with lock:
                current_estado = estado_real
                current_autorizado = autorizado
                current_en_suministro = en_suministro
                current_driver_id = driver_id
            
            print(f"\n--- MENÚ ENGINE {cp_id} ---")
            if current_autorizado and not current_en_suministro:
                print(f"   💡 Driver {current_driver_id} AUTORIZADO - Use opción 4")
                print(f"   📞 Driver está esperando en su terminal...")
            elif current_en_suministro:
                print(f"   🔋 SUMINISTRANDO a Driver {current_driver_id} - Use opción 5 para finalizar")
            
            print("1 - Simular AVERÍA")
            print("2 - Simular REPARACIÓN") 
            print("3 - Petición de SUMINISTRO desde CP (introducir ID de driver)")
            print("4 - Empezar SUMINISTRO (si está autorizado)")
            print("5 - Terminar SUMINISTRO (envía ticket)")
            print("6 - Estado actual de la BD (debug)")
            print("7 - Salir del menú")
            print(f"Estado actual: {current_estado}")
            
            try:
                choice = input("Seleccione opción: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaliendo del menú...")
                break
            
            if choice == "1":
                # OPCIÓN 1: Simular AVERÍA (NO USA KAFKA)
                with lock:
                    estado_anterior = estado_real
                    health_ok = False
                    estado = "AVERIADO"
                    estado_real = "AVERIADO"
                
                print(f"[ENGINE {cp_id}] ⚠️ Simulando avería.")
                print(f"[ENGINE {cp_id}] 🔴 Monitor detectará la avería y notificará a CENTRAL vía socket")
                
                # Si está suministrando, interrumpir
                if estado_anterior == "SUMINISTRANDO":
                    print(f"[ENGINE {cp_id}] ⚠️ AVERÍA DETECTADA DURANTE SUMINISTRO!")
                    stop_supply_emergencia()
                
            elif choice == "2":
                # OPCIÓN 2: Simular REPARACIÓN (NO USA KAFKA)
                with lock:
                    # Solo reparar si está averiado
                    if estado_real != "AVERIADO":
                        print(f"[ENGINE {cp_id}] ⚠️ No necesita reparación: estado actual {estado_real}")
                        continue
                        
                    health_ok = True
                    estado = "ACTIVADO"
                    estado_real = "ACTIVADO"
                
                print(f"[ENGINE {cp_id}] ✅ Reparación completada. Estado: ACTIVADO")
                print(f"[ENGINE {cp_id}] 🟢 Monitor detectará la recuperación y notificará a CENTRAL vía socket")
                    
            elif choice == "3":
                # OPCIÓN 3: Petición desde el propio CP (origen=engine)
                with lock:
                    if estado_real == "AVERIADO":
                        print(f"[ENGINE {cp_id}] ❌ No se puede solicitar suministro: CP en estado AVERIADO")
                        continue
                    if estado_real == "SUMINISTRANDO":
                        print(f"[ENGINE {cp_id}] ❌ No se puede solicitar suministro: Ya está SUMINISTRANDO")
                        continue
                    if estado_real == "PARADO":
                        print(f"[ENGINE {cp_id}] ❌ No se puede solicitar suministro: CP está PARADO")
                        continue
                    
                driver_id_input = input("Introduce ID del driver: ").strip()
                if not driver_id_input:
                    print("ID inválido.")
                    continue
                    
                print(f"[ENGINE {cp_id}] Enviando petición de suministro para driver {driver_id_input} (desde CP)")
                try:
                    # Marcar origen como 'engine' para que CENTRAL no envíe notificación al driver
                    #producer.send(CHARGING_REQUESTS, {
                    #    "idCP": cp_id, 
                    #    "idDriver": driver_id_input,
                    #    "origen": "engine"
                    #})
                    producer.send(CHARGING_REQUESTS, {
                        "idCP": cp_id, 
                        "idDriver": driver_id_input
                    })

                    producer.flush()
                    print(f"[ENGINE {cp_id}] ✅ Petición enviada correctamente")
                    print(f"[ENGINE {cp_id}] ⏳ Esperando respuesta de CENTRAL...")
                except Exception as e:
                    print(f"[ENGINE {cp_id}] ❌ Error enviando petición: {e}")
                    
            elif choice == "4":
                # OPCIÓN 4: Empezar SUMINISTRO - MEJORADO
                with lock:
                    current_estado = estado_real
                    current_autorizado = autorizado
                    current_en_suministro = en_suministro
                    current_driver_id = driver_id
                
                if current_estado == "AVERIADO":
                    print(f"[ENGINE {cp_id}] ❌ No se puede suministrar: CP en estado AVERIADO")
                elif current_estado == "PARADO":
                    print(f"[ENGINE {cp_id}] ❌ No se puede suministrar: CP está PARADO")
                elif current_autorizado and not current_en_suministro:
                    # INICIAR INMEDIATAMENTE en segundo plano
                    supply_thread = threading.Thread(target=start_supply, daemon=True)
                    supply_thread.start()
                    print(f"[ENGINE {cp_id}] 🚀 Iniciando suministro en segundo plano...")
                    # Actualizar estado visualmente
                    with lock:
                        en_suministro = True
                elif current_en_suministro:
                    print(f"[ENGINE {cp_id}] ⚠️ Ya está suministrando a Driver {current_driver_id}")
                else:
                    print(f"[ENGINE {cp_id}] ❌ No hay autorización para suministrar")
                    print(f"[ENGINE {cp_id}] 💡 Use la opción 3 para solicitar autorización primero")
                    
            elif choice == "5":
                # OPCIÓN 5: Terminar SUMINISTRO
                stop_supply()
                
            elif choice == "6":
                # OPCIÓN 6: Debug - mostrar estado interno
                with lock:
                    print(f"\n[ENGINE {cp_id}] 🔍 ESTADO INTERNO:")
                    print(f"   Estado real: {estado_real}")
                    print(f"   Autorizado: {autorizado}")
                    print(f"   En suministro: {en_suministro}")
                    print(f"   Driver ID: {driver_id}")
                    print(f"   Health OK: {health_ok}")
                    print(f"   Consumo actual: {consumo_total} kWh")
                    print(f"   Precio actual: {precio_total} €")
                
            elif choice == "7":
                # OPCIÓN 7: Salir
                print("Saliendo del menú...")
                menu_activo = False
                break
                
            else:
                print("Opción no válida.")

    menu_thread_instance = threading.Thread(target=menu_thread, daemon=True)
    menu_thread_instance.start()

    print(f"[ENGINE {cp_id}] Sistema iniciado. Esperando autorizaciones y comandos...")
    print(f"[ENGINE {cp_id}] Estados posibles: ACTIVADO, PARADO, SUMINISTRANDO, AVERIADO")
    
    try:
        while menu_activo:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"[ENGINE {cp_id}] Terminando por interrupción del usuario.")
    finally:
        menu_activo = False
        print(f"[ENGINE {cp_id}] Programa finalizado.")

if _name_ == "_main_":
    main()
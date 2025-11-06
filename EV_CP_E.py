# EV_CP_E.py - VERSIÓN EXTENDIDA CON CONTROL CENTRAL (PARAR / REANUDAR)
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

    # PRODUCTOR KAFKA
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # CONSUMIDORES KAFKA
    # Autorizaciones de CENTRAL -> CP
    consumer_authorize = KafkaConsumer(
        AUTHORIZE_SUPPLY,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_engine_{cp_id}",
        auto_offset_reset='latest',        
        enable_auto_commit=True
    )

    # Órdenes PARAR/REANUDAR desde CENTRAL
    consumer_control = KafkaConsumer(
        CP_CONTROL,
        bootstrap_servers=broker,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"cp_control_{cp_id}",
        auto_offset_reset='latest',        
        enable_auto_commit=True
    )

    # ESTADO INTERNO DEL CP 
    estado = "ACTIVADO"          # estado lógico mostrado en la central
    estado_real = "ACTIVADO"     # estado operativo usado para decidir si se puede cargar
    health_ok = True             # salud física (monitor)

    en_suministro = False        # hay carga en curso?
    autorizado = False           # central ha autorizado a un driver concreto?
    driver_id = None             # driver actual autorizado / en carga

    consumo_total = 0.0          # energía acumulada en la sesión activa
    precio_total = 0.0           
    hora_inicio = None

    menu_activo = True

    # Lock para sincronización entre hilos
    lock = threading.Lock()

    # Servidor TCP para el monitor
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

    # FUNCIONES DE SUMINISTRO
    def start_supply():
        """
        Inicia el suministro enviando consumo cada segundo a CENTRAL.
        """
        nonlocal en_suministro, consumo_total, precio_total, hora_inicio, estado, estado_real, driver_id
        
        with lock:
            if not autorizado or driver_id is None:
                print("[ENGINE] No hay autorización válida.")
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
            # Notificar cambio de estado a CENTRAL
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "SUMINISTRANDO"})
            producer.flush()            
        except Exception as e:
            print(f"[ENGINE {cp_id}] Error enviando estado a CENTRAL: {e}")

        # Bucle de suministro (envío en tiempo real cada 1s)
        while True:
            with lock:
                if not en_suministro or not health_ok:
                    break

                # Simulación consumo incremental
                consumo_total += 0.5  # kWh simulados / segundo
                precio_total = round(consumo_total * 0.25, 2)

                consumo_actual_envio = consumo_total
                precio_actual_envio = precio_total
                driver_actual_envio = driver_id

            try:
                # Mandar consumo acumulado a CENTRAL y a DRIVER
                producer.send(CP_CONSUMPTION, {
                    "idCP": cp_id,
                    "consumo": consumo_actual_envio,
                    "importe": precio_actual_envio,
                    "conductor": driver_actual_envio
                })

                producer.flush()

                print(f"[ENGINE {cp_id}] +0.5 kWh -> Total {consumo_actual_envio} kWh / {precio_actual_envio} €")

            except Exception as e:
                print(f"[ENGINE {cp_id}] Error enviando consumo en tiempo real: {e}")

            time.sleep(1)

        # Si hemos salido porque health_ok se puso False (avería física durante la carga),
        # cerramos emergencia:
        debo_cortar = False
        with lock:
            if not health_ok and en_suministro:
                debo_cortar = True
                print(f"[ENGINE {cp_id}] AVERÍA DETECTADA DURANTE SUMINISTRO!")
                ###stop_supply_emergencia()
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
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
            producer.flush()
            
            producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
            producer.flush()

            print(f"[ENGINE {cp_id}] SUMINISTRO FINALIZADO PARA CONDUCTOR {current_driver_id}")
            print(f"    Energía: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} €")
            print(f"[ENGINE {cp_id}] Estado cambiado a ACTIVADO y notificado a CENTRAL")
        except Exception as e:
            print(f"[ENGINE {cp_id}] Error enviando ticket a CENTRAL: {e}")
        
        with lock:
            autorizado = False
            driver_id = None
            consumo_total = 0.0
            precio_total = 0.0

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
                "mensaje": f"Suministro interrumpido por avería. Consumo hasta el momento: {current_consumo_total} kWh / {current_precio_total} €"
            }
        
        try:
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "AVERIADO"})
            producer.flush()
            
            producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
            producer.flush()

            print(f"[ENGINE {cp_id}] SUMINISTRO INTERRUMPIDO POR AVERÍA!")
            print(f"    Consumo hasta el corte: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} €")
            print(f"[ENGINE {cp_id}] Estado cambiado a AVERIADO y notificado a CENTRAL")
        except Exception as e:
            print(f"[ENGINE {cp_id}] Error enviando ticket de emergencia a CENTRAL: {e}")
        
        with lock:
            autorizado = False
            driver_id = None
            consumo_total = 0.0
            precio_total = 0.0

    def stop_supply_forzado_por_central():
        """
        Finaliza el suministro porque CENTRAL ha dado orden PARAR.
        Similar a stop_supply_emergencia(), pero el CP queda en PARADO
        (fuera de servicio por orden central), no en AVERIADO.
        """
        nonlocal en_suministro, estado, estado_real, autorizado, consumo_total, precio_total, driver_id

        with lock:
            if not en_suministro:
                # No estábamos suministrando, simplemente quedará PARADO más adelante
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
                "mensaje": f"Suministro interrumpido por orden de la central. Consumo hasta el momento: {current_consumo_total} kWh / {current_precio_total} €"
            }

        try:
            # Informar a CENTRAL que quedamos PARADO
            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "PARADO"})
            producer.flush()
            print("[DEB] Mandado PARADO a CP_STATUS")

            # Enviar ticket parcial igual que en emergencia
            producer.send(CP_SUPPLY_COMPLETE, {"idCP": cp_id, "ticket": ticket})
            producer.flush()
            print("[DEB] Mandado ticket a CP_SUPPLY_COMPLETE")

            print(f"[ENGINE {cp_id}] SUMINISTRO CORTADO POR ORDEN DE CENTRAL!")
            print(f"    Consumo hasta el corte: {current_consumo_total} kWh")
            print(f"    Importe: {current_precio_total} €")
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
        """
        Hilo que escucha autorizaciones de CENTRAL por Kafka.
        """
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
                        print(f"\n[ENGINE {cp_id}] AUTORIZACIÓN CONCEDIDA para Driver {driver_id}")
                        print(f"[ENGINE {cp_id}] Use la opción 4 del menú para INICIAR suministro")
                        print(f"[ENGINE {cp_id}] Driver {driver_id} está esperando en su terminal...")
                    elif estado_real == "AVERIADO":
                        print(f"[ENGINE {cp_id}] Autorización rechazada: CP en estado AVERIADO")
                    elif estado_real == "PARADO":
                        print(f"[ENGINE {cp_id}] Autorización rechazada: CP en estado PARADO (orden CENTRAL)")
                    elif estado_real == "SUMINISTRANDO" or en_suministro:
                        print(f"[ENGINE {cp_id}] Autorización rechazada: Ya está SUMINISTRANDO")
                    else:
                        print(f"[ENGINE {cp_id}] Autorización rechazada: Estado {estado_real}")
                        
            elif authorize == "NO":
                motivo = event.get("motivo", "DESCONOCIDO")
                mensaje = event.get("mensaje", "Solicitud rechazada")
                driver_rechazado = event.get("idDriver")
                
                print(f"\n[ENGINE {cp_id}] AUTORIZACIÓN RECHAZADA para Driver {driver_rechazado}")
                print(f"[ENGINE {cp_id}] Motivo: {motivo}")
                print(f"[ENGINE {cp_id}] {mensaje}")
                
                with lock:
                    if driver_id == driver_rechazado:
                        autorizado = False
                        driver_id = None

    threading.Thread(target=consume_authorize_loop, daemon=True).start()

    # CONSUMIR ÓRDENES DE CONTROL DESDE CENTRAL (PARAR / REANUDAR)
    def consume_control_loop():
        """
        Hilo que escucha órdenes CP_CONTROL de CENTRAL.
        Acciones: PARAR, REANUDAR.
        """
        nonlocal estado, estado_real, en_suministro, health_ok, autorizado, driver_id

        for msg in consumer_control:
            event = msg.value
            accion = event.get("accion")
            target = str(event.get("idCP"))

            # Ignorar órdenes que no van dirigidas a mí ni a "todos"
            if target != cp_id and target != "todos":
                continue

            # ORDEN PARAR
            if accion == "PARAR":
                ejecutar_corte = False
                with lock:
                    print(f"\n[ENGINE {cp_id}] ORDEN CENTRAL: PARAR")

                    if en_suministro:
                        ejecutar_corte = True
                        print(f"[ENGINE {cp_id}] Cortando suministro activo por orden CENTRAL...")

                    else:
                        # No estábamos suministrando: sólo pasamos a PARADO aquí
                        estado = "PARADO"
                        estado_real = "PARADO"
                        autorizado = False
                        driver_id = None
                if ejecutar_corte:
                    stop_supply_forzado_por_central()
                    # y ahora avisamos a Central de que este CP está PARADO
                    producer.send(CP_STATUS, {"idCP": cp_id, "estado": "PARADO"})
                    producer.flush()        

                # No tocamos health_ok. No es avería física.

            # ORDEN REANUDAR
            elif accion == "REANUDAR":
                with lock:
                    print(f"\n[ENGINE {cp_id}] ORDEN CENTRAL: REANUDAR")

                    # Si está averiado físicamente, no podemos volver a ACTIVADO
                    if estado_real == "AVERIADO" or not health_ok:
                        print(f"[ENGINE {cp_id}] No puedo reanudar: sigo en AVERIADO físico")
                    else:
                        # Volvemos a estar operativos
                        en_suministro = False
                        autorizado = False
                        driver_id = None
                        estado = "ACTIVADO"
                        estado_real = "ACTIVADO"

                        try:
                            producer.send(CP_STATUS, {"idCP": cp_id, "estado": "ACTIVADO"})
                            producer.flush()
                            print(f"[ENGINE {cp_id}] Estado reanudado a ACTIVADO, notificado a CENTRAL")
                        except Exception as e:
                            print(f"[ENGINE {cp_id}] Error notificando ACTIVADO a CENTRAL: {e}")

    threading.Thread(target=consume_control_loop, daemon=True).start()

    def menu_thread():
        nonlocal health_ok, estado, estado_real, autorizado, driver_id, en_suministro, menu_activo
        
        while menu_activo:
            with lock:
                current_estado = estado_real
                current_autorizado = autorizado
                current_en_suministro = en_suministro
                current_driver_id = driver_id
            
            print(f"\n--- MENÚ ENGINE {cp_id} ---")
            if current_autorizado and not current_en_suministro:
                print(f" Driver {current_driver_id} AUTORIZADO - Use opción 4")
                print(f" Driver está esperando en su terminal...")
            elif current_en_suministro:
                print(f" SUMINISTRANDO a Driver {current_driver_id} - Use opción 5 para finalizar")
            
            print("1 - Simular AVERÍA (física)")
            print("2 - Simular REPARACIÓN")
            print("3 - Petición de SUMINISTRO desde CP (introducir ID de driver)")
            print("4 - Empezar SUMINISTRO (si está autorizado)")
            print("5 - Terminar SUMINISTRO (envía ticket)")
            print("6 - Estado interno (debug)")
            print("7 - Salir del menú")
            print(f"Estado actual: {current_estado}")
            
            try:
                choice = input("Seleccione opción: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaliendo del menú...")
                break
            
            if choice == "1":
                # Simular avería física local
                with lock:
                    estado_anterior = estado_real
                    health_ok = False
                    estado = "AVERIADO"
                    estado_real = "AVERIADO"
                
                print(f"[ENGINE {cp_id}] Simulando avería física.")
                print(f"[ENGINE {cp_id}] Monitor detectará la avería y notificará a CENTRAL vía socket")
                
                if estado_anterior == "SUMINISTRANDO":
                    print(f"[ENGINE {cp_id}] AVERÍA DURANTE SUMINISTRO!")
                    stop_supply_emergencia()
                
            elif choice == "2":
                # Reparación física local
                with lock:
                    if estado_real != "AVERIADO":
                        print(f"[ENGINE {cp_id}] No necesita reparación: estado actual {estado_real}")
                        continue
                    health_ok = True
                    estado = "ACTIVADO"
                    estado_real = "ACTIVADO"
                
                print(f"[ENGINE {cp_id}] Reparación completada. Estado: ACTIVADO")
                print(f"[ENGINE {cp_id}] Monitor notificará recuperación a CENTRAL vía socket")
                    
            elif choice == "3":
                with lock:
                    if estado_real in ("AVERIADO", "PARADO"):
                        print(f"[ENGINE {cp_id}] No se puede solicitar suministro: CP en estado {estado_real}")
                        continue
                    if estado_real == "SUMINISTRANDO":
                        print(f"[ENGINE {cp_id}] Ya está SUMINISTRANDO")
                        continue
                    
                driver_id_input = input("Introduce ID del driver: ").strip()
                if not driver_id_input:
                    print("ID inválido.")
                    continue
                    
                print(f"[ENGINE {cp_id}] Enviando petición de suministro para driver {driver_id_input} (desde CP)")
                try:
                    producer.send(SUPPLY_REQUEST_TO_CENTRAL, {
                        "idCP": cp_id, 
                        "idDriver": driver_id_input
                    })
                    producer.flush()
                    print(f"[ENGINE {cp_id}] Petición enviada. Esperando autorización de CENTRAL...")
                except Exception as e:
                    print(f"[ENGINE {cp_id}] Error enviando petición: {e}")
                    
            elif choice == "4":
                # Empezar suministro en segundo plano
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
                    print(f"[ENGINE {cp_id}] Ya está suministrando a Driver {current_drv}")
                else:
                    print(f"[ENGINE {cp_id}] No hay autorización para suministrar")
                    print(f"[ENGINE {cp_id}] Use la opción 3 para solicitar autorización primero")
                    
            elif choice == "5":
                # Terminar SUMINISTRO voluntariamente
                stop_supply()
                
            elif choice == "6":
                # Debug estado interno
                with lock:
                    print(f"\n[ENGINE {cp_id}] ESTADO INTERNO:")
                    print(f"   Estado real: {estado_real}")
                    print(f"   Autorizado: {autorizado}")
                    print(f"   En suministro: {en_suministro}")
                    print(f"   Driver ID: {driver_id}")
                    print(f"   Health OK: {health_ok}")
                    print(f"   Consumo actual: {consumo_total} kWh")
                    print(f"   Precio actual: {precio_total} €")
                
            elif choice == "7":
                print("Saliendo del menú...")
                menu_activo = False
                break
            else:
                print("Opción no válida.")

    menu_thread_instance = threading.Thread(target=menu_thread, daemon=True)
    menu_thread_instance.start()

    print(f"[ENGINE {cp_id}] Sistema iniciado. Esperando autorizaciones, órdenes de CENTRAL y comandos locales...")
    print(f"[ENGINE {cp_id}] Estados posibles: ACTIVADO, PARADO, SUMINISTRANDO, AVERIADO")
    
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
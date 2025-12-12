# EV_CP_M.py - Release 2 con registro HTTPS y autenticacion
import sys
import json
import time
import socket
import threading
import os
import urllib3

# Desactivar warnings de SSL para certificados autofirmados
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Archivo de configuracion local del CP
CONFIG_FILE = "cp_config.json"

def load_config():
    """Carga la configuracion del CP desde archivo."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_config(config):
    """Guarda la configuracion del CP en archivo."""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

def main():
    if len(sys.argv) < 6:
        print("Uso: python EV_CP_M.py <cp_id> <engine_ip> <engine_port> <central_ip> <central_port> [registry_url]")
        sys.exit(1)

    cp_id = str(sys.argv[1])
    engine_ip = sys.argv[2]
    engine_port = int(sys.argv[3])
    central_ip = sys.argv[4]
    central_port = int(sys.argv[5])
    registry_url = sys.argv[6] if len(sys.argv) > 6 else "https://localhost:5001"

    # Cargar configuracion existente
    config = load_config()
    if config.get("idCP") != cp_id:
        config = {"idCP": cp_id}

    # Conexion continua con CENTRAL por TCP
    def connect_to_central():
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((central_ip, central_port))
                print(f"[CP_MONITOR {cp_id}] Conectado a CENTRAL ({central_ip}:{central_port})")
                return s
            except socket.timeout:
                print(f"[CP_MONITOR {cp_id}] Timeout conectando a CENTRAL, reintentando...")
            except ConnectionRefusedError:
                print(f"[CP_MONITOR {cp_id}] CENTRAL no disponible, reintentando en 3 segundos...")
            except Exception as e:
                print(f"[CP_MONITOR {cp_id}] Error conectando a CENTRAL: {e}, reintentando...")
            time.sleep(3)

    def send_to_central(msg):
        nonlocal central_socket
        max_retries = 3
        for attempt in range(max_retries):
            try:
                central_socket.sendall((json.dumps(msg) + "\n").encode("utf-8"))
                return True
            except Exception as e:
                print(f"[CP_MONITOR {cp_id}] Error enviando a CENTRAL (intento {attempt+1}/{max_retries}): {e}")
                try:
                    central_socket.close()
                except:
                    pass
                central_socket = connect_to_central()
                if attempt == max_retries - 1:
                    print(f"[CP_MONITOR {cp_id}] No se pudo enviar mensaje despues de {max_retries} intentos")
                    return False
        return False

    def receive_from_central():
        """Recibe respuesta de Central (para autenticacion)."""
        try:
            central_socket.settimeout(10)
            data = b""
            while True:
                chunk = central_socket.recv(1024)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break
            central_socket.settimeout(5)
            if data:
                return json.loads(data.decode('utf-8').strip())
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Error recibiendo de CENTRAL: {e}")
        return None

    central_socket = connect_to_central()

    # =====================================================
    # FUNCIONES DE REGISTRO Y AUTENTICACION
    # =====================================================

    def registrar_en_registry():
        """Registra el CP en EV_Registry via HTTPS."""
        nonlocal config
        import requests

        precio = input("Precio por kWh (default 0.30): ").strip() or "0.30"
        ubicacion = input("Ubicacion/Ciudad (default Madrid): ").strip() or "Madrid"

        try:
            data = {
                "idCP": cp_id,
                "precio": float(precio),
                "ubicacion": ubicacion
            }
            print(f"[CP_MONITOR {cp_id}] Conectando a Registry: {registry_url}/register")
            response = requests.post(
                f"{registry_url}/register",
                json=data,
                verify=False,  # Aceptar certificados autofirmados
                timeout=10
            )

            result = response.json()

            if not result.get('error'):
                auth_token = result['credenciales']['authToken']
                print(f"[CP_MONITOR {cp_id}] Registro exitoso!")
                print(f"[CP_MONITOR {cp_id}] Token: {auth_token[:16]}...")

                # Guardar en configuracion local
                config["authToken"] = auth_token
                config["precio"] = float(precio)
                config["ubicacion"] = ubicacion
                config["authenticated"] = False
                config["encryption_key"] = None
                save_config(config)

                print(f"[CP_MONITOR {cp_id}] Configuracion guardada en {CONFIG_FILE}")
                return True
            else:
                print(f"[CP_MONITOR {cp_id}] Error: {result.get('message')}")
                return False

        except requests.exceptions.SSLError as e:
            print(f"[CP_MONITOR {cp_id}] Error SSL: {e}")
            print(f"[CP_MONITOR {cp_id}] Asegurese de que Registry esta ejecutandose con HTTPS")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"[CP_MONITOR {cp_id}] Error de conexion: {e}")
            print(f"[CP_MONITOR {cp_id}] Asegurese de que Registry esta ejecutandose en {registry_url}")
            return False
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Error contactando Registry: {e}")
            return False

    def autenticar_en_central():
        """Autentica el CP en Central usando el authToken del Registry."""
        nonlocal config

        auth_token = config.get("authToken")
        if not auth_token:
            print(f"[CP_MONITOR {cp_id}] Error: No hay authToken. Registrese primero (opcion R)")
            return False

        print(f"[CP_MONITOR {cp_id}] Enviando autenticacion a CENTRAL...")

        auth_msg = {
            "type": "authenticate",
            "idCP": cp_id,
            "authToken": auth_token
        }

        if send_to_central(auth_msg):
            # Esperar respuesta con encryption_key
            response = receive_from_central()

            if response and response.get("success"):
                encryption_key = response.get("encryption_key")
                config["encryption_key"] = encryption_key
                config["authenticated"] = True
                save_config(config)

                print(f"[CP_MONITOR {cp_id}] Autenticacion EXITOSA!")
                print(f"[CP_MONITOR {cp_id}] Clave de cifrado recibida y guardada")
                return True
            elif response:
                print(f"[CP_MONITOR {cp_id}] Autenticacion FALLIDA: {response.get('error', 'Error desconocido')}")
                return False
            else:
                print(f"[CP_MONITOR {cp_id}] No se recibio respuesta de CENTRAL")
                return False
        else:
            print(f"[CP_MONITOR {cp_id}] Error enviando autenticacion")
            return False

    def ver_configuracion():
        """Muestra la configuracion actual del CP."""
        print(f"\n[CP_MONITOR {cp_id}] CONFIGURACION ACTUAL:")
        print(f"   idCP: {config.get('idCP', 'N/A')}")
        print(f"   authToken: {config.get('authToken', 'N/A')[:16] + '...' if config.get('authToken') else 'N/A'}")
        print(f"   encryption_key: {'SI' if config.get('encryption_key') else 'NO'}")
        print(f"   authenticated: {config.get('authenticated', False)}")
        print(f"   precio: {config.get('precio', 'N/A')}")
        print(f"   ubicacion: {config.get('ubicacion', 'N/A')}")

    # =====================================================
    # REGISTRO INICIAL EN CENTRAL (si ya esta autenticado)
    # =====================================================

    if config.get("authenticated") and config.get("encryption_key"):
        print(f"[CP_MONITOR {cp_id}] Ya autenticado, registrando en CENTRAL...")
        register_msg = {
            "type": "register",
            "idCP": cp_id,
            "precio": config.get("precio", 0.30),
            "ubicacion": config.get("ubicacion", f"Zona-{cp_id}")
        }
        if send_to_central(register_msg):
            print(f"[CP_MONITOR {cp_id}] Registrado en CENTRAL")
    else:
        print(f"[CP_MONITOR {cp_id}] CP no autenticado. Use el menu para registrarse y autenticarse.")

    # Hilo para mostrar estado cada segundo
    def mostrar_estado_continuo():
        ultimo_estado = None
        try:
            while True:
                ok = False
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(1)
                    s.connect((engine_ip, engine_port))
                    s.sendall(b"PING")
                    data = s.recv(1024).decode().strip()
                    s.close()
                    if data == "PONG":
                        ok = True
                except:
                    ok = False

                estado_actual = "ENGINE CONECTADO" if ok else "ENGINE AVERIADO"
                if estado_actual != ultimo_estado:
                    print(f"[CP_MONITOR {cp_id}] {estado_actual}")
                    ultimo_estado = estado_actual

                time.sleep(1)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Error en hilo de estado: {e}")

    # Iniciar hilo para mostrar estado continuo
    threading.Thread(target=mostrar_estado_continuo, daemon=True).start()

    # Hilo para menu interactivo
    def menu_interactivo():
        nonlocal config
        while True:
            print(f"\n--- MENU CP_MONITOR {cp_id} ---")
            print("R - Registrarse en Registry (HTTPS)")
            print("A - Autenticarse en Central")
            print("C - Ver configuracion")
            print("Q - Salir del menu")

            try:
                opcion = input("Opcion: ").strip().upper()
            except (EOFError, KeyboardInterrupt):
                break

            if opcion == "R":
                registrar_en_registry()
            elif opcion == "A":
                autenticar_en_central()
            elif opcion == "C":
                ver_configuracion()
            elif opcion == "Q":
                print("Saliendo del menu...")
                break

    # Iniciar menu en hilo separado
    threading.Thread(target=menu_interactivo, daemon=True).start()

    # Bucle principal: health check y notificacion a CENTRAL (cada 5 segundos)
    fallo_prev = False
    engine_conectado_prev = False

    try:
        while True:
            # Health check del Engine
            ok = False
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1)
                s.connect((engine_ip, engine_port))
                s.sendall(b"PING")
                data = s.recv(1024).decode().strip()
                s.close()
                if data == "PONG":
                    ok = True
            except:
                ok = False

            # Si no responde el Engine (AVERIA)
            if not ok:
                if not fallo_prev:
                    # Primera vez que falla: enviar alerta
                    alert_msg = {"type": "alert", "idCP": cp_id, "alerta": "ENGINE_NO_RESPONDE"}
                    if send_to_central(alert_msg):
                        print(f"[CP_MONITOR {cp_id}] ENGINE no responde, alerta enviada a CENTRAL")
                    fallo_prev = True

                # Enviar estado AVERIADO
                health_msg = {"type": "health", "idCP": cp_id, "salud": "KO"}
                send_to_central(health_msg)

                if engine_conectado_prev:
                    engine_conectado_prev = False

            # Si el Engine responde OK
            else:
                health_msg = {"type": "health", "idCP": cp_id, "salud": "OK"}
                send_to_central(health_msg)

                if fallo_prev:
                    # Se recupero de una averia
                    print(f"[CP_MONITOR {cp_id}] ENGINE recuperado, notificado a CENTRAL")
                    fallo_prev = False

                if not engine_conectado_prev:
                    engine_conectado_prev = True

            # Esperar 5 segundos antes del siguiente envio a CENTRAL
            time.sleep(5)

    except KeyboardInterrupt:
        print(f"\n[CP_MONITOR {cp_id}] Monitor cerrado por el usuario")
    except Exception as e:
        print(f"[CP_MONITOR {cp_id}] Error: {e}")
    finally:
        # Cerrar socket de Central
        try:
            central_socket.close()
            print(f"[CP_MONITOR {cp_id}] Socket de Central cerrado")
        except:
            pass
        print(f"[CP_MONITOR {cp_id}] Monitor finalizado correctamente")

if __name__ == "__main__":
    main()

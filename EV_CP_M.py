# EV_CP_M.py - Release 2 con registro HTTPS y autenticacion
import sys
import json
import time
import socket
import threading
import os
import urllib3
import requests

# Desactivar warnings de SSL para certificados autofirmados
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Importar modulo de cifrado
try:
    from crypto_utils import encrypt_message
except ImportError:
    print("[CP_MONITOR] ADVERTENCIA: crypto_utils no encontrado, cifrado deshabilitado")
    encrypt_message = None

# Archivo de configuración local del CP
# Contiene los datos de autenticación (de aquí se saca la credencial)
CONFIG_FILE = None

# Carga la configuración del CP
def load_config():
    if not CONFIG_FILE:
        print("[CP_MONITOR] CONFIG_FILE no definido")
        return {}
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"[CP_MONITOR] Error leyendo {CONFIG_FILE}: {e}")
            return {}
    else:
        print(f"[CP_MONITOR] No existe el fichero de configuración {CONFIG_FILE}, se creará al registrarse")
        return {}

# Guarda la configuración del CP en un fichero local
def save_config(config):
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

    # Asegurar que registry_url tiene protocolo https://
    if not registry_url.startswith("http://") and not registry_url.startswith("https://"):
        registry_url = "https://" + registry_url

    # Cargar configuración existente
    global CONFIG_FILE
    CONFIG_FILE = f"cp_config_{cp_id}.json"
    config = load_config()

    # Conexión continua con CENTRAL por TCP
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

    # Mandar mensajitos a central
    def send_to_central(msg):
        nonlocal central_socket, config
        max_retries = 3

        # Recargar configuración para detectar cambios en caliente del token
        config = load_config()

        # Incluir authToken en el mensaje antes de cifrar
        msg_with_token = dict(msg)  # copia para no mutar el original
        msg_with_token["authToken"] = config.get("authToken")

        # Cifrar mensaje si tenemos clave de cifrado
        encryption_key = config.get("encryption_key")
        if encryption_key and encrypt_message:
            try:
                encrypted_data = encrypt_message(encryption_key, msg_with_token)
                # Enviar mensaje cifrado con idCP para que Central pueda identificar la clave
                msg_to_send = {"encrypted": encrypted_data, "idCP": cp_id}
            except Exception as e:
                print(f"[CP_MONITOR {cp_id}] Error cifrando mensaje: {e}")
                msg_to_send = msg_with_token  # Fallback sin cifrar
        else:
            msg_to_send = msg_with_token

        # Intentarlo varias veces
        for attempt in range(max_retries):
            try:
                central_socket.sendall((json.dumps(msg_to_send) + "\n").encode("utf-8"))
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
    
    central_socket = None 
    central_connected = False

    # =====================================================
    # FUNCIONES DE REGISTRO Y AUTENTICACIÓN
    # =====================================================

    # Registra el CP en EV_Registry via HTTPS
    def registrar_en_registry():
        nonlocal config

        # Verificar si el CP ya está registrado
        if config.get("authToken"):
            print(f"[CP_MONITOR {cp_id}] Ya esta registrado. Use opcion 3 para darse de baja primero.")
            return False

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

                # Guardar en configuración local
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

    # Autentica el CP en Central comprobando que existe en la BD via API REST
    def autenticar_en_central(): 
        nonlocal config, central_socket, central_connected
        import requests

        # Verificar que está registrado
        if not config.get("authToken"):
            print(f"[CP_MONITOR {cp_id}] Error: No esta registrado. Registrese primero (opcion 1)")
            return False

        # URL de API_Central (puerto 5002 en el mismo PC que Central)
        api_url = f"http://{central_ip}:5002"

        print(f"[CP_MONITOR {cp_id}] Enviando autenticacion a API_Central: {api_url}/authenticate")

        try:
            data = {
                "idCP": cp_id
            }
            response = requests.post(
                f"{api_url}/authenticate",
                json=data,
                timeout=10
            )

            result = response.json()

            if result.get("success"):
                encryption_key = result.get("encryption_key")
                config["encryption_key"] = encryption_key
                config["authenticated"] = True
                save_config(config)

                print(f"[CP_MONITOR {cp_id}] Autenticacion EXITOSA!")
                print(f"[CP_MONITOR {cp_id}] Clave de cifrado recibida y guardada")

                # Conectar a Central TCP para health checks
                print(f"[CP_MONITOR {cp_id}] Conectando a CENTRAL para health checks...")
                central_socket = connect_to_central()
                central_connected = True

                # Registrar en Central
                register_msg = {
                    "type": "register",
                    "idCP": cp_id,
                    "precio": config.get("precio", 0.30),
                    "ubicacion": config.get("ubicacion", f"Zona-{cp_id}")
                }
                if send_to_central(register_msg):
                    print(f"[CP_MONITOR {cp_id}] Registrado en CENTRAL, iniciando health checks...")

                return True
            else:
                print(f"[CP_MONITOR {cp_id}] Autenticacion FALLIDA: {result.get('error', 'Error desconocido')}")
                return False

        except requests.exceptions.ConnectionError as e:
            print(f"[CP_MONITOR {cp_id}] Error de conexion a API_Central: {e}")
            print(f"[CP_MONITOR {cp_id}] Asegurese de que Central esta ejecutandose en {central_ip}")
            return False
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Error en autenticacion: {e}")
            return False

    # Da de baja el CP del Registry via HTTPS
    def darse_de_baja():
        nonlocal config, central_socket, central_connected

        auth_token = config.get("authToken")
        if not auth_token:
            print(f"[CP_MONITOR {cp_id}] Error: No hay authToken. No esta registrado.")
            return False

        try:
            print(f"[CP_MONITOR {cp_id}] Enviando baja a Registry: {registry_url}/unregister/{cp_id}")
            response = requests.delete(
                f"{registry_url}/unregister/{cp_id}",
                verify=False,
                timeout=10
            )

            result = response.json()

            if not result.get('error'):
                print(f"[CP_MONITOR {cp_id}] Baja exitosa en Registry!")

                # Limpiar configuración local
                config["authToken"] = None
                config["encryption_key"] = None
                config["authenticated"] = False
                save_config(config)

                # Desconectar de Central
                if central_connected and central_socket:
                    try:
                        central_socket.close()
                    except:
                        pass
                    central_socket = None
                    central_connected = False
                    print(f"[CP_MONITOR {cp_id}] Desconectado de CENTRAL")

                print(f"[CP_MONITOR {cp_id}] Configuracion limpiada")
                return True
            else:
                print(f"[CP_MONITOR {cp_id}] Error: {result.get('message')}")
                return False

        except requests.exceptions.ConnectionError as e:
            print(f"[CP_MONITOR {cp_id}] Error de conexion: {e}")
            return False
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Error contactando Registry: {e}")
            return False

    # Muestra la configuración actual del CP
    def ver_configuracion():
        
        print(f"\n[CP_MONITOR {cp_id}] CONFIGURACION ACTUAL:")
        print(f"   idCP: {config.get('idCP', 'N/A')}")
        print(f"   authToken: {config.get('authToken', 'N/A')[:16] + '...' if config.get('authToken') else 'N/A'}")
        print(f"   encryption_key: {'SI' if config.get('encryption_key') else 'NO'}")
        print(f"   authenticated: {config.get('authenticated', False)}")
        print(f"   precio: {config.get('precio', 'N/A')}")
        print(f"   ubicacion: {config.get('ubicacion', 'N/A')}")

    # =====================================================
    # REGISTRO INICIAL EN CENTRAL (si ya está autenticado)
    # =====================================================

    # Hilo para escuchar respuestas de CENTRAL (errores de token, ordenes, etc.)
    token_invalido = threading.Event()

    def escuchar_central():
        nonlocal central_socket, central_connected, config
        try:
            conn_file = central_socket.makefile('r', encoding='utf-8')
            while central_connected and not token_invalido.is_set():
                try:
                    line = conn_file.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    msg = json.loads(line)
                    if msg.get("type") == "error" and msg.get("error") == "TOKEN_INVALIDO":
                        print(f"\n[CP_MONITOR {cp_id}] *** ERROR: TOKEN INVALIDO - SUMINISTRO DETENIDO ***")
                        print(f"[CP_MONITOR {cp_id}] El token ha sido modificado o revocado.")
                        print(f"[CP_MONITOR {cp_id}] Debe volver a registrarse y autenticarse.")
                        token_invalido.set()
                        config["authenticated"] = False
                        config["encryption_key"] = None
                        save_config(config)
                        break
                except Exception as e:
                    if central_connected:
                        print(f"[CP_MONITOR {cp_id}] Error leyendo de CENTRAL: {e}")
                    break
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Error en hilo de escucha: {e}")

    if config.get("authenticated") and config.get("encryption_key"):
        print(f"[CP_MONITOR {cp_id}] Ya autenticado, conectando a CENTRAL...")
        central_socket = connect_to_central()
        central_connected = True
        # Iniciar hilo de escucha
        threading.Thread(target=escuchar_central, daemon=True).start()
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

    # Iniciar el hilo para ir mostrando el estado
    threading.Thread(target=mostrar_estado_continuo, daemon=True).start()

    # Hilo para menú interactivo
    def menu_interactivo():
        nonlocal config
        while True:
            print(f"\n--- MENU CP_MONITOR {cp_id} ---")
            print("1 - Registrarse en Registry (HTTPS)")
            print("2 - Autenticarse en Central")
            print("3 - Darse de baja en Registry")
            print("4 - Ver configuracion")
            print("5 - Salir del menu")

            try:
                opcion = input("Opcion: ").strip()
            except (EOFError, KeyboardInterrupt):
                break

            if opcion == "1":
                registrar_en_registry()
            elif opcion == "2":
                autenticar_en_central()
            elif opcion == "3":
                darse_de_baja()
            elif opcion == "4":
                ver_configuracion()
            elif opcion == "5":
                print("Saliendo...")
                os._exit(0)

    # Iniciar menú en un hilo separado
    threading.Thread(target=menu_interactivo, daemon=True).start()

    # Bucle principal: health check y notificacion a CENTRAL (cada 5 segundos)
    fallo_prev = False
    engine_conectado_prev = False

    try:
        while True:
            # Solo enviar health checks si estamos conectados a Central
            if not central_connected:
                time.sleep(2)
                continue

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

                # Enviar estado AVERIADO (incluir token para validacion)
                health_msg = {"type": "health", "idCP": cp_id, "salud": "KO", "authToken": config.get("authToken")}
                send_to_central(health_msg)

                if engine_conectado_prev:
                    engine_conectado_prev = False

            # Si el Engine responde OK
            else:
                health_msg = {"type": "health", "idCP": cp_id, "salud": "OK", "authToken": config.get("authToken")}
                send_to_central(health_msg)

                if fallo_prev:
                    # Se recuperó de una avería
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
        # Cerrar socket de Central si existe
        if central_socket:
            try:
                central_socket.close()
                print(f"[CP_MONITOR {cp_id}] Socket de Central cerrado")
            except:
                pass
        print(f"[CP_MONITOR {cp_id}] Monitor finalizado correctamente")

if __name__ == "__main__":
    main()
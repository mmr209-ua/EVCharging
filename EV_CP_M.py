# EV_CP_M
import sys
import json
import time
import socket
import threading

def main():
    if len(sys.argv) != 4:
        print("Uso: py EV_CP_M.py <central_ip:central_port> <cp_id> <engine_ip:engine_port>")
        sys.exit(1)

    central = sys.argv[1]
    cp_id = sys.argv[2]
    engine = sys.argv[3]

    # Separar IP y puerto de la central
    central_ip, central_port = central.split(":")
    central_port = int(central_port)

    # Separar IP y puerto del engine
    engine_ip, engine_port = engine.split(":")
    engine_port = int(engine_port)


    # ==========================================================
    # Conexión persistente con CENTRAL por TCP con manejo elegante de errores
    # ==========================================================
    def connect_to_central():
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((central_ip, central_port))
                print(f"[CP_MONITOR {cp_id}] ✅ Conectado a CENTRAL ({central_ip}:{central_port})")
                return s
            except socket.timeout:
                print(f"[CP_MONITOR {cp_id}] ⏰ Timeout conectando a CENTRAL, reintentando...")
            except ConnectionRefusedError:
                print(f"[CP_MONITOR {cp_id}] 🔌 CENTRAL no disponible, reintentando en 3 segundos...")
            except Exception as e:
                print(f"[CP_MONITOR {cp_id}] 🔌 Error conectando a CENTRAL: {e}, reintentando...")
            time.sleep(3)

    def send_to_central(msg):
        nonlocal central_socket
        max_retries = 3
        for attempt in range(max_retries):
            try:
                central_socket.sendall((json.dumps(msg) + "\n").encode("utf-8"))
                return True
            except Exception as e:
                print(f"[CP_MONITOR {cp_id}] ❌ Error enviando a CENTRAL (intento {attempt+1}/{max_retries}): {e}")
                try:
                    central_socket.close()
                except:
                    pass
                central_socket = connect_to_central()
                if attempt == max_retries - 1:
                    print(f"[CP_MONITOR {cp_id}] ❌ No se pudo enviar mensaje después de {max_retries} intentos")
                    return False
        return False

    central_socket = connect_to_central()

    # ==========================================================
    # Registro inicial del CP
    # ==========================================================
    register_msg = {
        "type": "register",
        "idCP": cp_id,
        "precio": 0.30,
        "ubicacion": f"Zona-{cp_id}"
    }
    if send_to_central(register_msg):
        print(f"[CP_MONITOR {cp_id}] ✅ Registrado en CENTRAL")
    else:
        print(f"[CP_MONITOR {cp_id}] ❌ Falló el registro en CENTRAL")

    # ==========================================================
    # CORRECCIÓN ERROR 1: Hilo separado para mostrar estado cada segundo
    # ==========================================================
    def mostrar_estado_continuo():
        ultimo_estado = None
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

            estado_actual = "🟢 ENGINE CONECTADO" if ok else "🔴 ENGINE AVERIADO"
            if estado_actual != ultimo_estado:
                print(f"[CP_MONITOR {cp_id}] {estado_actual}")
                ultimo_estado = estado_actual
            
            time.sleep(1)

    # Iniciar hilo para mostrar estado continuo
    threading.Thread(target=mostrar_estado_continuo, daemon=True).start()

    # ==========================================================
    # Bucle principal para enviar estados a CENTRAL
    # ==========================================================
    fallo_prev = False
    engine_conectado_prev = False

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

        # --- Si no responde el Engine ---
        if not ok:
            if not fallo_prev:
                alert_msg = {"type": "alert", "idCP": cp_id, "alerta": "ENGINE_NO_RESPONDE"}
                if send_to_central(alert_msg):
                    print(f"[CP_MONITOR {cp_id}] ❌ ENGINE no responde, alerta enviada a CENTRAL")
                fallo_prev = True
            
            status_msg = {"type": "status", "idCP": cp_id, "estado": "AVERIADO"}
            send_to_central(status_msg)
            
            if engine_conectado_prev:
                engine_conectado_prev = False

        # --- Si el Engine responde correctamente ---
        else:
            if fallo_prev:
                health_msg = {"type": "health", "idCP": cp_id, "salud": "RECUPERADO"}
                if send_to_central(health_msg):
                    print(f"[CP_MONITOR {cp_id}] ✅ ENGINE recuperado, notificado a CENTRAL")
                fallo_prev = False

            status_msg = {"type": "status", "idCP": cp_id, "estado": "ACTIVADO"}
            health_msg = {"type": "health", "idCP": cp_id, "salud": "OK"}
            send_to_central(status_msg)
            send_to_central(health_msg)
            
            if not engine_conectado_prev:
                engine_conectado_prev = True

        time.sleep(5)  # Envío de estados a CENTRAL cada 5 segundos

if __name__ == "__main__":
    main()
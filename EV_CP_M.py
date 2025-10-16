import sys
import json
import time
import socket

def main():
    if len(sys.argv) < 6:
        print("Uso: python EV_CP_M.py <cp_id> <engine_ip> <engine_port> <central_ip> <central_port>")
        sys.exit(1)

    cp_id = str(sys.argv[1])
    engine_ip = sys.argv[2]
    engine_port = int(sys.argv[3])
    central_ip = sys.argv[4]
    central_port = int(sys.argv[5])

    # ==========================================================
    # Conexión persistente con CENTRAL por TCP
    # ==========================================================
    def connect_to_central():
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((central_ip, central_port))
                print(f"[CP_MONITOR {cp_id}] Conectado a CENTRAL ({central_ip}:{central_port})")
                return s
            except Exception as e:
                print(f"[CP_MONITOR {cp_id}] Esperando CENTRAL... ({e})")
                time.sleep(3)

    central_socket = connect_to_central()

    # ==========================================================
    # Función segura para enviar mensajes a CENTRAL
    # ==========================================================
    def send_to_central(msg):
        nonlocal central_socket
        try:
            central_socket.sendall((json.dumps(msg) + "\n").encode("utf-8"))
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Error enviando a CENTRAL: {e}, reconectando...")
            try:
                central_socket.close()
            except:
                pass
            central_socket = connect_to_central()
            central_socket.sendall((json.dumps(msg) + "\n").encode("utf-8"))

    # ==========================================================
    # Registro inicial del CP
    # ==========================================================
    register_msg = {
        "type": "register",
        "idCP": cp_id,
        "precio": 0.30,
        "ubicacion": f"Zona-{cp_id}"
    }
    send_to_central(register_msg)
    print(f"[CP_MONITOR {cp_id}] Registrado en CENTRAL")

    # ==========================================================
    # Bucle principal: chequea la salud del Engine y reporta a CENTRAL
    # ==========================================================
    fallo_prev = False

    while True:
        ok = False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((engine_ip, engine_port))
            s.sendall(b"PING")
            data = s.recv(1024).decode().strip()
            s.close()
            if data == "PONG":
                ok = True
        except Exception as e:
            print(f"[CP_MONITOR {cp_id}] Engine no responde: {e}")
            ok = False

        # --- Si no responde el Engine ---
        if not ok:
            if not fallo_prev:
                alert_msg = {"type": "alert", "idCP": cp_id, "alerta": "ENGINE_NO_RESPONDE"}
                send_to_central(alert_msg)
                print(f"[CP_MONITOR {cp_id}] ENGINE no responde, alerta enviada")
                fallo_prev = True
            status_msg = {"type": "status", "idCP": cp_id, "estado": "AVERIADO"}
            send_to_central(status_msg)

        # --- Si el Engine responde correctamente ---
        else:
            if fallo_prev:
                health_msg = {"type": "health", "idCP": cp_id, "salud": "RECUPERADO"}
                send_to_central(health_msg)
                print(f"[CP_MONITOR {cp_id}] ENGINE recuperado")
                fallo_prev = False

            status_msg = {"type": "status", "idCP": cp_id, "estado": "ACTIVADO"}
            health_msg = {"type": "health", "idCP": cp_id, "salud": "OK"}
            send_to_central(status_msg)
            send_to_central(health_msg)

        time.sleep(5)

if __name__ == "_main_":
    main()
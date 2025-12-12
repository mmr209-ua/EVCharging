# EV_Central.py - Release 2 con autenticacion, cifrado, auditoria y API REST
import sys
import threading
import json
import time
import socket
import sqlite3
from kafka import KafkaConsumer, KafkaProducer
from EV_DB import *
from EV_Topics import *
import tkinter as tk
from tkinter import ttk, messagebox

# Importar modulos de Release 2
try:
    from crypto_utils import generate_encryption_key, decrypt_message
except ImportError:
    print("[CENTRAL] ADVERTENCIA: crypto_utils no encontrado, cifrado deshabilitado")
    generate_encryption_key = lambda: "dummy_key"
    decrypt_message = lambda k, m: json.loads(m)

# ======================================================================
# Eventos y estado global del programa
# ======================================================================
BBDD = "Base_Datos.sqlite"
REFRESH_MS = 1500
stop_event = threading.Event()
actualizar_pantalla = threading.Event()
gui_instance = None
CP_CONSUMPTION_DATA = {}

# Colores por estado de CP
COLORS = {
    "ACTIVADO": "#77E977",
    "SUMINISTRANDO": "#77E977",
    "PARADO": "#FFA500",
    "AVERIADO": "#FF0000",
    "DESCONECTADO": "#9E9B9B",
}

def safe_log(msg: str):
    global gui_instance
    try:
        if gui_instance is not None and hasattr(gui_instance, "log_text") and gui_instance.log_text.winfo_exists():
            gui_instance.log(msg)
        else:
            print(msg)
    except Exception:
        print(msg)

# ======================================================================
# Sistema de Auditoria (Release 2)
# ======================================================================

def log_audit(event_type: str, source_ip: str, source_id: str, action: str, parameters: dict, result: str):
    """
    Registra evento en tabla AUDIT_LOG.

    Args:
        event_type: 'AUTH', 'STATE_CHANGE', 'INCIDENT', 'CONTROL_ORDER', 'WEATHER_ALERT', 'SECURITY'
        source_ip: IP del cliente (CP, Driver, EV_W)
        source_id: idCP o idDriver
        action: 'AUTHENTICATE', 'PARAR', 'REANUDAR', 'REVOKE_KEYS', etc.
        parameters: Dict con detalles del evento
        result: 'SUCCESS', 'FAILED', 'PENDING'
    """
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO AUDIT_LOG (event_type, source_ip, source_id, action, parameters, result)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (event_type, source_ip, source_id, action, json.dumps(parameters), result))
            conn.commit()
    except Exception as e:
        print(f"[AUDIT] Error registrando evento: {e}")

# ======================================================================
# Utilidades BD (SQLite)
# ======================================================================

def db_execute(conn, query, params=()):
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    return cur

def db_fetchall(conn, query, params=()):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchall()

def upsert_cp(conn, id_cp: str, estado: str, precio: float, ubicacion: str):
    db_execute(conn, """
        INSERT INTO CP (idCP, estado, precio, ubicacion)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(idCP) DO UPDATE SET
            estado=excluded.estado,
            precio=excluded.precio,
            ubicacion=excluded.ubicacion
    """, (id_cp, estado, precio, ubicacion))

# ======================================================================
# Inicializar central
# ======================================================================

def inicializar_estado_cps():
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute("UPDATE CP SET estado = 'DESCONECTADO'")
            conn.commit()
            safe_log("[CENTRAL] Todos los CPs marcados como DESCONECTADOS al iniciar")
        actualizar_pantalla.set()
    except sqlite3.Error as e:
        safe_log(f"[CENTRAL] [BBDD] Error inicializando estados de CPs: {e}")

# ======================================================================
# Descifrado de mensajes Kafka (Release 2)
# ======================================================================

def descifrar_mensaje_kafka(event, conn):
    """
    Descifra un mensaje Kafka si viene cifrado.
    Retorna el evento descifrado o None si falla.
    """
    if 'encrypted' not in event:
        # Mensaje sin cifrar (modo legacy/compatible)
        return event

    id_cp = event.get('idCP')
    if not id_cp:
        safe_log("[CENTRAL] Mensaje cifrado sin idCP, ignorando")
        return None

    try:
        cur = conn.cursor()
        cur.execute("SELECT encryption_key, authenticated FROM CP WHERE idCP=?", (id_cp,))
        row = cur.fetchone()

        if not row:
            safe_log(f"[CENTRAL] CP {id_cp} no registrado, mensaje ignorado")
            return None

        encryption_key = row[0]
        authenticated = row[1]

        if not authenticated or not encryption_key:
            safe_log(f"[CENTRAL] CP {id_cp} NO AUTENTICADO, mensaje cifrado ignorado")
            log_audit('SECURITY', 'unknown', id_cp, 'UNAUTHENTICATED_MESSAGE', {}, 'FAILED')
            return None

        decrypted = decrypt_message(encryption_key, event['encrypted'])
        return decrypted

    except Exception as e:
        safe_log(f"[CENTRAL] Error descifrando mensaje de CP {id_cp}: {e}")
        log_audit('SECURITY', 'unknown', id_cp, 'DECRYPT_FAILED', {'error': str(e)}, 'FAILED')
        return None

# ======================================================================
# Logica de negocio con KAFKA
# ======================================================================

def consume_loop(topic, producer, consumer):
    conn = None
    try:
        conn = sqlite3.connect(BBDD, check_same_thread=False)
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue
            for _tp, msgs in records.items():
                for msg in msgs:
                    event = msg.value
                    try:
                        # Intentar descifrar si viene cifrado
                        if 'encrypted' in event:
                            event = descifrar_mensaje_kafka(event, conn)
                            if event is None:
                                continue

                        if topic == CP_STATUS:
                            cambiar_estado_CP(event, conn)
                        elif topic == CP_CONSUMPTION:
                            monitorizar_consumo_CP(event)
                        elif topic == CP_SUPPLY_COMPLETE:
                            enviar_ticket(event, conn, producer)
                        elif topic == SUPPLY_REQUEST_TO_CENTRAL:
                            procesar_peticion_suministro(event, conn, producer)
                        elif topic == SUPPLY_HISTORY:
                            enviar_historial_driver(event, producer)
                    except Exception as e:
                        safe_log(f"[CENTRAL] Excepcion tramitando topic {topic}: {e}")
    except Exception as e:
        safe_log(f"[CENTRAL] Excepcion en hilo de consumo ({topic}): {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def cambiar_estado_CP(event, conn):
    id_cp = str(event.get("idCP"))
    estado = event.get("estado")
    cur = conn.cursor()
    cur.execute("SELECT estado FROM CP WHERE idCP = ?", (id_cp,))
    row = cur.fetchone()

    if not row or row[0] != estado:
        cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", (estado, id_cp))
        conn.commit()
        safe_log(f"[CENTRAL] Estado actualizado CP {id_cp} -> {estado}")
        log_audit('STATE_CHANGE', 'kafka', id_cp, f'ESTADO_{estado}', {'estado_anterior': row[0] if row else None}, 'SUCCESS')

    actualizar_pantalla.set()

def monitorizar_consumo_CP(event):
    id_cp = str(event.get("idCP"))
    CP_CONSUMPTION_DATA[id_cp] = {
        "kwh": float(event.get("consumo", 0.0)),
        "importe": float(event.get("importe", 0.0)),
        "conductor": event.get("conductor", "desconocido"),
    }
    actualizar_pantalla.set()

def procesar_peticion_suministro(event, conn, producer):
    safe_log(f"[CENTRAL] Peticion de recarga: {event}")
    id_cp = str(event.get("idCP"))
    id_driver = str(event.get("idDriver")) if event.get("idDriver") is not None else None

    if not id_cp:
        safe_log("[CENTRAL] ERROR: Peticion sin idCP.")
        return

    cur = conn.cursor()
    cur.execute("SELECT estado, paused_by_weather FROM CP WHERE idCP = ?", (id_cp,))
    row = cur.fetchone()

    if row is None:
        safe_log(f"[CENTRAL] ERROR: CP {id_cp} no esta registrado en la BD")
        if id_driver is not None:
            try:
                payload_cp = {"idCP": id_cp, "idDriver": id_driver, "authorize": "NO"}
                producer.send(AUTHORIZE_SUPPLY, payload_cp)
                producer.flush()
            except Exception as e:
                safe_log(f"[CENTRAL] Error notificando al driver que el CP no existe: {e}")
        return

    estado = row[0]
    paused_by_weather = row[1] if len(row) > 1 else 0

    # Verificar si esta pausado por clima
    if paused_by_weather:
        safe_log(f"[CENTRAL] CP {id_cp} PAUSADO POR ALERTA CLIMATICA")
        try:
            payload_cp = {"idCP": id_cp, "idDriver": id_driver, "authorize": "NO", "motivo": "ALERTA_CLIMATICA"}
            producer.send(AUTHORIZE_SUPPLY, payload_cp)
            producer.flush()
        except Exception as e:
            safe_log(f"[CENTRAL] Error enviando rechazo: {e}")
        return

    if estado == "ACTIVADO":
        if id_driver is not None:
            try:
                payload_cp = {"idCP": id_cp, "idDriver": id_driver, "authorize": "YES"}
                producer.send(AUTHORIZE_SUPPLY, payload_cp)
                producer.flush()
                safe_log(f"[CENTRAL] CP {id_cp} disponible; permiso CONCEDIDO")
                log_audit('CONTROL_ORDER', 'kafka', id_cp, 'AUTHORIZE_SUPPLY', {'idDriver': id_driver}, 'SUCCESS')
            except Exception as e:
                safe_log(f"[CENTRAL] Error enviando autorizacion CP: {e}")
    else:
        safe_log(f"[CENTRAL] CP {id_cp} NO DISPONIBLE (estado: {estado}).")
        try:
            payload_cp = {"idCP": id_cp, "idDriver": id_driver, "authorize": "NO"}
            producer.send(AUTHORIZE_SUPPLY, payload_cp)
            producer.flush()
            safe_log(f"[CENTRAL] CP {id_cp}; permiso DENEGADO")
        except Exception as e:
            safe_log(f"[CENTRAL] Error enviando autorizacion CP: {e}")

def enviar_ticket(event, conn, producer):
    id_cp = str(event.get("idCP"))
    ticket = event.get("ticket", {})
    estado = str(ticket.get("estado", ""))

    try:
        producer.send(DRIVER_SUPPLY_COMPLETE, {"ticket": ticket})
        producer.flush()
        safe_log(f"[CENTRAL] Suministro finalizado CP {id_cp}, ticket reenviado al Driver")
    except Exception as e:
        safe_log(f"[CENTRAL] Error reenviando ticket al Driver: {e}")

    try:
        conductor = int(ticket.get("idDriver", 0))
        energia = float(ticket.get("energia", 0))
        importe = float(ticket.get("precio_total", 0))
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO CONSUMO (conductor, cp, consumo, importe, estado)
            VALUES (?, ?, ?, ?, ?)
        """, (conductor, id_cp, energia, importe, estado))
        conn.commit()
        safe_log(f"[CENTRAL] Guardado suministro completado en BD: CP {id_cp}, Conductor {conductor}.")
    except Exception as e:
        safe_log(f"[CENTRAL][BBDD] Error guardando consumo: {e}")

    # Verificar si debe quedar pausado por clima
    cur = conn.cursor()
    cur.execute("SELECT paused_by_weather FROM CP WHERE idCP = ?", (id_cp,))
    row = cur.fetchone()

    if row and row[0]:
        # Pausado por clima: dejar en PARADO
        cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("PARADO", id_cp))
        conn.commit()
        safe_log(f"[CENTRAL] CP {id_cp} finalizado pero queda PARADO por alerta climatica")
    elif estado == "COMPLETADO":
        cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("ACTIVADO", id_cp))
        conn.commit()

    CP_CONSUMPTION_DATA.pop(id_cp, None)
    actualizar_pantalla.set()

def enviar_historial_driver(event, producer):
    id_driver = str(event.get("idDriver"))
    if not id_driver:
        safe_log("[CENTRAL] SUPPLY_HISTORY recibido sin idDriver")
        return

    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT conductor, cp, consumo, importe, estado
                FROM CONSUMO
                WHERE conductor = ?
                ORDER BY timestamp DESC
            """, (id_driver,))
            registros = [
                {"idDriver": r[0], "idCP": r[1], "energia": r[2], "importe": r[3], "estado": r[4]}
                for r in cur.fetchall()
            ]
        payload = registros if registros else []
        producer.send(SUMINISTROS_COMPLETADOS, payload)
        producer.flush()
        safe_log(f"[CENTRAL] Historial enviado a Driver {id_driver} ({len(payload)} registros)")
    except Exception as e:
        safe_log(f"[CENTRAL] Error enviando historial a Driver {id_driver}: {e}")

# ======================================================================
# Logica de negocio con SOCKETS (TCP)
# ======================================================================

def tcp_server(listen_port, producer):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", int(listen_port)))
    server.listen()
    safe_log(f"[CENTRAL] Servidor TCP escuchando en puerto {listen_port}")
    while not stop_event.is_set():
        try:
            server.settimeout(1)
            socket_conn, addr = server.accept()
            threading.Thread(target=handle_tcp_client,args=(socket_conn,addr, producer),daemon=True).start()
        except socket.timeout:
            continue
        except Exception as e:
            if not stop_event.is_set():
                safe_log(f"[CENTRAL][TCP] Error en accept: {e}")

def handle_tcp_client(socket_conn, addr, producer):
    db_conn = None
    id_cp_asociado = None
    try:
        db_conn = sqlite3.connect(BBDD, check_same_thread=False)
        conn_file = socket_conn.makefile('r', encoding='utf-8')

        while not stop_event.is_set():
            line = conn_file.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                safe_log(f"[CENTRAL][TCP] JSON invalido desde {addr}: {line}")
                continue

            msg_type = msg.get("type")
            id_cp_asociado = msg.get("idCP", id_cp_asociado)

            try:
                if msg_type == "register":
                    registrar_CP(msg, db_conn, addr)
                elif msg_type == "authenticate":
                    # Autenticacion de CP (Release 2)
                    response = autenticar_CP(msg, db_conn, addr)
                    socket_conn.sendall((json.dumps(response) + "\n").encode('utf-8'))
                elif msg_type == "alert":
                    safe_log(f"[CENTRAL][TCP] ALERTA de MONITOR {msg.get('idCP')}: {msg.get('alerta')}")
                    log_audit('INCIDENT', addr[0], msg.get('idCP'), 'ALERT', {'alerta': msg.get('alerta')}, 'SUCCESS')
                elif msg_type == "health":
                    comprobar_salud_CP({"idCP": msg.get("idCP"), "salud": msg.get("salud")}, db_conn, addr)
            except Exception as e:
                safe_log(f"[CENTRAL][TCP] Error tramitando mensaje de {addr}: {e}")

    except Exception as e:
        safe_log(f"[CENTRAL][TCP] Error en cliente {addr}: {e}")

    finally:
        if id_cp_asociado:
            try:
                cur = db_conn.cursor()
                cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("DESCONECTADO", id_cp_asociado))
                db_conn.commit()
                safe_log(f"[CENTRAL][TCP] Monitor {addr} desconectado -> CP {id_cp_asociado} DESCONECTADO")
                actualizar_pantalla.set()
            except Exception as e:
                safe_log(f"[CENTRAL][TCP] Error al marcar DESCONECTADO: {e}")

        try:
            socket_conn.close()
        except Exception:
            pass
        try:
            if db_conn:
                db_conn.close()
        except Exception:
            pass

def registrar_CP(event, conn, addr):
    id_cp = str(event.get("idCP"))
    precio = float(event.get("precio", 0))
    ubicacion = event.get("ubicacion", "")
    upsert_cp(conn, id_cp, "ACTIVADO", precio, ubicacion)
    safe_log(f"[CENTRAL] Registrado/actualizado CP {id_cp} en BD")
    log_audit('STATE_CHANGE', addr[0], id_cp, 'REGISTER', {'precio': precio, 'ubicacion': ubicacion}, 'SUCCESS')
    actualizar_pantalla.set()

def autenticar_CP(msg, conn, addr):
    """
    Autentica un CP usando el auth_token del Registry.
    Si es valido, genera y devuelve una encryption_key unica.
    """
    id_cp = str(msg.get("idCP"))
    auth_token = msg.get("authToken")

    safe_log(f"[CENTRAL] Intento de autenticacion de CP {id_cp} desde {addr}")

    cur = conn.cursor()
    cur.execute("SELECT auth_token, authenticated FROM CP WHERE idCP = ?", (id_cp,))
    row = cur.fetchone()

    if not row:
        safe_log(f"[CENTRAL] CP {id_cp} no registrado en Registry")
        log_audit('AUTH', addr[0], id_cp, 'AUTHENTICATE', {'reason': 'CP not registered'}, 'FAILED')
        return {"success": False, "error": "CP no registrado. Registrese primero en Registry."}

    stored_token = row[0]

    if stored_token != auth_token:
        safe_log(f"[CENTRAL] Token invalido para CP {id_cp}")
        log_audit('AUTH', addr[0], id_cp, 'AUTHENTICATE', {'reason': 'Invalid token'}, 'FAILED')
        return {"success": False, "error": "Token de autenticacion invalido"}

    # Generar clave de cifrado unica
    encryption_key = generate_encryption_key()

    # Guardar en BD
    cur.execute("""
        UPDATE CP SET encryption_key = ?, authenticated = 1, estado = 'ACTIVADO'
        WHERE idCP = ?
    """, (encryption_key, id_cp))
    conn.commit()

    safe_log(f"[CENTRAL] CP {id_cp} AUTENTICADO correctamente")
    log_audit('AUTH', addr[0], id_cp, 'AUTHENTICATE', {'encryption_key': encryption_key[:16] + '...'}, 'SUCCESS')
    actualizar_pantalla.set()

    return {"success": True, "encryption_key": encryption_key}

def comprobar_salud_CP(event, conn, addr=None):
    id_cp = str(event.get("idCP"))
    salud = event.get("salud")
    cur = conn.cursor()
    cur.execute("SELECT estado FROM CP WHERE idCP = ?", (id_cp,))
    row = cur.fetchone()
    estado_actual = row[0] if row else None

    if estado_actual != "PARADO":
        if salud == "KO":
            if estado_actual != "AVERIADO":
                safe_log(f"[CENTRAL] CP {id_cp} AVERIADO")
                cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("AVERIADO", id_cp))
                conn.commit()
                log_audit('INCIDENT', addr[0] if addr else 'unknown', id_cp, 'AVERIA', {}, 'SUCCESS')
                actualizar_pantalla.set()
        else:
            if estado_actual != "ACTIVADO" and estado_actual != "SUMINISTRANDO":
                safe_log(f"[CENTRAL] CP {id_cp} RECUPERADO")
                cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("ACTIVADO", id_cp))
                conn.commit()
                log_audit('INCIDENT', addr[0] if addr else 'unknown', id_cp, 'RECUPERADO', {}, 'SUCCESS')
                actualizar_pantalla.set()

# ======================================================================
# Enviar lista de CPs disponibles a los Drivers
# ======================================================================

def enviar_lista_cps_disponibles(producer):
    while not stop_event.is_set():
        try:
            with sqlite3.connect(BBDD) as conn:
                cur = conn.cursor()
                cur.execute("SELECT idCP FROM CP WHERE estado = 'ACTIVADO' AND paused_by_weather = 0")
                cps = [row[0] for row in cur.fetchall()]
            producer.send(LISTA_CPS_DISPONIBLES, cps)
            producer.flush()
        except Exception as e:
            safe_log(f"[CENTRAL] Error enviando lista de CPs disponibles: {e}")
        time.sleep(5)

# ======================================================================
# GUI Tkinter
# ======================================================================

class CentralGUI(tk.Tk):

    def __init__(self, producer):
        super().__init__()
        self.title("EV Central - Release 2")
        self.geometry("1100x700")
        self.producer = producer

        # Tabla con la monitorizacion de los CPs
        cols = ("idCP", "Estado", "Precio", "Ubicacion", "Consumo kWh", "Importe", "Auth", "Clima")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=14)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, stretch=True, width=100)
        self.tree.column("idCP", width=60)
        self.tree.column("Auth", width=50)
        self.tree.column("Clima", width=50)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 6))

        # Botones de control
        btn_frame = tk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=6)
        tk.Button(btn_frame, text="Parar CP", command=self.parar_sel).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Reanudar CP", command=self.reanudar_sel).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Parar TODOS", command=self.parar_todos).pack(side=tk.LEFT, padx=15)
        tk.Button(btn_frame, text="Reanudar TODOS", command=self.reanudar_todos).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Restaurar Claves", command=self.restaurar_claves, bg="#FFD700").pack(side=tk.LEFT, padx=15)
        tk.Button(btn_frame, text="Salir", command=self.salir).pack(side=tk.RIGHT, padx=5)

        # Logs
        self.log_text = tk.Text(self, height=12, wrap=tk.WORD, bg="#1e1e1e", fg="#d6ffd6")
        self.log_text.pack(fill=tk.BOTH, expand=False, padx=10, pady=(6, 10))

        self.after(500, self.refresco_periodico)
        self.tree.bind("<Button-1>", self.click_fuera)
        self.protocol("WM_DELETE_WINDOW", self.salir)

    def log(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def refresco_periodico(self):
        if stop_event.is_set():
            return

        recargar = False
        if actualizar_pantalla.is_set():
            actualizar_pantalla.clear()
            recargar = True

        cps = self.leer_cps()
        suministrando = any(estado == "SUMINISTRANDO" for _, estado, *_ in cps)
        if suministrando:
            recargar = True

        if recargar:
            self.actualizar_tabla()

        self.after(1000 if suministrando else REFRESH_MS, self.refresco_periodico)

    def click_fuera(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region not in ("cell", "tree"):
            self.tree.selection_remove(self.tree.selection())

    def leer_cps(self):
        try:
            with sqlite3.connect(BBDD) as conn:
                cur = conn.cursor()
                cur.execute("SELECT idCP, estado, precio, ubicacion, authenticated, paused_by_weather FROM CP ORDER BY idCP ASC")
                return cur.fetchall()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error leyendo CPs: {e}")
            return []

    def actualizar_tabla(self):
        selected = self._sel_id()
        cps = self.leer_cps()
        self.tree.delete(*self.tree.get_children())

        for row in cps:
            idCP = row[0]
            estado = row[1]
            precio = row[2]
            ubicacion = row[3]
            authenticated = row[4] if len(row) > 4 else 0
            paused_by_weather = row[5] if len(row) > 5 else 0

            consumo = CP_CONSUMPTION_DATA.get(str(idCP), {})
            kwh = consumo.get("kwh", None)
            importe = consumo.get("importe", None)

            estado_display = estado
            if estado == "PARADO":
                if paused_by_weather:
                    estado_display += " (Clima)"
                else:
                    estado_display += " (Manual)"

            kwh_display = f"{kwh:.2f}" if estado == "SUMINISTRANDO" and kwh is not None else ""
            importe_display = f"{importe:.2f}" if estado == "SUMINISTRANDO" and importe is not None else ""
            auth_display = "Si" if authenticated else "No"
            clima_display = "Alerta" if paused_by_weather else "OK"

            iid = self.tree.insert("", tk.END,
                values=(idCP, estado_display, precio, ubicacion, kwh_display, importe_display, auth_display, clima_display))

            bg = COLORS.get(estado, COLORS["DESCONECTADO"])
            self.tree.item(iid, tags=(estado,))
            self.tree.tag_configure(estado, background=bg)

            if idCP == selected:
                self.tree.selection_set(iid)

    def _sel_id(self):
        sel = self.tree.selection()
        if not sel:
            return None
        vals = self.tree.item(sel[0], "values")
        return vals[0] if vals else None

    def parar_sel(self):
        id_cp = self._sel_id()
        if not id_cp:
            self.log("[CENTRAL][GUI] Selecciona un CP primero.")
            return
        try:
            with sqlite3.connect(BBDD) as conn:
                db_execute(conn, "UPDATE CP SET estado = ? WHERE idCP = ?", ("PARADO", id_cp))
            self.producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": id_cp})
            self.producer.flush()
            self.log(f"[CENTRAL] Orden PARAR enviada a CP {id_cp}")
            log_audit('CONTROL_ORDER', 'localhost', id_cp, 'PARAR', {'source': 'GUI'}, 'SUCCESS')
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error PARAR {id_cp}: {e}")

    def reanudar_sel(self):
        id_cp = self._sel_id()
        if not id_cp:
            self.log("[CENTRAL][GUI] Selecciona un CP primero.")
            return
        try:
            with sqlite3.connect(BBDD) as conn:
                db_execute(conn, "UPDATE CP SET estado = ?, paused_by_weather = 0 WHERE idCP = ?", ("ACTIVADO", id_cp))
            self.producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": id_cp})
            self.producer.flush()
            self.log(f"[CENTRAL] Orden REANUDAR enviada a CP {id_cp}")
            log_audit('CONTROL_ORDER', 'localhost', id_cp, 'REANUDAR', {'source': 'GUI'}, 'SUCCESS')
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error REANUDAR {id_cp}: {e}")

    def parar_todos(self):
        try:
            with sqlite3.connect(BBDD) as conn:
                db_execute(conn, "UPDATE CP SET estado = 'PARADO'")
            self.producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": "todos"})
            self.producer.flush()
            self.log("[CENTRAL] Enviada orden PARAR a TODOS")
            log_audit('CONTROL_ORDER', 'localhost', 'ALL', 'PARAR', {'source': 'GUI'}, 'SUCCESS')
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error PARAR TODOS: {e}")

    def reanudar_todos(self):
        try:
            with sqlite3.connect(BBDD) as conn:
                db_execute(conn, "UPDATE CP SET estado = 'ACTIVADO', paused_by_weather = 0")
            self.producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": "todos"})
            self.producer.flush()
            self.log("[CENTRAL] Enviada orden REANUDAR a TODOS")
            log_audit('CONTROL_ORDER', 'localhost', 'ALL', 'REANUDAR', {'source': 'GUI'}, 'SUCCESS')
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error REANUDAR TODOS: {e}")

    def restaurar_claves(self):
        """Revoca todas las claves de cifrado y fuerza re-autenticacion."""
        confirm = messagebox.askyesno(
            "Confirmar",
            "Esto revocara todas las claves de cifrado.\nLos CPs deberan re-autenticarse.\n\nContinuar?"
        )
        if not confirm:
            return

        try:
            with sqlite3.connect(BBDD) as conn:
                conn.execute("UPDATE CP SET encryption_key = NULL, authenticated = 0")
                conn.commit()

            log_audit('SECURITY', 'localhost', 'CENTRAL', 'REVOKE_KEYS', {'action': 'Revoke all encryption keys'}, 'SUCCESS')

            self.log("[CENTRAL] Todas las claves de cifrado REVOCADAS")
            self.log("[CENTRAL] Los CPs deben re-autenticarse para volver a operar")
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL] Error revocando claves: {e}")

    def salir(self):
        try:
            stop_event.set()
            self.log("[CENTRAL] Cerrando aplicacion...")
            try:
                self.producer.flush()
                self.producer.close()
            except Exception:
                pass
            self.after(300, self.destroy)
        except Exception:
            pass

# ======================================================================
# Main
# ======================================================================
def main():
    if len(sys.argv) < 4:
        print("Uso: py EV_Central.py <puerto_tcp> <broker_ip:puerto> <db_ip>")
        sys.exit(1)

    listen_port = sys.argv[1]
    broker = sys.argv[2]
    db_host = sys.argv[3]

    with sqlite3.connect(BBDD) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    consumers = {
        CP_STATUS: KafkaConsumer(
            CP_STATUS,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_cp_status",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        CP_CONSUMPTION: KafkaConsumer(
            CP_CONSUMPTION,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_consumption",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        CP_SUPPLY_COMPLETE: KafkaConsumer(
            CP_SUPPLY_COMPLETE,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_supply_complete",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        SUPPLY_REQUEST_TO_CENTRAL: KafkaConsumer(
            SUPPLY_REQUEST_TO_CENTRAL,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_supply_request_from_driver",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
        SUPPLY_HISTORY: KafkaConsumer(
            SUPPLY_HISTORY,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="central_supply_history_request",
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        ),
    }

    safe_log("[CENTRAL] Escuchando topics Kafka y conexiones TCP...")

    inicializar_estado_cps()

    # Hilo servidor TCP
    t_tcp = threading.Thread(target=tcp_server, args=(listen_port, producer), daemon=True)
    t_tcp.start()

    # Hilos consumidores Kafka
    threads = []
    for topic, consumer in consumers.items():
        t = threading.Thread(target=consume_loop, args=(topic, producer, consumer), daemon=True)
        t.start()
        threads.append(t)

    # Hilo que informa periodicamente a los drivers sobre CPs disponibles
    t_lista = threading.Thread(target=enviar_lista_cps_disponibles, args=(producer,), daemon=True)
    t_lista.start()

    # Iniciar API_Central en hilo separado (Release 2)
    try:
        from API_Central import app as api_app, set_kafka_producer
        set_kafka_producer(producer)

        def run_api():
            import logging
            log = logging.getLogger('werkzeug')
            log.setLevel(logging.ERROR)
            api_app.run(host='0.0.0.0', port=5002, debug=False, use_reloader=False)

        t_api = threading.Thread(target=run_api, daemon=True)
        t_api.start()
        safe_log("[CENTRAL] API_Central iniciado en puerto 5002")
    except Exception as e:
        safe_log(f"[CENTRAL] No se pudo iniciar API_Central: {e}")

    # Lanzar GUI
    global gui_instance
    gui_instance = CentralGUI(producer)
    try:
        gui_instance.mainloop()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        safe_log("[CENTRAL] Cerrando conexiones y esperando hilos...")

        for t in threads:
            t.join(timeout=2)

        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        safe_log("[CENTRAL] Recursos cerrados. Hasta luego")

if __name__ == "__main__":
    main()

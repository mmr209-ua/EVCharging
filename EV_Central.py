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
from tkinter import ttk

# ======================================================================
# Eventos y estado global del programa
# ======================================================================
BBDD = "Base_Datos.sqlite"
REFRESH_MS = 1500
stop_event = threading.Event()          # para apagar ordenadamente
actualizar_pantalla = threading.Event() # para refrescar GUI
gui_instance = None                     # referencia global a la interfaz gráfica de usuario
CP_CONSUMPTION_DATA = {}                # datos del CP acerca del consumo en un suministro en tiempo real

# Colores por estado de CP
COLORS = {
    "ACTIVADO": "#77E977",
    "SUMINISTRANDO": "#77E977",
    "PARADO": "#FFA500",
    "AVERIADO": "#FF0000",
    "DESCONECTADO": "#9E9B9B",
}

# Mostrar logs en la GUI
def safe_log(msg: str):
    global gui_instance
    try:
        if gui_instance is not None and hasattr(gui_instance, "log_text") and gui_instance.log_text.winfo_exists():
            gui_instance.log(msg)
        else:
            print(msg)  # fallback a consola si la GUI ya está cerrada
    except Exception:
        # Evitar cualquier error si la GUI ya fue destruida
        print(msg)


# ======================================================================
# Utilidades BD (SQLite)
# ======================================================================

# Ejecutar sentencia
def db_execute(conn, query, params=()):
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    return cur

# Recoger registros
def db_fetchall(conn, query, params=()):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchall()

# Insertar CP si no existe
# Si existe, actualizar precio, ubicacion y estado
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

# Marca todos los CPs existentes como DESCONECTADOS al iniciar la Central.
def inicializar_estado_cps():
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute("UPDATE CP SET estado = 'DESCONECTADO'")
            conn.commit()
            safe_log("[CENTRAL] Todos los CPs marcados como DESCONECTADOS al iniciar")
        actualizar_pantalla.set()  # Forzamos un refresco inicial de la GUI
    except sqlite3.Error as e:
        safe_log(f"[CENTRAL] [BBDD] Error inicializando estados de CPs: {e}")

# ======================================================================
# Lógica de negocio con KAFKA
# ======================================================================

# Hilos de consumidores
def consume_loop(topic, producer, consumer):
    conn = None
    try:
        # Creamos una conexión para cada hilo
        conn = sqlite3.connect(BBDD, check_same_thread=False)
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue
            for _tp, msgs in records.items():
                for msg in msgs:
                    event = msg.value
                    try:
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
                        safe_log(f"[CENTRAL] Excepción tramitando topic {topic}: {e}")
    except Exception as e:
        safe_log(f"[CENTRAL] Excepción en hilo de consumo ({topic}): {e}")
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

# Cambiar el estado del CP debido a alguna accion relacionada con un suministro
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
    
    actualizar_pantalla.set()

# Monitorizar varios datos si el CP se haya suministrando
def monitorizar_consumo_CP(event):
    id_cp = str(event.get("idCP"))

    CP_CONSUMPTION_DATA[id_cp] = {
        "kwh": float(event.get("consumo", 0.0)),
        "importe": float(event.get("importe", 0.0)),
        "conductor": event.get("conductor", "desconocido"),
    }
    actualizar_pantalla.set()

# Central valida la disponibilidad del CP y, si procede, autoriza
def procesar_peticion_suministro(event, conn, producer):
    safe_log(f"[CENTRAL] Petición de recarga: {event}")
    id_cp = str(event.get("idCP"))
    id_driver = str(event.get("idDriver")) if event.get("idDriver") is not None else None

    if not id_cp:
        safe_log("[CENTRAL] ERROR: Petición sin idCP.")
        return

    cur = conn.cursor()
    cur.execute("SELECT estado FROM CP WHERE idCP = ?", (id_cp,))
    row = cur.fetchone()
    
    # Si no se encuentra el CP en la BD, mandar noti al conductor
    if row is None:
        safe_log(f"[CENTRAL] ERROR: CP {id_cp} no está registrado en la BD")
        if id_driver is not None:
            try:
                payload_cp = {"idCP": id_cp, "idDriver": id_driver, "authorize": "NO"}
                producer.send(AUTHORIZE_SUPPLY, payload_cp)
                producer.flush()
            except Exception as e:
                safe_log(f"[CENTRAL] Error notificando al driver que el CP no existe: {e}")
        return

    # Obtener estado del CP
    estado = row[0]

    # Si el CP se encuentra disponible, entonces entonces autoriza el suministro notificando al CP y al driver
    if estado == "ACTIVADO":
        if id_driver is not None:
            try:
                payload_cp = {"idCP": id_cp, "idDriver": id_driver, "authorize": "YES"}
                producer.send(AUTHORIZE_SUPPLY, payload_cp)
                producer.flush()
                safe_log(f"[CENTRAL] CP {id_cp} disponible; permiso para comenzar el suministro CONCEDIDO")
            except Exception as e:
                safe_log(f"[CENTRAL] Error enviando autorización CP: {e}")
    
    # Si el CP no se encuentra disponible, rechazar petición
    else:
        safe_log(f"[CENTRAL] CP {id_cp} NO DISPONIBLE (estado: {estado}).")
        try:
            payload_cp = {"idCP": id_cp, "idDriver": id_driver, "authorize": "NO"}
            producer.send(AUTHORIZE_SUPPLY, payload_cp)
            producer.flush()
            safe_log(f"[CENTRAL] CP {id_cp} disponible; permiso para comenzar el suministro DENEGADO")
        except Exception as e:
            safe_log(f"[CENTRAL] Error enviando autorización CP: {e}")


# Enviar ticket al conductor de que el suministro ha finalizado
def enviar_ticket(event, conn, producer):
    id_cp = str(event.get("idCP"))
    ticket = event.get("ticket", {})
    estado = str(ticket.get("estado", ""))

    # Mandar ticket el driver
    try:
        producer.send(DRIVER_SUPPLY_COMPLETE, {"ticket": ticket})
        producer.flush()
        safe_log(f"[CENTRAL] Suministro finalizado CP {id_cp}, ticket reenviado al Driver")
    except Exception as e:
        safe_log(f"[CENTRAL] Error reenviando ticket al Driver: {e}")

    # Guardar el ticket en la BD
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

    # Si el suministro se ha completado con éxito, marcar el CP como activado de nuevo
    if (estado == "COMPLETADO"):
        cur = conn.cursor()
        cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("ACTIVADO", id_cp))
        conn.commit()
        
    # Borrar los datos de consumo del dict
    CP_CONSUMPTION_DATA.pop(id_cp, None)
    actualizar_pantalla.set()

# Mandar una lista al driver solicitante con los suministros completados
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
# Lógica de negocio con SOCKETS
# ======================================================================

# Servidor TCP para la comunicación entre Central y los distintos CPs
def tcp_server(listen_port, producer):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", int(listen_port)))
    server.listen()
    while True:
        socket_conn, addr = server.accept()
        threading.Thread(target=handle_tcp_client,args=(socket_conn,addr, producer),daemon=True).start()

# Socket concreto entre Central y un CP_Monitor 
def handle_tcp_client(socket_conn, addr, producer):
    db_conn = None
    id_cp_asociado = None
    try:
        db_conn = sqlite3.connect(BBDD, check_same_thread=False)
        conn_file = socket_conn.makefile('r', encoding='utf-8')

        # Va leyendo las lineas del archivo JSON separadas por '\n'
        while not stop_event.is_set():
            line = conn_file.readline() # Lee linea
            if not line: # Si se ha llegado al final del archivo se sale del bucle
                break
            line = line.strip() # Elimina espacios en blanco, saltos de linea, etc.
            if not line: # Si después de limpiar la línea queda vacía => la ignora y pasa a la siguiente iteración
                continue

            # Trata de leer el contenido
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                safe_log(f"[CENTRAL][TCP] JSON inválido desde {addr}: {line}")
                continue

            # Obtiene tipo de mensaje y CP del que proviene
            msg_type = msg.get("type")
            id_cp_asociado = msg.get("idCP", id_cp_asociado)

            # Despachamos al módulo que toque
            try:
                if msg_type == "register":
                    registrar_CP(msg, db_conn)
                elif msg_type == "alert":
                    safe_log(f"[CENTRAL][TCP] ALERTA de MONITOR {msg.get('idCP')}: {msg.get('alerta')}")
                elif msg_type == "health":
                    comprobar_salud_CP({"idCP": msg.get("idCP"), "salud": msg.get("salud")}, db_conn)
            except Exception as e:
                safe_log(f"[CENTRAL][TCP] Error tramitando mensaje de {addr}: {e}")

    except Exception as e:
        safe_log(f"[CENTRAL][TCP] Error en cliente {addr}: {e}")

    finally:
        # Marca CP como desconectado si había uno asociado
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

# Registrar un CP
def registrar_CP(event, conn):
    id_cp = str(event.get("idCP"))
    precio = float(event.get("precio", 0))
    ubicacion = event.get("ubicacion", "")
    upsert_cp(conn, id_cp, "ACTIVADO", precio, ubicacion)
    safe_log(f"[CENTRAL] Registrado/actualizado CP {id_cp} en BD")
    actualizar_pantalla.set()

# Monitorizar la salud del CP
def comprobar_salud_CP(event, conn):
    id_cp = str(event.get("idCP"))
    salud = event.get("salud")
    cur = conn.cursor()
    cur.execute("SELECT estado FROM CP WHERE idCP = ?", (id_cp,))
    row = cur.fetchone()
    estado_actual = row[0] if row else None

    # Si el CP no se encuentra parado por orden de central...
    if estado_actual != "PARADO":
        if salud == "KO":
            if estado_actual != "AVERIADO":
                safe_log(f"[CENTRAL] CP {id_cp} AVERIADO")
                cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("AVERIADO", id_cp))
                conn.commit()
                actualizar_pantalla.set()
        else:  # OK
            if estado_actual != "ACTIVADO" and estado_actual != "SUMINISTRANDO":
                safe_log(f"[CENTRAL] CP {id_cp} RECUPERADO")
                cur.execute("UPDATE CP SET estado = ? WHERE idCP = ?", ("ACTIVADO", id_cp))
                conn.commit()
                actualizar_pantalla.set()

# ======================================================================
# Enviar lista de CPs disponibles a los Drivers
# ======================================================================

# Mandar una lista a driver con los CPs disponibles
def enviar_lista_cps_disponibles(producer):
    while not stop_event.is_set():
        try:
            with sqlite3.connect(BBDD) as conn:
                cur = conn.cursor()
                cur.execute("SELECT idCP FROM CP WHERE estado = 'ACTIVADO'")
                cps = [row[0] for row in cur.fetchall()]
            producer.send(LISTA_CPS_DISPONIBLES, cps)
            producer.flush()
        except Exception as e:
            safe_log(f"[CENTRAL] Error enviando lista de CPs disponibles: {e}")
        time.sleep(5)  # cada 5 segundos

# ======================================================================
# GUI Tkinter
# ======================================================================

class CentralGUI(tk.Tk):

    # INICIALIZAR INTERFAZ
    def __init__(self, producer):
        super().__init__()
        self.title("EV Central")
        self.geometry("1000x650")
        self.producer = producer

        # Tabla con la monitorización de los CPs
        cols = ("idCP", "Estado", "Precio €/kWh", "Ubicación", "Consumo kWh", "Importe €", "Conductor")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=16)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, stretch=True, width=120)
        self.tree.column("idCP", width=80)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=(10, 6))

        # Botones para mandar órdenes a los CPs
        btn_frame = tk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=6)
        tk.Button(btn_frame, text="Parar un CP", command=self.parar_sel).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Reanudar un CP", command=self.reanudar_sel).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Parar TODOS", command=self.parar_todos).pack(side=tk.LEFT, padx=15)
        tk.Button(btn_frame, text="Reanudar TODOS", command=self.reanudar_todos).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Salir", command=self.salir).pack(side=tk.RIGHT, padx=5)

        # Logs
        self.log_text = tk.Text(self, height=10, wrap=tk.WORD, bg="#1e1e1e", fg="#d6ffd6")
        self.log_text.pack(fill=tk.BOTH, expand=False, padx=10, pady=(6, 10))

        # Refrescar pantalla cada tanto
        self.after(500, self.refresco_periodico)

        # De-seleccionar cualquier fila si se clica fuera de los items
        self.tree.bind("<Button-1>", self.click_fuera)

        # Interceptar el cierre de la ventana (botón X)
        self.protocol("WM_DELETE_WINDOW", self.salir)


    # MOSTRAR MENSAJES DE LOG
    def log(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    # REFRESCAR PANTALLA
    def refresco_periodico(self):
        if stop_event.is_set():
            return

        recargar = False

        # Comprobar si se ha ordenado actualizar pantalla debido a algun cambio en los CPs
        if actualizar_pantalla.is_set():
            actualizar_pantalla.clear()
            recargar = True

        # Comprobar si hay CPs suministrando
        cps = self.leer_cps()
        suministrando = any(estado == "SUMINISTRANDO" for _, estado, *_ in cps)
        if suministrando:
            recargar = True

        #  Actualizar la pantalla si hay algún cambio
        if recargar:
            self.actualizar_tabla()

        self.after(1000 if suministrando else REFRESH_MS, self.refresco_periodico)

    # DE-SELECCIONAR UN ITEM SI SE CLICKA FUERA
    def click_fuera(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region not in ("cell", "tree"):
            self.tree.selection_remove(self.tree.selection())

    # LEER INFO DE CPS DE LA BD
    def leer_cps(self):
        try:
            with sqlite3.connect(BBDD) as conn:
                cur = conn.cursor()
                cur.execute("SELECT idCP, estado, precio, ubicacion FROM CP ORDER BY idCP ASC")
                return cur.fetchall()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error leyendo CPs: {e}")
            return []

    # ACTUALIZAR TABLA DE MONITORIZACIÓN DE CPs
    def actualizar_tabla(self):
        selected = self._sel_id()  # Guardar el CP seleccionado antes de refrescar
        cps = self.leer_cps()
        self.tree.delete(*self.tree.get_children())

        # Procesar fila a fila
        for idCP, estado, precio, ubicacion in cps:
            consumo = CP_CONSUMPTION_DATA.get(str(idCP), {})
            kwh = consumo.get("kwh", None)
            importe = consumo.get("importe", None)
            conductor = consumo.get("conductor", "")

            # Añadir leyenda si el CP está PARADO
            estado_display = estado
            if estado == "PARADO":
                estado_display += " (Fuera de servicio)"

            # Solo mostrar consumo e importe si el CP está suministrando
            if estado == "SUMINISTRANDO" and kwh is not None and importe is not None:
                kwh_display = f"{kwh:.2f}"
                importe_display = f"{importe:.2f}"
            else:
                kwh_display = ""
                importe_display = ""

            iid = self.tree.insert("", tk.END,
                values=(idCP, estado_display, precio, ubicacion, kwh_display, importe_display, conductor))

            # Generar items
            bg = COLORS.get(estado, COLORS["DESCONECTADO"])
            self.tree.item(iid, tags=(estado,))
            self.tree.tag_configure(estado, background=bg)

            if idCP == selected:
                self.tree.selection_set(iid)

    # OBTENER EL CP QUE SE ESTÁ SELECCIONANDO
    def _sel_id(self):
        sel = self.tree.selection()
        if not sel:
            return None
        vals = self.tree.item(sel[0], "values")
        return vals[0] if vals else None

    # ORDEN: parar un CP
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
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error PARAR {id_cp}: {e}")

    # ORDEN: reanudar un CP
    def reanudar_sel(self):
        id_cp = self._sel_id()
        if not id_cp:
            self.log("[CENTRAL][GUI] Selecciona un CP primero.")
            return
        try:
            with sqlite3.connect(BBDD) as conn:
                db_execute(conn, "UPDATE CP SET estado = ? WHERE idCP = ?", ("ACTIVADO", id_cp))
            self.producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": id_cp})
            self.producer.flush()
            self.log(f"[CENTRAL] Orden REANUDAR enviada a CP {id_cp}")
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error REANUDAR {id_cp}: {e}")

    # ORDEN: parar TODOS los CPs
    def parar_todos(self):
        try:
            with sqlite3.connect(BBDD) as conn:
                db_execute(conn, "UPDATE CP SET estado = 'PARADO'")
            self.producer.send(CP_CONTROL, {"accion": "PARAR", "idCP": "todos"})
            self.producer.flush()
            self.log("[CENTRAL] Enviada orden PARAR a TODOS")
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error PARAR TODOS: {e}")

    # ORDEN: reanudar TODOS los CPs
    def reanudar_todos(self):
        try:
            with sqlite3.connect(BBDD) as conn:
                db_execute(conn, "UPDATE CP SET estado = 'ACTIVADO'")
            self.producer.send(CP_CONTROL, {"accion": "REANUDAR", "idCP": "todos"})
            self.producer.flush()
            self.log("[CENTRAL] Enviada orden REANUDAR a TODOS")
            actualizar_pantalla.set()
        except Exception as e:
            self.log(f"[CENTRAL][GUI] Error REANUDAR TODOS: {e}")

    # ORDEN: desconectar Central
    def salir(self):
        try:
            stop_event.set()
            self.log("[CENTRAL] Cerrando aplicación...")

            # Cerrar el productor Kafka de forma segura
            try:
                self.producer.flush()
                self.producer.close()
            except Exception:
                pass

            # Destruir ventana después de breve pausa
            self.after(300, self.destroy)
        except Exception:
            # Evita excepciones si ya se cerró
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
    db_host = sys.argv[3]   # Este parametro no es necesario con SQLite

    # Habilitar acceso concurrente sin bloqueos entre los hilos de lectura y escritura.
    with sqlite3.connect(BBDD) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()

    # Producer Kafka
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Consumidores Kafka
    consumers = {
        CP_STATUS : KafkaConsumer(
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

    # Marcar CPs existentes como DESCONECTADOS (reinicio de Central)
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

    # Hilo que informa periódicamente a los drivers sobre CPs disponibles
    t_lista = threading.Thread(target=enviar_lista_cps_disponibles, args=(producer,), daemon=True)
    t_lista.start()

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

#
if __name__ == "__main__":
    main()
import threading
import json
import time
import uuid
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from EV_Topics import *
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

class EVDriverApp:
    def __init__(self, root, broker, driver_id):
        self.root = root
        self.root.title(f"EV Driver {driver_id}")
        self.broker = broker
        self.driver_id = driver_id
        self.cp_disponibles = []
        self.consumo_actual = {}  # {idCP: {"energia": x, "importe": y}}
        self.current_cp = None
        self.kafka_ready = False

        # Inicializar referencias a None
        self.producer = None
        self.consumer_cps = None
        self.consumer_auth = None
        self.consumer_ticket = None
        self.consumer_consumo = None
        self.consumer_historial = None

        # Definimos un evento para saber si nos ha llegado el ticket de un suministro finalizado
        self.ticket_event = threading.Event()
        self.ticket_cp = None

        # Definimos una lista para los suministros completados
        self.suministros_completados = []

        # Crear interfaz PRIMERO (para que la ventana aparezca)
        self.create_ui()

        # Inicializar Kafka en segundo plano
        threading.Thread(target=self.init_kafka, daemon=True).start() 

    # -----------------------------
    # INICIALIZACION KAFKA (en segundo plano)
    # -----------------------------
    def init_kafka(self):
        """Inicializa conexiones Kafka en segundo plano."""
        self.root.after(0, lambda: self.log("Conectando con Kafka..."))
        try:
            # Productor
            self.producer = KafkaProducer(
                bootstrap_servers=self.broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )

            # Consumidores
            session_id = str(uuid.uuid4())[:8]

            # Consumer para lista de CPs - asignacion manual de TODAS las particiones
            self.consumer_cps = KafkaConsumer(
                bootstrap_servers=self.broker,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset='latest',
                enable_auto_commit=False
            )
            # Obtener todas las particiones del topic y asignarlas
            partitions = self.consumer_cps.partitions_for_topic(LISTA_CPS_DISPONIBLES)
            if partitions:
                tps = [TopicPartition(LISTA_CPS_DISPONIBLES, p) for p in partitions]
                self.consumer_cps.assign(tps)
                # Ir al final de todas las particiones
                self.consumer_cps.seek_to_end()
                self.root.after(0, lambda: self.log(f"Escuchando {len(tps)} particion(es) del topic CPs"))
            else:
                # Si no existe el topic, asignar particion 0 por defecto
                tp = TopicPartition(LISTA_CPS_DISPONIBLES, 0)
                self.consumer_cps.assign([tp])
                self.consumer_cps.seek_to_end(tp)
                self.root.after(0, lambda: self.log("Topic CPs no encontrado, usando particion 0"))

            # Otros consumidores con group_id unico
            consumer_config = {
                'bootstrap_servers': self.broker,
                'value_deserializer': lambda m: json.loads(m.decode("utf-8")),
                'auto_offset_reset': 'latest'
            }

            self.consumer_auth = KafkaConsumer(
                AUTHORIZE_SUPPLY,
                group_id=f"driver_{self.driver_id}_auth_{session_id}",
                **consumer_config
            )
            self.consumer_ticket = KafkaConsumer(
                DRIVER_SUPPLY_COMPLETE,
                group_id=f"driver_{self.driver_id}_tickets_{session_id}",
                **consumer_config
            )
            self.consumer_consumo = KafkaConsumer(
                CP_CONSUMPTION,
                group_id=f"driver_{self.driver_id}_consumo_{session_id}",
                **consumer_config
            )
            self.consumer_historial = KafkaConsumer(
                SUMINISTROS_COMPLETADOS,
                group_id=f"driver_{self.driver_id}_historial_{session_id}",
                **consumer_config
            )

            self.kafka_ready = True
            self.root.after(0, lambda: self.log("Conectado a Kafka correctamente"))

            # Crear hilos de escucha
            threading.Thread(target=self.listen_cp_disponibles, daemon=True).start()
            threading.Thread(target=self.listen_authorizations, daemon=True).start()
            threading.Thread(target=self.listen_tickets, daemon=True).start()
            threading.Thread(target=self.listen_consumption, daemon=True).start()
            threading.Thread(target=self.listen_historial, daemon=True).start()

            # Pedir historial de suministros
            self.producer.send(SUPPLY_HISTORY, {"idDriver": self.driver_id})
            self.producer.flush()

        except Exception as e:
            self.root.after(0, lambda err=str(e): self.log(f"Error conectando a Kafka: {err}"))
            self.kafka_ready = False

    # -----------------------------
    # GUI TKINTER
    # -----------------------------
    def create_ui(self):
        # Mostrar CPs disponibles
        frame_lista = ttk.LabelFrame(self.root, text="CPs disponibles para suministro")
        frame_lista.pack(fill="both", expand=True, padx=10, pady=10)

        self.lista_cp = tk.Listbox(frame_lista, height=8)
        self.lista_cp.pack(fill="both", expand=True, padx=5, pady=5)

        frame_botones = ttk.Frame(self.root)
        frame_botones.pack(fill="x", pady=10)

        ttk.Button(frame_botones, text="Solicitar suministro a un CP",
                   command=self.solicitar_suministro).pack(side="left", expand=True, padx=10)
        ttk.Button(frame_botones, text="Cargar JSON con servicios",
                   command=self.cargar_json).pack(side="left", expand=True, padx=10)

        # Mostrar los suministros que se hayen en marcha
        frame_suministro = ttk.LabelFrame(self.root, text="Suministro en marcha")
        frame_suministro.pack(fill="both", expand=True, padx=10, pady=10)

        cols = ("idCP", "Energía (kWh)", "Importe (€)")
        self.tree_consumo = ttk.Treeview(frame_suministro, columns=cols, show="headings", height=5)
        for c in cols:
            self.tree_consumo.heading(c, text=c)
            self.tree_consumo.column(c, stretch=True, width=120)
        self.tree_consumo.pack(fill="both", expand=True, padx=5, pady=5)

        # Mostrar los suministros completados
        frame_completados = ttk.LabelFrame(self.root, text="Suministros completados")
        frame_completados.pack(fill="both", expand=True, padx=10, pady=10)

        cols_comp = ("idCP", "Energía (kWh)", "Importe (€)", "Suministro")
        self.tree_completados = ttk.Treeview(frame_completados, columns=cols_comp, show="headings", height=5)
        for c in cols_comp:
            self.tree_completados.heading(c, text=c)
            self.tree_completados.column(c, stretch=True, width=120)
        self.tree_completados.pack(fill="both", expand=True, padx=5, pady=5)

        # Panel de logs (en lugar de etiqueta de estado)
        self.log_text = tk.Text(self.root, height=10, wrap=tk.WORD, bg="#1e1e1e", fg="#d6ffd6")
        self.log_text.pack(fill="both", expand=False, padx=10, pady=10)

    # Método de log (igual que en Central)
    def log(self, msg):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    # -----------------------------
    # LISTENERS KAFKA
    # -----------------------------

    # Mantenerse a la escucha para ver qué CPs se encuentran disponibles
    def listen_cp_disponibles(self):
        self.root.after(0, lambda: self.log("Esperando lista de CPs..."))
        while True:
            try:
                # Poll con timeout de 3 segundos
                records = self.consumer_cps.poll(timeout_ms=3000)
                if records:
                    for tp, messages in records.items():
                        for msg in messages:
                            data = msg.value
                            if isinstance(data, list):
                                self.cp_disponibles = data
                                # Actualizar GUI en el hilo principal
                                self.root.after(0, self.update_cp_list)
                                if data:
                                    self.root.after(0, lambda d=data: self.log(f"CPs disponibles: {d}"))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self.log(f"Error recibiendo CPs: {err}"))
                time.sleep(2)

    # Mantenerse a la escucha por si llega alguna autorizacion de suministro
    def listen_authorizations(self):
        for msg in self.consumer_auth:
            data = msg.value
            id_driver = str(data.get("idDriver", ""))
            if id_driver != str(self.driver_id):
                continue
            auth = data.get("authorize")
            id_cp = data.get("idCP")
            if auth == "YES":
                self.root.after(0, lambda cp=id_cp: self.log(f"Suministro AUTORIZADO en CP {cp}"))
            else:
                self.root.after(0, lambda cp=id_cp: self.log(f"Suministro DENEGADO en CP {cp}"))
                self.ticket_cp = id_cp
                self.ticket_event.set() # activar evento para q pase a la siguiente linea del fichero

    # Mantenerse a la escucha por si llega algun ticket de finalizacion de suministro
    def listen_tickets(self):
        for msg in self.consumer_ticket:
            event = msg.value
            ticket = event.get("ticket", {})
            id_driver = str(ticket.get("idDriver", ""))

            # Si el ticket no va dirigido a este conductor, entonces lo ignora
            if id_driver != str(self.driver_id):
                continue

            # Obtener info del ticket
            id_cp = str(ticket.get("idCP", ""))
            estado = str(ticket.get("estado", ""))
            motivo = str(ticket.get("motivo", ""))
            energia = ticket.get("energia", 0)
            importe = ticket.get("precio_total", 0)

            # Guardar ticket
            self.suministros_completados.append({
                    "idCP": id_cp,
                    "energia": energia,
                    "importe": importe,
                    "estado": estado,
                })
            self.root.after(0, self.update_completados_table)

            # Mostrar mensaje pertinente
            if estado == "COMPLETADO":
                self.root.after(0, lambda cp=id_cp, e=energia, i=importe: self.log(f"Recarga completada con éxito en CP {cp}: {e:.2f} kWh, {i:.2f} €"))
            else:
                self.root.after(0, lambda cp=id_cp, m=motivo, e=energia, i=importe: self.log(f"Recarga interrumpida en CP {cp} {m}: {e:.2f} kWh, {i:.2f} €"))

            # Eliminar la entrada de la tabla de suministrando actualmente
            self.consumo_actual.pop(id_cp, None)
            self.root.after(0, self.update_consumo_table)

            # Avisar a procesar_servicios() de que llegó el ticket
            self.ticket_cp = id_cp
            self.ticket_event.set()

    # Mantenerse a la escucha para obtener datos acerca del suministro
    def listen_consumption(self):
        for msg in self.consumer_consumo:
            event = msg.value
            id_cp = str(event.get("idCP"))
            conductor = str(event.get("conductor", ""))
            if conductor != str(self.driver_id):
                continue  # solo mostrar consumos del conductor actual
            energia = float(event.get("consumo", 0))
            importe = float(event.get("importe", 0))
            self.consumo_actual[id_cp] = {"energia": energia, "importe": importe}
            self.root.after(0, self.update_consumo_table)

    # Mantenerse a la escucha para obtener datos acerca de los suministros completados
    def listen_historial(self):
        for msg in self.consumer_historial:
            registros = msg.value
            # Filtrar solo los del conductor actual
            propios = [r for r in registros if str(r.get("idDriver")) == str(self.driver_id)]
            if not propios:
                continue
            self.suministros_completados = [
                {
                    "idDriver": r.get("idDriver"),
                    "idCP": r.get("idCP"),
                    "energia": r.get("energia"),
                    "importe": r.get("importe"),
                    "estado": r.get("estado", "DESCONOCIDO")
                }
                for r in propios
            ]
            self.root.after(0, self.update_completados_table)

    # -----------------------------------------------------------
    # LÓGICA GENERAL PARA REFRESCAR LAS DISTINTAS PANTALLAS
    # -----------------------------------------------------------
    
    # CPs disponibles
    def update_cp_list(self):
        self.lista_cp.delete(0, tk.END)
        for cp in self.cp_disponibles:
            self.lista_cp.insert(tk.END, f"CP {cp}")

    # Suministros en marcha
    def update_consumo_table(self):
        self.tree_consumo.delete(*self.tree_consumo.get_children())
        for id_cp, datos in self.consumo_actual.items():
            self.tree_consumo.insert("", tk.END, values=(
                id_cp,
                f"{datos['energia']:.2f}",
                f"{datos['importe']:.2f}"
            ))

    # Suministros completados
    def update_completados_table(self):
        self.tree_completados.delete(*self.tree_completados.get_children())
        for item in self.suministros_completados:
            self.tree_completados.insert("", tk.END, values=(
                item["idCP"],
                f"{item['energia']:.2f}",
                f"{item['importe']:.2f}",
                item["estado"],
            ))

    # --------------------------------
    # LÓGICA PARA LOS BOTONES
    # --------------------------------

    # Solicitar suministro a un CP concreto
    def solicitar_suministro(self):
        seleccion = self.lista_cp.curselection()
        if not seleccion:
            messagebox.showwarning("Atención", "Seleccione un CP de la lista.")
            return
        cp_id = self.cp_disponibles[seleccion[0]]
        self.enviar_solicitud(cp_id)

    # Cargar fichero JSON con los servicios a pedir
    def cargar_json(self):
        ruta = filedialog.askopenfilename(filetypes=[("Archivos JSON", "*.json")])
        if not ruta:
            return
        try:
            with open(ruta, "r", encoding="utf-8") as f:
                servicios = json.load(f)
            if not isinstance(servicios, list):
                raise ValueError("El JSON debe contener una lista de servicios.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo leer el archivo JSON:\n{e}")
            return
        threading.Thread(target=self.procesar_servicios, args=(servicios,), daemon=True).start()

    # Procesar las peticiones dentro del fichero
    def procesar_servicios(self, servicios):
        for servicio in servicios:
            cp_id = servicio.get("idCP")
            if not cp_id:
                continue

            self.ticket_event.clear()  # resetear evento antes de enviar
            self.enviar_solicitud(cp_id)

            # Esperar hasta que llegue el ticket correspondiente
            while True:
                self.ticket_event.wait()  # se desbloquea cuando llegue un ticket
                if self.ticket_cp == cp_id:
                    break
                else:
                    # Si el ticket era de otro CP, sigue esperando
                    self.ticket_event.clear()

            self.root.after(0, lambda: self.log("Pasando al siguiente servicio...\n"))

            # Esperar 4 segundos antes de pasar al siguiente CP
            time.sleep(4)

        self.root.after(0, lambda: self.log("Suministros finalizados."))

    # Solicitar suministro a Central para un CP concreto del fichero
    def enviar_solicitud(self, cp_id):
        if not self.kafka_ready or not self.producer:
            self.root.after(0, lambda: self.log("Kafka no esta conectado. Espera un momento..."))
            return
        try:
            request = {"idDriver": self.driver_id, "idCP": cp_id}
            self.producer.send(SUPPLY_REQUEST_TO_CENTRAL, request)
            self.producer.flush()
            self.root.after(0, lambda cp=cp_id: self.log(f"Solicitud de suministro enviada a Central para CP {cp}"))
        except Exception as e:
            self.root.after(0, lambda: self.log("Imposible conectar con la CENTRAL."))

# -----------------------------
# Main
# -----------------------------
def main():
    import sys
    if len(sys.argv) < 3:
        print("Uso: py EV_Driver.py <broker_ip:puerto> <driver_id>")
        sys.exit(1)

    broker = sys.argv[1]
    driver_id = sys.argv[2]

    root = tk.Tk()
    app = EVDriverApp(root, broker, driver_id)
    root.mainloop()

#
if __name__ == "__main__":
    main()
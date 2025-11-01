import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from EV_Topics import *

class EVDriverApp:
    def __init__(self, root, broker, driver_id):
        self.root = root
        self.root.title(f"EV Driver {driver_id}")
        self.broker = broker
        self.driver_id = driver_id
        self.cp_disponibles = []
        self.consumo_actual = {}  # {idCP: {"kwh": x, "importe": y}}
        self.current_cp = None

        # Productor
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        # Consumidores
        self.consumer_cps = KafkaConsumer(
            LISTA_CPS_DISPONIBLES,
            bootstrap_servers=self.broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=f"driver_{driver_id}_cps",
            auto_offset_reset='earliest'
        )
        self.consumer_auth = KafkaConsumer(
            AUTHORIZE_SUPPLY,
            bootstrap_servers=self.broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=f"driver_{driver_id}_auth",
            auto_offset_reset='earliest'
        )
        self.consumer_ticket = KafkaConsumer(
            DRIVER_SUPPLY_COMPLETE,
            bootstrap_servers=self.broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=f"driver_{driver_id}_tickets",
            auto_offset_reset='earliest'
        )
        self.consumer_consumo = KafkaConsumer(
            CP_CONSUMPTION,
            bootstrap_servers=self.broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=f"driver_{driver_id}_consumo",
            auto_offset_reset='earliest'
        )

        # Crear interfaz
        self.create_ui()

        # Crear hilos de escucha
        threading.Thread(target=self.listen_cp_disponibles, daemon=True).start()
        threading.Thread(target=self.listen_authorizations, daemon=True).start()
        threading.Thread(target=self.listen_tickets, daemon=True).start()
        threading.Thread(target=self.listen_consumption, daemon=True).start()

        # Definimos un evento para saber si nos ha llegado el ticket de un suministro finalizado
        self.ticket_event = threading.Event()
        self.ticket_cp = None

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

        cols = ("idCP", "Consumo (kWh)", "Importe (€)")
        self.tree_consumo = ttk.Treeview(frame_suministro, columns=cols, show="headings", height=5)
        for c in cols:
            self.tree_consumo.heading(c, text=c)
            self.tree_consumo.column(c, stretch=True, width=120)
        self.tree_consumo.pack(fill="both", expand=True, padx=5, pady=5)

        self.status_label = ttk.Label(self.root, text="Esperando CPs disponibles...")
        self.status_label.pack(pady=5)

    # -----------------------------
    # LISTENERS KAFKA
    # -----------------------------

    # Mantenerse a la escucha para ver qué CPs se encuentran disponibles
    def listen_cp_disponibles(self):
        for msg in self.consumer_cps:
            data = msg.value
            if isinstance(data, list):
                self.cp_disponibles = data
                self.update_cp_list()

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
                self.status_label.config(text=f"Suministro AUTORIZADO en CP {id_cp}")
            else:
                self.status_label.config(text=f"Suministro DENEGADO en CP {id_cp}")
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
            energia = ticket.get("energia", 0)
            importe = ticket.get("precio_total", 0)
            self.status_label.config(
                text=f"Recarga completada en CP {id_cp}: {energia:.2f} kWh, {importe:.2f} €"
            )

            # Eliminar la entrada de la tabla de suministrando actualmente
            self.consumo_actual.pop(id_cp, None)
            self.update_consumo_table()

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
            kwh = float(event.get("consumo", 0))
            importe = float(event.get("importe", 0))
            self.consumo_actual[id_cp] = {"kwh": kwh, "importe": importe}
            self.update_consumo_table()

    # -----------------------------
    # LÓGICA GENERAL
    # -----------------------------
    
    def update_cp_list(self):
        self.lista_cp.delete(0, tk.END)
        for cp in self.cp_disponibles:
            self.lista_cp.insert(tk.END, f"CP {cp}")

    def update_consumo_table(self):
        self.tree_consumo.delete(*self.tree_consumo.get_children())
        for id_cp, datos in self.consumo_actual.items():
            self.tree_consumo.insert("", tk.END, values=(
                id_cp,
                f"{datos['kwh']:.2f}",
                f"{datos['importe']:.2f}"
            ))

    # -----------------------------
    # LÓGICA PARA LOS BOTONES
    # -----------------------------

    # Solicitar suministro a un CP concreto
    def solicitar_suministro(self):
        seleccion = self.lista_cp.curselection()
        if not seleccion:
            messagebox.showwarning("Atención", "Seleccione un CP de la lista.")
            return
        cp_id = self.cp_disponibles[seleccion[0]]
        self.enviar_solicitud(cp_id)
        self.status_label.config(text=f"Solicitud de suministro enviada al CP {cp_id}")

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
            self.status_label.config(text=f"Solicitud enviada a CP {cp_id}, esperando ticket...")

            # Esperar hasta que llegue el ticket correspondiente
            while True:
                self.ticket_event.wait()  # se desbloquea cuando llegue un ticket
                if self.ticket_cp == cp_id:
                    break
                else:
                    # Si el ticket era de otro CP, sigue esperando
                    self.ticket_event.clear()

            self.status_label.config(text=f"Ticket recibido de CP {cp_id}. Pasando al siguiente...")

            # Esperar 4 segundos antes de pasar al siguiente CP
            time.sleep(4)

        self.status_label.config(text=f"Suministros finalizados.")

    # Solicitar suministro a Central para un CP concreto del fichero
    def enviar_solicitud(self, cp_id):
        request = {"idDriver": self.driver_id, "idCP": cp_id}
        self.producer.send(SUPPLY_REQUEST_TO_CENTRAL, request)
        self.producer.flush()

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

if __name__ == "__main__":
    main()
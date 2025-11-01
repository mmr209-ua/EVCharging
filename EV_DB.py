#EV_DB   
import sqlite3

BBDD = "Base_Datos.sqlite"

try:
    with open(BBDD, 'x'):
        pass
except:
        print(f"{BBDD} ya creada")

with sqlite3.connect(BBDD) as conn:

    print(f"Conectado a {BBDD}")

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CP (
            idCP VARCHAR(10) PRIMARY KEY,
            estado TEXT NOT NULL CHECK (estado IN ('ACTIVADO','PARADO','SUMINISTRANDO','AVERIADO','DESCONECTADO')),
            precio DECIMAL(10,2) NOT NULL,
            ubicacion VARCHAR(100) NOT NULL
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CONDUCTOR (
            idConductor INTEGER PRIMARY KEY AUTOINCREMENT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CONSUMO (
            idConsumo INTEGER PRIMARY KEY AUTOINCREMENT,
            conductor INT NOT NULL,
            cp VARCHAR(10) NOT NULL,
            consumo DECIMAL(10,2) NOT NULL,
            importe DECIMAL(10,2) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (conductor) REFERENCES CONDUCTOR(idConductor),
            FOREIGN KEY (cp) REFERENCES CP(idCP)
        )
    """)
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

    # Tabla de Puntos de Recarga (CP) - Release 2
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CP (
            idCP VARCHAR(10) PRIMARY KEY,
            estado TEXT NOT NULL CHECK (estado IN ('ACTIVADO','PARADO','SUMINISTRANDO','AVERIADO','DESCONECTADO')),
            precio DECIMAL(10,2) NOT NULL,
            ubicacion VARCHAR(100) NOT NULL,
            auth_token TEXT,
            encryption_key TEXT,
            authenticated BOOLEAN DEFAULT 0,
            paused_by_weather BOOLEAN DEFAULT 0
        );
    """)

    # Migrar tabla existente si faltan columnas (para compatibilidad)
    try:
        cursor.execute("ALTER TABLE CP ADD COLUMN auth_token TEXT")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE CP ADD COLUMN encryption_key TEXT")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE CP ADD COLUMN authenticated BOOLEAN DEFAULT 0")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE CP ADD COLUMN paused_by_weather BOOLEAN DEFAULT 0")
    except:
        pass

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
            estado TEXT NOT NULL CHECK (estado IN ('COMPLETADO','INTERRUMPIDO')),
            consumo DECIMAL(10,2) NOT NULL,
            importe DECIMAL(10,2) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (conductor) REFERENCES CONDUCTOR(idConductor),
            FOREIGN KEY (cp) REFERENCES CP(idCP)
        )
    """)

    # Tabla de Auditoria - Release 2
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS AUDIT_LOG (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT NOT NULL,
            source_ip TEXT,
            source_id TEXT,
            action TEXT NOT NULL,
            parameters TEXT,
            result TEXT
        )
    """)

    # Tabla de Alertas Climaticas - Release 2
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS WEATHER_ALERTS (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ubicacion VARCHAR(100) NOT NULL,
            temperatura DECIMAL(5,2),
            alert_active BOOLEAN
        )
    """)

    conn.commit()
    print("Tablas creadas/actualizadas correctamente")

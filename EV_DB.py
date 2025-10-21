#EV_DB
import mysql.connector
from mysql.connector import errorcode

DB_NAME = "EV_DB"
DB_USER = "root"
DB_PASSWORD = "programacion2"

def get_connection(db_host):
    """
    Devuelve una conexión a la BD usando el usuario Kafka.
    """
    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        # Si la BD no existe, la creamos
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            conn = mysql.connector.connect(
                host=db_host,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            conn.commit()
            cursor.close()
            conn.close()
            # Conectamos de nuevo a la BD recién creada
            return mysql.connector.connect(
                host=db_host,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
        else:
            raise


def init_db(conn):
    cursor = conn.cursor()

    # CORRECCIÓN ERROR 5: Mejorar estructura de tablas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CP (
            idCP VARCHAR(20) PRIMARY KEY,
            estado ENUM('ACTIVADO','PARADO','SUMINISTRANDO','AVERIADO','DESCONECTADO') NOT NULL DEFAULT 'DESCONECTADO',
            precio DECIMAL(10,2) NOT NULL DEFAULT 0.25,
            ubicacion VARCHAR(100) NOT NULL DEFAULT 'Desconocida',
            consumo_actual DECIMAL(10,2) DEFAULT 0
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CONDUCTOR (
            idConductor VARCHAR(20) PRIMARY KEY
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CONSUMO (
            idConsumo INT AUTO_INCREMENT PRIMARY KEY,
            conductor VARCHAR(20) NOT NULL,
            cp VARCHAR(20) NOT NULL,
            consumo DECIMAL(10,2) NOT NULL,
            importe DECIMAL(10,2) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (conductor) REFERENCES CONDUCTOR(idConductor),
            FOREIGN KEY (cp) REFERENCES CP(idCP)
        );
    """)

    conn.commit()
    cursor.close()
    print("[DB] Tablas inicializadas correctamente.")
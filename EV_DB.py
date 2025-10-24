import mysql.connector

def init_db(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CP (
            idCP INT AUTO_INCREMENT PRIMARY KEY,
            estado ENUM('ACTIVADO','PARADO','SUMINISTRANDO','AVERIADO','DESCONECTADO') NOT NULL,
            precio DECIMAL(10,2) NOT NULL,
            ubicacion VARCHAR(100) NOT NULL
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CONDUCTOR (
            idConductor INT AUTO_INCREMENT PRIMARY KEY
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CONSUMO (
            idConsumo INT AUTO_INCREMENT PRIMARY KEY,
            conductor INT NOT NULL,
            cp INT NOT NULL,
            consumo DECIMAL(10,2) NOT NULL,
            importe DECIMAL(10,2) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (conductor) REFERENCES CONDUCTOR(idConductor),
            FOREIGN KEY (cp) REFERENCES CP(idCP)
        )
    """)

    conn.commit()

def get_connection(db_host):
    conn = mysql.connector.connect(
        host=db_host,
        user="root",
        password="password",
        database="EVCharging",
        autocommit=True
    )

    return conn

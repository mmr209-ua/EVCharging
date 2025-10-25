# EV_Topics.py
"""
Tópicos Kafka y utilidades de ayuda.
Requiere `kafka-python` si quieres usar ensure_topics().
"""
from dataclasses import dataclass

# Nombres de tópicos
CENTRAL_TELEMETRY = "central.telemetry"           # Telemetría de CPs -> Central
CENTRAL_EVENTS    = "central.events"              # Eventos generales hacia Central (arranque CP, averías, etc.)

DRIVER_REQUESTS   = "driver.requests"             # Peticiones de los Drivers -> Central
DRIVER_EVENTS_FMT = "driver.{driver_id}.events"   # Respuestas para un Driver concreto

CP_COMMANDS_FMT   = "cp.{cp_id}.commands"         # Comandos hacia un CP en concreto
CP_EVENTS         = "cp.events"                   # Eventos generales de CP hacia Central (fallback)

# Tipos de mensajes (valor del campo 'type')
MSG_CP_REGISTER   = "CP_REGISTER"
MSG_CP_STATUS     = "CP_STATUS"                   # cambios de estado reportados por monitor
MSG_CP_TELEMETRY  = "CP_TELEMETRY"
MSG_CP_TICKET     = "CP_TICKET"

MSG_DRV_REQUEST   = "DRV_REQUEST"
MSG_DRV_UPDATE    = "DRV_UPDATE"
MSG_DRV_TICKET    = "DRV_TICKET"

MSG_CMD_START     = "CMD_START"
MSG_CMD_STOP      = "CMD_STOP"
MSG_CMD_OUTOFORDER= "CMD_OUT_OF_ORDER"
MSG_CMD_RESUME    = "CMD_RESUME"

# Estados de CP
ST_ACTIVADO       = "ACTIVADO"
ST_PARADO         = "PARADO"
ST_SUMINISTRANDO  = "SUMINISTRANDO"
ST_AVERIADO       = "AVERIADO"
ST_DESC           = "DESCONECTADO"

# Utilidad para crear topics (opcional)
def ensure_topics(bootstrap_servers: str):
    """
    Crea topics si no existen. Requiere kafka-python>=2.0.2.
    """
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="ev_topics_admin")
        existing = set(admin.list_topics())
        to_create = []
        base_topics = [
            CENTRAL_TELEMETRY, CENTRAL_EVENTS, DRIVER_REQUESTS, CP_EVENTS
        ]
        # algunos tópicos se crean dinámicamente al primer uso (por driver o cp)
        for t in base_topics:
            if t not in existing:
                to_create.append(NewTopic(name=t, num_partitions=3, replication_factor=1))
        if to_create:
            admin.create_topics(new_topics=to_create, validate_only=False)
        admin.close()
        print("[EV_Topics] Topics base verificados/creados.")
    except Exception as e:
        print(f"[EV_Topics] Aviso: no se pudieron crear/verificar topics: {e}")
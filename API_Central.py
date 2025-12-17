# API_Central.py - API REST para Release 2
# Expone estado de Central para Front Web y EV_W

import sqlite3
import json
from flask import Flask, jsonify, request
from flask_cors import CORS

# Importar modulo de cifrado (Release 2)
try:
    from crypto_utils import generate_encryption_key
except ImportError:
    print("[API_CENTRAL] ADVERTENCIA: crypto_utils no encontrado, cifrado deshabilitado")
    generate_encryption_key = lambda: "dummy_key"

app = Flask(__name__)

# ======================================================================
# SISTEMA DE AUDITORIA
# ======================================================================

def log_audit(event_type: str, source_ip: str, source_id: str, action: str, parameters: dict, result: str):
    """
    Registra evento en tabla AUDIT_LOG.

    Args:
        event_type: 'AUTH', 'WEATHER_ALERT', 'CONTROL_ORDER', etc.
        source_ip: IP del cliente
        source_id: idCP, idDriver o identificador
        action: Descripcion de la accion
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
            print(f"[API_CENTRAL][AUDIT] {event_type} | {source_ip} | {source_id} | {action} | {result}")
    except Exception as e:
        print(f"[API_CENTRAL][AUDIT] Error registrando evento: {e}")

CORS(app)  # Permitir CORS para el Front Web

BBDD = "Base_Datos.sqlite"

# Variable global para el producer de Kafka (se inyecta desde EV_Central)
kafka_producer = None
# Evento para actualizar pantalla de Central (se inyecta desde EV_Central)
actualizar_pantalla_event = None

def set_kafka_producer(producer):
    """Inyecta el producer de Kafka desde EV_Central."""
    global kafka_producer
    kafka_producer = producer

def set_actualizar_pantalla(event):
    """Inyecta el evento de actualizar pantalla desde EV_Central."""
    global actualizar_pantalla_event
    actualizar_pantalla_event = event

def trigger_pantalla_update():
    """Dispara actualizacion de la pantalla de Central."""
    if actualizar_pantalla_event:
        actualizar_pantalla_event.set()

# ======================================================================
# UTILIDADES BD
# ======================================================================

def db_fetchall(query, params=()):
    try:
        with sqlite3.connect(BBDD) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]
    except sqlite3.Error as e:
        print(f"[API_CENTRAL] Error BD: {e}")
        return []

def db_execute(query, params=()):
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            return True
    except sqlite3.Error as e:
        print(f"[API_CENTRAL] Error BD: {e}")
        return False

# ======================================================================
# ENDPOINTS
# ======================================================================

@app.route('/cps', methods=['GET'])
def get_cps():
    """
    Lista todos los CPs con su estado.

    Response:
    [
        {
            "idCP": "1",
            "estado": "ACTIVADO",
            "precio": 0.30,
            "ubicacion": "Madrid",
            "authToken": "abc123...",
            "authenticated": true,
            "paused_by_weather": false
        }
    ]
    """
    rows = db_fetchall("""
        SELECT idCP, estado, precio, ubicacion, auth_token, authenticated, paused_by_weather
        FROM CP ORDER BY idCP ASC
    """)

    result = []
    for r in rows:
        result.append({
            "idCP": r["idCP"],
            "estado": r["estado"],
            "precio": r["precio"],
            "ubicacion": r["ubicacion"],
            "authToken": r["auth_token"][:8] + "..." if r["auth_token"] else None,
            "authenticated": bool(r["authenticated"]),
            "paused_by_weather": bool(r["paused_by_weather"])
        })

    return jsonify(result), 200

@app.route('/drivers', methods=['GET'])
def get_drivers():
    """
    Lista conductores registrados.

    Response:
    [{"idConductor": 1}, {"idConductor": 2}]
    """
    rows = db_fetchall("SELECT idConductor FROM CONDUCTOR")
    return jsonify(rows), 200

@app.route('/transactions', methods=['GET'])
def get_transactions():
    """
    Lista las ultimas transacciones.

    Query params:
        limit: numero maximo de resultados (default 50)

    Response:
    [
        {
            "idConsumo": 1,
            "conductor": 1,
            "cp": "1",
            "estado": "COMPLETADO",
            "consumo": 25.5,
            "importe": 6.38,
            "timestamp": "2025-12-11 10:30:00"
        }
    ]
    """
    limit = request.args.get('limit', 50, type=int)
    rows = db_fetchall("""
        SELECT idConsumo, conductor, cp, estado, consumo, importe, timestamp
        FROM CONSUMO
        ORDER BY timestamp DESC
        LIMIT ?
    """, (limit,))

    return jsonify(rows), 200

@app.route('/weather_alert', methods=['POST'])
def weather_alert():
    """
    Recibe alertas climaticas de EV_W.

    Request JSON:
    {
        "ubicacion": "Madrid",
        "alert": true,
        "temperatura": -5.2
    }

    Acciones:
    - Si alert=true (temp < 0C):
      - Identificar CPs en esa ubicacion
      - Marcar para parar (paused_by_weather=1)
      - Enviar orden PARAR via Kafka
    - Si alert=false (temp >= 0C):
      - Cancelar alerta, enviar REANUDAR
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": True, "message": "Datos requeridos"}), 400

    ubicacion = data.get('ubicacion')
    alert = data.get('alert')
    temperatura = data.get('temperatura', 0)

    if not ubicacion:
        return jsonify({"error": True, "message": "Ubicacion requerida"}), 400

    try:
        # Registrar alerta en BD
        db_execute("""
            INSERT INTO WEATHER_ALERTS (ubicacion, temperatura, alert_active)
            VALUES (?, ?, ?)
        """, (ubicacion, temperatura, alert))

        # Obtener CPs en esa ubicacion (busqueda case-insensitive)
        cps = db_fetchall("SELECT idCP, estado FROM CP WHERE LOWER(ubicacion) = LOWER(?)", (ubicacion,))

        print(f"[API_CENTRAL] Buscando CPs en ubicacion '{ubicacion}' -> Encontrados: {len(cps)}")
        for cp in cps:
            print(f"[API_CENTRAL]   CP {cp['idCP']} estado={cp['estado']}")

        affected_cps = []

        if alert:
            # Temperatura bajo cero - PARAR CPs
            for cp in cps:
                id_cp = cp["idCP"]
                estado = cp["estado"]

                # Marcar como pausado por clima
                db_execute("UPDATE CP SET paused_by_weather = 1 WHERE idCP = ?", (id_cp,))
                print(f"[API_CENTRAL] CP {id_cp}: paused_by_weather = 1")

                if estado == "SUMINISTRANDO":
                    # No interrumpir suministro activo, solo marcar para parar despues
                    affected_cps.append({"idCP": id_cp, "action": "MARKED_FOR_STOP"})
                elif estado == "ACTIVADO":
                    # Parar inmediatamente
                    db_execute("UPDATE CP SET estado = 'PARADO' WHERE idCP = ?", (id_cp,))
                    affected_cps.append({"idCP": id_cp, "action": "STOPPED"})

                    # Enviar orden PARAR via Kafka si hay producer
                    if kafka_producer:
                        try:
                            kafka_producer.send("CP_CONTROL", {"accion": "PARAR", "idCP": id_cp})
                            kafka_producer.flush()
                        except Exception as e:
                            print(f"[API_CENTRAL] Error enviando PARAR a Kafka: {e}")
                else:
                    # CP en otro estado (DESCONECTADO, PARADO, etc.) - solo marcar paused_by_weather
                    affected_cps.append({"idCP": id_cp, "action": "WEATHER_FLAGGED", "estado_actual": estado})

            print(f"[API_CENTRAL] Alerta ACTIVADA para {ubicacion}: {temperatura}C, CPs afectados: {len(affected_cps)}")

        else:
            # Temperatura normal - REANUDAR CPs
            # Obtener CPs con su estado de pausa por clima (busqueda case-insensitive)
            cps = db_fetchall("SELECT idCP, estado, paused_by_weather FROM CP WHERE LOWER(ubicacion) = LOWER(?)", (ubicacion,))

            for cp in cps:
                id_cp = cp["idCP"]
                estado = cp["estado"]
                was_paused_by_weather = cp["paused_by_weather"]

                # Quitar marca de pausa por clima y reactivar si estaba pausado por clima
                if was_paused_by_weather:
                    db_execute("UPDATE CP SET paused_by_weather = 0, estado = 'ACTIVADO' WHERE idCP = ?", (id_cp,))
                    affected_cps.append({"idCP": id_cp, "action": "RESUMED"})

                    # Enviar orden REANUDAR via Kafka
                    if kafka_producer:
                        try:
                            kafka_producer.send("CP_CONTROL", {"accion": "REANUDAR", "idCP": id_cp})
                            kafka_producer.flush()
                        except Exception as e:
                            print(f"[API_CENTRAL] Error enviando REANUDAR a Kafka: {e}")
                else:
                    # Solo quitar marca si no estaba pausado
                    db_execute("UPDATE CP SET paused_by_weather = 0 WHERE idCP = ?", (id_cp,))

            print(f"[API_CENTRAL] Alerta CANCELADA para {ubicacion}: {temperatura}C, CPs reanudados: {len(affected_cps)}")

        # Registrar en auditoria
        log_audit('WEATHER_ALERT', request.remote_addr, 'EV_W',
                 'ALERT_ACTIVATED' if alert else 'ALERT_CANCELLED',
                 {'ubicacion': ubicacion, 'temperatura': temperatura, 'cps_afectados': len(affected_cps)}, 'SUCCESS')

        # Actualizar pantalla de Central para mostrar cambios
        trigger_pantalla_update()

        return jsonify({
            "success": True,
            "message": f"Alerta {'activada' if alert else 'cancelada'} para {ubicacion}",
            "affected_cps": affected_cps
        }), 200

    except Exception as e:
        print(f"[API_CENTRAL] Error procesando alerta: {e}")
        log_audit('WEATHER_ALERT', request.remote_addr, 'EV_W', 'ALERT_ERROR',
                 {'error': str(e)}, 'FAILED')
        return jsonify({"error": True, "message": str(e)}), 500

@app.route('/audit', methods=['GET'])
def get_audit():
    """
    Lista los ultimos eventos de auditoria.

    Query params:
        limit: numero maximo de resultados (default 100)
    """
    limit = request.args.get('limit', 100, type=int)
    rows = db_fetchall("""
        SELECT id, timestamp, event_type, source_ip, source_id, action, parameters, result
        FROM AUDIT_LOG
        ORDER BY timestamp DESC
        LIMIT ?
    """, (limit,))

    return jsonify(rows), 200

@app.route('/weather', methods=['GET'])
def get_weather_status():
    """
    Obtiene el estado actual del clima por ubicacion.
    """
    rows = db_fetchall("""
        SELECT ubicacion, temperatura, alert_active, timestamp
        FROM WEATHER_ALERTS
        WHERE id IN (
            SELECT MAX(id) FROM WEATHER_ALERTS GROUP BY ubicacion
        )
        ORDER BY ubicacion
    """)

    return jsonify(rows), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de health check."""
    return jsonify({"status": "ok", "service": "API_Central"}), 200

@app.route('/cp/<id_cp>/ubicacion', methods=['PUT'])
def cambiar_ubicacion_cp(id_cp):
    """
    Cambia la ubicacion de un CP.

    Request JSON:
    {
        "ubicacion": "Paris"
    }
    """
    data = request.get_json()
    if not data or 'ubicacion' not in data:
        return jsonify({"error": True, "message": "Ubicacion requerida"}), 400

    nueva_ubicacion = data['ubicacion']

    try:
        # Verificar que el CP existe
        rows = db_fetchall("SELECT ubicacion FROM CP WHERE idCP = ?", (id_cp,))
        if not rows:
            return jsonify({"error": True, "message": "CP no encontrado"}), 404

        ubicacion_anterior = rows[0]['ubicacion']

        # Actualizar ubicacion
        db_execute("UPDATE CP SET ubicacion = ? WHERE idCP = ?", (nueva_ubicacion, id_cp))

        print(f"[API_CENTRAL] CP {id_cp}: ubicacion cambiada de '{ubicacion_anterior}' a '{nueva_ubicacion}'")
        log_audit('STATE_CHANGE', request.remote_addr, id_cp, 'CAMBIO_UBICACION',
                 {'ubicacion_anterior': ubicacion_anterior, 'ubicacion_nueva': nueva_ubicacion}, 'SUCCESS')

        # Actualizar pantalla de Central
        trigger_pantalla_update()

        return jsonify({
            "success": True,
            "message": f"Ubicacion de CP {id_cp} cambiada a {nueva_ubicacion}",
            "ubicacion_anterior": ubicacion_anterior,
            "ubicacion_nueva": nueva_ubicacion
        }), 200

    except Exception as e:
        print(f"[API_CENTRAL] Error cambiando ubicacion: {e}")
        return jsonify({"error": True, "message": str(e)}), 500

@app.route('/authenticate', methods=['POST'])
def authenticate_cp():
    """
    Autentica un CP comprobando que existe en la base de datos.
    Si existe, genera y devuelve una encryption_key unica.

    Request JSON:
    {
        "idCP": "1"
    }

    Response JSON (exito):
    {
        "success": true,
        "encryption_key": "clave_generada..."
    }

    Response JSON (error):
    {
        "success": false,
        "error": "Mensaje de error"
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "Datos requeridos"}), 400

    id_cp = str(data.get("idCP", ""))

    if not id_cp:
        return jsonify({"success": False, "error": "idCP es requerido"}), 400

    print(f"[API_CENTRAL] Intento de autenticacion de CP {id_cp}")

    try:
        # Verificar que el CP existe en la base de datos
        rows = db_fetchall("SELECT idCP FROM CP WHERE idCP = ?", (id_cp,))

        if not rows:
            print(f"[API_CENTRAL] CP {id_cp} no registrado en Registry")
            log_audit('AUTH', request.remote_addr, id_cp, 'AUTH_FAILED',
                     {'reason': 'CP not registered'}, 'FAILED')
            return jsonify({"success": False, "error": "CP no registrado. Registrese primero en Registry."}), 404

        # Generar clave de cifrado unica
        encryption_key = generate_encryption_key()

        # Guardar en BD
        db_execute("""
            UPDATE CP SET encryption_key = ?, authenticated = 1, estado = 'ACTIVADO'
            WHERE idCP = ?
        """, (encryption_key, id_cp))

        print(f"[API_CENTRAL] CP {id_cp} AUTENTICADO correctamente")
        log_audit('AUTH', request.remote_addr, id_cp, 'AUTH_SUCCESS',
                 {'encryption_key_prefix': encryption_key[:16] + '...', 'estado': 'ACTIVADO'}, 'SUCCESS')

        return jsonify({"success": True, "encryption_key": encryption_key}), 200

    except Exception as e:
        print(f"[API_CENTRAL] Error en autenticacion: {e}")
        log_audit('AUTH', request.remote_addr, id_cp, 'AUTH_ERROR',
                 {'error': str(e)}, 'FAILED')
        return jsonify({"success": False, "error": f"Error interno: {e}"}), 500

# ======================================================================
# CONTROL DE CPs (para Web Dashboard)
# ======================================================================

@app.route('/cp/<id_cp>/parar', methods=['POST'])
def parar_cp(id_cp):
    """Para un CP especifico."""
    try:
        rows = db_fetchall("SELECT estado FROM CP WHERE idCP = ?", (id_cp,))
        if not rows:
            return jsonify({"success": False, "error": "CP no encontrado"}), 404

        estado_actual = rows[0]['estado']
        if estado_actual in ('PARADO', 'AVERIADO', 'DESCONECTADO', 'DESACTIVADO'):
            return jsonify({"success": False, "error": f"CP ya esta en estado {estado_actual}"}), 400

        db_execute("UPDATE CP SET estado = 'PARADO' WHERE idCP = ?", (id_cp,))

        # Enviar orden via Kafka
        if kafka_producer:
            try:
                kafka_producer.send("CP_CONTROL", {"accion": "PARAR", "idCP": id_cp})
                kafka_producer.flush()
            except Exception as e:
                print(f"[API_CENTRAL] Error enviando PARAR a Kafka: {e}")

        log_audit('CONTROL_ORDER', request.remote_addr, id_cp, 'PARAR', {'source': 'WebDashboard'}, 'SUCCESS')
        trigger_pantalla_update()

        return jsonify({"success": True, "message": f"CP {id_cp} parado"}), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/cp/<id_cp>/reanudar', methods=['POST'])
def reanudar_cp(id_cp):
    """Reanuda un CP especifico."""
    try:
        rows = db_fetchall("SELECT estado, paused_by_weather FROM CP WHERE idCP = ?", (id_cp,))
        if not rows:
            return jsonify({"success": False, "error": "CP no encontrado"}), 404

        estado_actual = rows[0]['estado']
        paused_by_weather = rows[0]['paused_by_weather']

        if paused_by_weather:
            return jsonify({"success": False, "error": "CP tiene alerta de clima activa. No puede reanudarse hasta que la temperatura suba."}), 400

        if estado_actual not in ('PARADO',):
            return jsonify({"success": False, "error": f"CP no puede reanudarse desde estado {estado_actual}"}), 400

        db_execute("UPDATE CP SET estado = 'ACTIVADO' WHERE idCP = ?", (id_cp,))

        # Enviar orden via Kafka
        if kafka_producer:
            try:
                kafka_producer.send("CP_CONTROL", {"accion": "REANUDAR", "idCP": id_cp})
                kafka_producer.flush()
            except Exception as e:
                print(f"[API_CENTRAL] Error enviando REANUDAR a Kafka: {e}")

        log_audit('CONTROL_ORDER', request.remote_addr, id_cp, 'REANUDAR', {'source': 'WebDashboard'}, 'SUCCESS')
        trigger_pantalla_update()

        return jsonify({"success": True, "message": f"CP {id_cp} reanudado"}), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/cp/parar_todos', methods=['POST'])
def parar_todos_cps():
    """Para todos los CPs (igual que boton GUI de Central)."""
    try:
        # Actualizar TODOS los CPs a PARADO (igual que GUI)
        db_execute("UPDATE CP SET estado = 'PARADO'")

        # Enviar UN solo mensaje Kafka con idCP="todos" (igual que GUI)
        if kafka_producer:
            try:
                kafka_producer.send("CP_CONTROL", {"accion": "PARAR", "idCP": "todos"})
                kafka_producer.flush()
            except Exception as e:
                print(f"[API_CENTRAL] Error enviando PARAR TODOS a Kafka: {e}")

        log_audit('CONTROL_ORDER', request.remote_addr, 'ALL', 'PARAR_TODOS',
                 {'source': 'WebDashboard'}, 'SUCCESS')
        trigger_pantalla_update()

        return jsonify({"success": True, "message": "Orden PARAR enviada a TODOS los CPs"}), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/cp/reanudar_todos', methods=['POST'])
def reanudar_todos_cps():
    """Reanuda todos los CPs que estan PARADOS y sin alerta de clima."""
    try:
        # Solo reanudar CPs en estado PARADO y sin alerta de clima activa
        db_execute("UPDATE CP SET estado = 'ACTIVADO' WHERE estado = 'PARADO' AND paused_by_weather = 0")

        # Enviar mensaje Kafka
        if kafka_producer:
            try:
                kafka_producer.send("CP_CONTROL", {"accion": "REANUDAR", "idCP": "todos"})
                kafka_producer.flush()
            except Exception as e:
                print(f"[API_CENTRAL] Error enviando REANUDAR TODOS a Kafka: {e}")

        log_audit('CONTROL_ORDER', request.remote_addr, 'ALL', 'REANUDAR_TODOS',
                 {'source': 'WebDashboard'}, 'SUCCESS')
        trigger_pantalla_update()

        return jsonify({"success": True, "message": "Orden REANUDAR enviada a TODOS los CPs"}), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/restaurar_claves', methods=['POST'])
def restaurar_claves():
    """Revoca todas las claves de cifrado y desactiva los CPs."""
    try:
        db_execute("UPDATE CP SET encryption_key = NULL, authenticated = 0, estado = 'DESACTIVADO'")

        log_audit('SECURITY', request.remote_addr, 'CENTRAL', 'REVOKE_KEYS',
                 {'action': 'Revoke all encryption keys via WebDashboard'}, 'SUCCESS')
        trigger_pantalla_update()

        return jsonify({
            "success": True,
            "message": "Todas las claves revocadas. Los CPs deben re-autenticarse."
        }), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ======================================================================
# MAIN (standalone mode)
# ======================================================================

if __name__ == '__main__':
    print("[API_CENTRAL] Iniciando API REST en puerto 5002")
    print("[API_CENTRAL] Endpoints disponibles:")
    print("  GET  /cps           - Lista CPs")
    print("  GET  /drivers       - Lista conductores")
    print("  GET  /transactions  - Lista transacciones")
    print("  POST /weather_alert - Recibe alertas de EV_W")
    print("  POST /authenticate  - Autentica CP con token")
    print("  GET  /audit         - Lista auditoria")
    print("  GET  /weather       - Estado del clima")
    print("  GET  /health        - Health check")
    app.run(host='0.0.0.0', port=5002, debug=False)

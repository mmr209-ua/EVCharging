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
CORS(app)  # Permitir CORS para el Front Web

BBDD = "Base_Datos.sqlite"

# Variable global para el producer de Kafka (se inyecta desde EV_Central)
kafka_producer = None

def set_kafka_producer(producer):
    """Inyecta el producer de Kafka desde EV_Central."""
    global kafka_producer
    kafka_producer = producer

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

        # Obtener CPs en esa ubicacion
        cps = db_fetchall("SELECT idCP, estado FROM CP WHERE ubicacion = ?", (ubicacion,))

        affected_cps = []

        if alert:
            # Temperatura bajo cero - PARAR CPs
            for cp in cps:
                id_cp = cp["idCP"]
                estado = cp["estado"]

                # Marcar como pausado por clima
                db_execute("UPDATE CP SET paused_by_weather = 1 WHERE idCP = ?", (id_cp,))

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

            print(f"[API_CENTRAL] Alerta ACTIVADA para {ubicacion}: {temperatura}C, CPs afectados: {len(affected_cps)}")

        else:
            # Temperatura normal - REANUDAR CPs
            for cp in cps:
                id_cp = cp["idCP"]
                estado = cp["estado"]

                # Quitar marca de pausa por clima
                db_execute("UPDATE CP SET paused_by_weather = 0 WHERE idCP = ?", (id_cp,))

                if estado == "PARADO":
                    # Reanudar
                    db_execute("UPDATE CP SET estado = 'ACTIVADO' WHERE idCP = ?", (id_cp,))
                    affected_cps.append({"idCP": id_cp, "action": "RESUMED"})

                    # Enviar orden REANUDAR via Kafka
                    if kafka_producer:
                        try:
                            kafka_producer.send("CP_CONTROL", {"accion": "REANUDAR", "idCP": id_cp})
                            kafka_producer.flush()
                        except Exception as e:
                            print(f"[API_CENTRAL] Error enviando REANUDAR a Kafka: {e}")

            print(f"[API_CENTRAL] Alerta CANCELADA para {ubicacion}: {temperatura}C, CPs reanudados: {len(affected_cps)}")

        # Registrar en auditoria
        db_execute("""
            INSERT INTO AUDIT_LOG (event_type, source_ip, source_id, action, parameters, result)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            'WEATHER_ALERT',
            request.remote_addr,
            'EV_W',
            'ALERT_RECEIVED' if alert else 'ALERT_CANCELLED',
            json.dumps({"ubicacion": ubicacion, "temperatura": temperatura}),
            'SUCCESS'
        ))

        return jsonify({
            "success": True,
            "message": f"Alerta {'activada' if alert else 'cancelada'} para {ubicacion}",
            "affected_cps": affected_cps
        }), 200

    except Exception as e:
        print(f"[API_CENTRAL] Error procesando alerta: {e}")
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

@app.route('/authenticate', methods=['POST'])
def authenticate_cp():
    """
    Autentica un CP usando el auth_token del Registry.
    Si es valido, genera y devuelve una encryption_key unica.

    Request JSON:
    {
        "idCP": "1",
        "authToken": "uuid-token..."
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
    auth_token = data.get("authToken", "")

    if not id_cp or not auth_token:
        return jsonify({"success": False, "error": "idCP y authToken son requeridos"}), 400

    print(f"[API_CENTRAL] Intento de autenticacion de CP {id_cp}")

    try:
        # Verificar que el CP existe y tiene el token correcto
        rows = db_fetchall("SELECT auth_token, authenticated FROM CP WHERE idCP = ?", (id_cp,))

        if not rows:
            print(f"[API_CENTRAL] CP {id_cp} no registrado en Registry")
            db_execute("""
                INSERT INTO AUDIT_LOG (event_type, source_ip, source_id, action, parameters, result)
                VALUES (?, ?, ?, ?, ?, ?)
            """, ('AUTH', request.remote_addr, id_cp, 'AUTHENTICATE', json.dumps({'reason': 'CP not registered'}), 'FAILED'))
            return jsonify({"success": False, "error": "CP no registrado. Registrese primero en Registry."}), 404

        stored_token = rows[0]["auth_token"]

        if stored_token != auth_token:
            print(f"[API_CENTRAL] Token invalido para CP {id_cp}")
            db_execute("""
                INSERT INTO AUDIT_LOG (event_type, source_ip, source_id, action, parameters, result)
                VALUES (?, ?, ?, ?, ?, ?)
            """, ('AUTH', request.remote_addr, id_cp, 'AUTHENTICATE', json.dumps({'reason': 'Invalid token'}), 'FAILED'))
            return jsonify({"success": False, "error": "Token de autenticacion invalido"}), 401

        # Generar clave de cifrado unica
        encryption_key = generate_encryption_key()

        # Guardar en BD
        db_execute("""
            UPDATE CP SET encryption_key = ?, authenticated = 1, estado = 'ACTIVADO'
            WHERE idCP = ?
        """, (encryption_key, id_cp))

        print(f"[API_CENTRAL] CP {id_cp} AUTENTICADO correctamente")
        db_execute("""
            INSERT INTO AUDIT_LOG (event_type, source_ip, source_id, action, parameters, result)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('AUTH', request.remote_addr, id_cp, 'AUTHENTICATE', json.dumps({'encryption_key': encryption_key[:16] + '...'}), 'SUCCESS'))

        return jsonify({"success": True, "encryption_key": encryption_key}), 200

    except Exception as e:
        print(f"[API_CENTRAL] Error en autenticacion: {e}")
        return jsonify({"success": False, "error": f"Error interno: {e}"}), 500

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

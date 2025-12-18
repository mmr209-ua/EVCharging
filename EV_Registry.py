# EV_Registry.py - Release 2 con HTTPS
# NO accede a la BD directamente - usa API_Central
import json
import uuid
import sys
import ssl
import os
import ipaddress
import requests
from flask import Flask, request, jsonify

# ======================================================================
# CONFIGURACION
# ======================================================================
HOST = "0.0.0.0"
PORT = 5001

# URL de API_Central (se puede configurar por argumento)
API_CENTRAL_BASE = "http://localhost:5002"

# Certificados SSL (generados con openssl)
CERT_FILE = "registry_cert.pem"
KEY_FILE = "registry_key.pem"

app = Flask(__name__)

# ======================================================================
# FUNCIONES AUXILIARES PARA API_CENTRAL
# ======================================================================

def log_audit_via_api(event_type: str, source_ip: str, source_id: str, action: str, parameters: dict, result: str):
    """Registra evento de auditoria via API_Central."""
    try:
        payload = {
            "event_type": event_type,
            "source_ip": source_ip,
            "source_id": source_id,
            "action": action,
            "parameters": parameters,
            "result": result
        }
        requests.post(f"{API_CENTRAL_BASE}/audit/log", json=payload, timeout=5)
        print(f"[REGISTRY][AUDIT] {event_type} | {source_ip} | {source_id} | {action} | {result}")
    except Exception as e:
        # Solo imprimir localmente si falla
        print(f"[REGISTRY][AUDIT] (local) {event_type} | {source_ip} | {source_id} | {action} | {result}")

def register_cp_in_central(id_cp: str, precio: float, ubicacion: str, auth_token: str) -> dict:
    """Registra un CP en Central via API."""
    try:
        payload = {
            "idCP": id_cp,
            "precio": precio,
            "ubicacion": ubicacion,
            "auth_token": auth_token
        }
        response = requests.post(f"{API_CENTRAL_BASE}/registry/register", json=payload, timeout=10)
        return response.json()
    except requests.exceptions.ConnectionError:
        return {"error": True, "message": "No se puede conectar a API_Central"}
    except Exception as e:
        return {"error": True, "message": str(e)}

def unregister_cp_in_central(id_cp: str) -> dict:
    """Da de baja un CP en Central via API."""
    try:
        response = requests.delete(f"{API_CENTRAL_BASE}/registry/unregister/{id_cp}", timeout=10)
        return response.json()
    except requests.exceptions.ConnectionError:
        return {"error": True, "message": "No se puede conectar a API_Central"}
    except Exception as e:
        return {"error": True, "message": str(e)}

def get_cp_from_central(id_cp: str) -> dict:
    """Obtiene info de un CP desde Central via API."""
    try:
        response = requests.get(f"{API_CENTRAL_BASE}/registry/status/{id_cp}", timeout=10)
        return response.json()
    except requests.exceptions.ConnectionError:
        return {"error": True, "message": "No se puede conectar a API_Central"}
    except Exception as e:
        return {"error": True, "message": str(e)}

# ======================================================================
# API REST ENDPOINTS
# ======================================================================

@app.route('/register', methods=['POST'])
def register_cp():
    """
    Registra un nuevo CP o actualiza su informacion inicial.
    Genera y retorna credenciales para la autenticacion en EV_Central.

    Request JSON:
        {
            "idCP": "1",
            "precio": 0.30,
            "ubicacion": "Madrid"
        }

    Response JSON:
        {
            "error": false,
            "message": "CP registrado exitosamente",
            "credenciales": {
                "idCP": "1",
                "authToken": "uuid-token..."
            }
        }
    """
    try:
        data = request.get_json()
        if not data or 'idCP' not in data:
            return jsonify({"error": True, "message": "Datos de registro incompletos"}), 400

        id_cp = str(data.get("idCP"))
        precio = float(data.get("precio", 0.30))
        ubicacion = data.get("ubicacion", f"Zona-{id_cp}")

        # Generar Token de Autenticacion
        auth_token = str(uuid.uuid4())

        # Registrar en Central via API
        result = register_cp_in_central(id_cp, precio, ubicacion, auth_token)

        if result.get("error"):
            log_audit_via_api('REGISTER', request.remote_addr, id_cp, 'CP_REGISTER',
                     {'error': result.get('message', 'Unknown error')}, 'FAILED')
            return jsonify({"error": True, "message": result.get("message", "Error registrando en Central")}), 500

        response = {
            "error": False,
            "message": f"CP {id_cp} registrado exitosamente. Use el token para autenticarse en Central.",
            "credenciales": {
                "idCP": id_cp,
                "authToken": auth_token
            }
        }
        print(f"[REGISTRY] Registro de CP {id_cp} completado. Token: {auth_token[:8]}...")
        log_audit_via_api('REGISTER', request.remote_addr, id_cp, 'CP_REGISTER',
                 {'precio': precio, 'ubicacion': ubicacion, 'token_prefix': auth_token[:8]}, 'SUCCESS')
        return jsonify(response), 201

    except Exception as e:
        print(f"[REGISTRY] Error en /register: {e}")
        log_audit_via_api('REGISTER', request.remote_addr, data.get('idCP', 'unknown') if data else 'unknown',
                 'CP_REGISTER', {'error': str(e)}, 'FAILED')
        return jsonify({"error": True, "message": f"Error interno: {e}"}), 500

@app.route('/unregister/<id_cp>', methods=['DELETE'])
def unregister_cp(id_cp):
    """
    Da de baja un CP del sistema.
    El ID se pasa en la URL: DELETE /unregister/1
    """
    try:
        id_cp = str(id_cp)

        # Verificar que el CP existe via API
        check_result = get_cp_from_central(id_cp)
        if check_result.get("error"):
            if "no encontrado" in check_result.get("message", "").lower():
                log_audit_via_api('UNREGISTER', request.remote_addr, id_cp, 'CP_UNREGISTER',
                         {'error': 'CP not found'}, 'FAILED')
                return jsonify({"error": True, "message": "CP no encontrado"}), 404
            # Otro error de conexion
            return jsonify({"error": True, "message": check_result.get("message")}), 500

        # Dar de baja via API
        result = unregister_cp_in_central(id_cp)

        if result.get("error"):
            log_audit_via_api('UNREGISTER', request.remote_addr, id_cp, 'CP_UNREGISTER',
                     {'error': result.get('message')}, 'FAILED')
            return jsonify({"error": True, "message": result.get("message", "Error dando de baja")}), 500

        print(f"[REGISTRY] CP {id_cp} dado de baja")
        log_audit_via_api('UNREGISTER', request.remote_addr, id_cp, 'CP_UNREGISTER',
                 {'action': 'CP eliminado via API'}, 'SUCCESS')
        return jsonify({"error": False, "message": f"CP {id_cp} dado de baja correctamente"}), 200

    except Exception as e:
        print(f"[REGISTRY] Error en /unregister: {e}")
        log_audit_via_api('UNREGISTER', request.remote_addr, id_cp, 'CP_UNREGISTER',
                 {'error': str(e)}, 'FAILED')
        return jsonify({"error": True, "message": f"Error interno: {e}"}), 500

@app.route('/status/<id_cp>', methods=['GET'])
def get_cp_status(id_cp):
    """
    Obtiene el estado de registro de un CP.
    """
    try:
        result = get_cp_from_central(id_cp)

        if result.get("error"):
            if "no encontrado" in result.get("message", "").lower():
                return jsonify({"error": True, "message": "CP no encontrado"}), 404
            return jsonify({"error": True, "message": result.get("message")}), 500

        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": True, "message": f"Error: {e}"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de health check."""
    # Verificar conexion con Central
    try:
        response = requests.get(f"{API_CENTRAL_BASE}/health", timeout=5)
        central_ok = response.status_code == 200
    except:
        central_ok = False

    return jsonify({
        "status": "ok",
        "service": "EV_Registry",
        "central_connected": central_ok
    }), 200

# ======================================================================
# MAIN
# ======================================================================

def generate_self_signed_cert():
    """Genera certificados autofirmados si no existen usando Python."""
    if not os.path.exists(CERT_FILE) or not os.path.exists(KEY_FILE):
        print("[REGISTRY] Generando certificados SSL autofirmados...")
        try:
            from cryptography import x509
            from cryptography.x509.oid import NameOID
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.hazmat.primitives import serialization
            import datetime

            # Generar clave privada
            key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )

            # Crear certificado
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, "ES"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Madrid"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Madrid"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "EVCharging"),
                x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
            ])

            cert = x509.CertificateBuilder().subject_name(
                subject
            ).issuer_name(
                issuer
            ).public_key(
                key.public_key()
            ).serial_number(
                x509.random_serial_number()
            ).not_valid_before(
                datetime.datetime.utcnow()
            ).not_valid_after(
                datetime.datetime.utcnow() + datetime.timedelta(days=365)
            ).add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                ]),
                critical=False,
            ).sign(key, hashes.SHA256(), default_backend())

            # Guardar clave privada
            with open(KEY_FILE, "wb") as f:
                f.write(key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption()
                ))

            # Guardar certificado
            with open(CERT_FILE, "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))

            print("[REGISTRY] Certificados generados correctamente")
        except ImportError:
            print("[REGISTRY] ERROR: Instale cryptography: pip install cryptography")
            return False
        except Exception as e:
            print(f"[REGISTRY] ERROR generando certificados: {e}")
            return False
    return True

def wait_for_central():
    """Espera a que API_Central este disponible."""
    import time
    print(f"[REGISTRY] Esperando conexion con API_Central ({API_CENTRAL_BASE})...")
    while True:
        try:
            response = requests.get(f"{API_CENTRAL_BASE}/health", timeout=5)
            if response.status_code == 200:
                print("[REGISTRY] Conectado a API_Central")
                return True
        except:
            pass
        print("[REGISTRY] API_Central no disponible, reintentando en 3 segundos...")
        time.sleep(3)

if __name__ == '__main__':
    # Argumento opcional para URL de Central
    if len(sys.argv) > 1:
        API_CENTRAL_BASE = sys.argv[1].rstrip('/')

    print("[REGISTRY] EV_Registry - Release 2")
    print(f"[REGISTRY] API Central: {API_CENTRAL_BASE}")
    print("[REGISTRY] NO usa BD local - opera via API_Central")

    # Verificar/generar certificados SSL
    generate_self_signed_cert()

    # Esperar a que Central este disponible
    wait_for_central()

    if os.path.exists(CERT_FILE) and os.path.exists(KEY_FILE):
        # Ejecutar con HTTPS
        print(f"[REGISTRY] Iniciando API REST (HTTPS) en {HOST}:{PORT}")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(CERT_FILE, KEY_FILE)
        app.run(host=HOST, port=PORT, debug=False, ssl_context=context)
    else:
        # Fallback a HTTP si no hay certificados
        print(f"[REGISTRY] ADVERTENCIA: Ejecutando en modo HTTP (inseguro)")
        print(f"[REGISTRY] Iniciando API REST en {HOST}:{PORT}")
        app.run(host=HOST, port=PORT, debug=False)

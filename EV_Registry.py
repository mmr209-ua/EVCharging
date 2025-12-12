# EV_Registry.py - Release 2 con HTTPS
import sqlite3
import json
import uuid
import sys
import ssl
import os
import ipaddress
from flask import Flask, request, jsonify

# ======================================================================
# CONFIGURACION
# ======================================================================
BBDD = "Base_Datos.sqlite"
HOST = "0.0.0.0"
PORT = 5001

# Certificados SSL (generados con openssl)
CERT_FILE = "registry_cert.pem"
KEY_FILE = "registry_key.pem"

app = Flask(__name__)

# ======================================================================
# UTILIDADES BD
# ======================================================================

def db_execute(query, params=()):
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            return True
    except sqlite3.Error as e:
        print(f"[REGISTRY] Error BD: {e}")
        return False

def db_fetchone(query, params=()):
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            return cur.fetchone()
    except sqlite3.Error as e:
        print(f"[REGISTRY] Error BD: {e}")
        return None

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

        # Insertar/Actualizar en la tabla CP
        success = db_execute("""
            INSERT INTO CP (idCP, estado, precio, ubicacion, auth_token, authenticated)
            VALUES (?, ?, ?, ?, ?, 0)
            ON CONFLICT(idCP) DO UPDATE SET
                precio=excluded.precio,
                ubicacion=excluded.ubicacion,
                auth_token=excluded.auth_token,
                authenticated=0,
                estado='DESCONECTADO'
        """, (id_cp, 'DESCONECTADO', precio, ubicacion, auth_token))

        if success:
            response = {
                "error": False,
                "message": f"CP {id_cp} registrado exitosamente. Use el token para autenticarse en Central.",
                "credenciales": {
                    "idCP": id_cp,
                    "authToken": auth_token
                }
            }
            print(f"[REGISTRY] Registro de CP {id_cp} completado. Token: {auth_token[:8]}...")
            return jsonify(response), 201
        else:
            return jsonify({"error": True, "message": "Error interno al guardar en BD"}), 500

    except Exception as e:
        print(f"[REGISTRY] Error en /register: {e}")
        return jsonify({"error": True, "message": f"Error interno: {e}"}), 500

@app.route('/unregister/<id_cp>', methods=['DELETE'])
def unregister_cp(id_cp):
    """
    Da de baja un CP del sistema.
    El ID se pasa en la URL: DELETE /unregister/1
    """
    try:
        id_cp = str(id_cp)

        # Verificar que el CP existe
        row = db_fetchone("SELECT auth_token FROM CP WHERE idCP = ?", (id_cp,))
        if not row:
            return jsonify({"error": True, "message": "CP no encontrado"}), 404

        # Eliminar CP de la base de datos
        success = db_execute("DELETE FROM CP WHERE idCP = ?", (id_cp,))

        if success:
            print(f"[REGISTRY] CP {id_cp} dado de baja")
            return jsonify({"error": False, "message": f"CP {id_cp} dado de baja correctamente"}), 200
        else:
            return jsonify({"error": True, "message": "Error al dar de baja CP"}), 500

    except Exception as e:
        print(f"[REGISTRY] Error en /unregister: {e}")
        return jsonify({"error": True, "message": f"Error interno: {e}"}), 500

@app.route('/status/<id_cp>', methods=['GET'])
def get_cp_status(id_cp):
    """
    Obtiene el estado de registro de un CP.
    """
    try:
        row = db_fetchone("SELECT idCP, estado, ubicacion, authenticated FROM CP WHERE idCP = ?", (id_cp,))
        if not row:
            return jsonify({"error": True, "message": "CP no encontrado"}), 404

        return jsonify({
            "error": False,
            "cp": {
                "idCP": row[0],
                "estado": row[1],
                "ubicacion": row[2],
                "authenticated": bool(row[3])
            }
        }), 200
    except Exception as e:
        return jsonify({"error": True, "message": f"Error: {e}"}), 500

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

if __name__ == '__main__':
    # Verificar/generar certificados SSL
    generate_self_signed_cert()

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

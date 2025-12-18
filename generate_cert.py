# EVCharging - Release 2 Certificado
# Instalar con: openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout registry_key.pem -out registry_cert.pem
import subprocess

openssl_cmd = [
    "openssl", "req",
    "-x509",
    "-nodes",
    "-days", "365",
    "-newkey", "rsa:2048",
    "-keyout", "registry_key.pem",
    "-out", "registry_cert.pem",
    "-subj", "/C=ES/ST=Comunidad Valenciana/L=Alicante/O=UA/OU=SD/CN=localhost"
]

subprocess.run(openssl_cmd, check=True)

print("Certificado generado: registry_cert.pem")
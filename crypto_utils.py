# crypto_utils.py - Modulo de cifrado simetrico para Release 2
# Utiliza Fernet (AES-128-CBC) de la libreria cryptography

from cryptography.fernet import Fernet
import json
import base64

def generate_encryption_key():
    """
    Genera una nueva clave Fernet (simetrica).
    Retorna la clave como string base64.
    """
    return Fernet.generate_key().decode('utf-8')

def encrypt_message(key: str, data: dict) -> str:
    """
    Cifra un mensaje JSON con Fernet.

    Args:
        key: Clave Fernet en formato string base64
        data: Diccionario a cifrar

    Returns:
        String base64 con el mensaje cifrado
    """
    f = Fernet(key.encode('utf-8'))
    json_str = json.dumps(data)
    encrypted = f.encrypt(json_str.encode('utf-8'))
    return base64.b64encode(encrypted).decode('utf-8')

def decrypt_message(key: str, encrypted_str: str) -> dict:
    """
    Descifra un mensaje JSON con Fernet.

    Args:
        key: Clave Fernet en formato string base64
        encrypted_str: String base64 con el mensaje cifrado

    Returns:
        Diccionario descifrado
    """
    f = Fernet(key.encode('utf-8'))
    encrypted_bytes = base64.b64decode(encrypted_str.encode('utf-8'))
    decrypted = f.decrypt(encrypted_bytes)
    return json.loads(decrypted.decode('utf-8'))

def encrypt_message_simple(key: str, data: dict) -> bytes:
    """
    Cifra un mensaje JSON con Fernet y retorna bytes directamente.
    Util para Kafka que espera bytes.
    """
    f = Fernet(key.encode('utf-8'))
    json_str = json.dumps(data)
    return f.encrypt(json_str.encode('utf-8'))

def decrypt_message_simple(key: str, encrypted_bytes: bytes) -> dict:
    """
    Descifra bytes directamente con Fernet.
    Util para Kafka.
    """
    f = Fernet(key.encode('utf-8'))
    decrypted = f.decrypt(encrypted_bytes)
    return json.loads(decrypted.decode('utf-8'))

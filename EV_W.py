# EV_W.py - Weather Control Office (Release 2)
# Consulta OpenWeather API y notifica alertas a Central

import requests
import time
import sqlite3
import sys
import os
from typing import Dict, List

BBDD = "Base_Datos.sqlite"
API_CENTRAL_URL = "http://localhost:5002/weather_alert"
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
POLL_INTERVAL = 4  # segundos
OPENWEATHER_API_KEY_FILE = "openweather_api_key.txt"

def load_api_key():
    """Lee la API key de OpenWeather desde archivo."""
    try:
        key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OPENWEATHER_API_KEY_FILE)
        with open(key_path, 'r') as f:
            api_key = f.read().strip()
            return api_key if api_key else None
    except FileNotFoundError:
        print(f"[EV_W] Archivo {OPENWEATHER_API_KEY_FILE} no encontrado")
        return None
    except Exception as e:
        print(f"[EV_W] Error leyendo API key: {e}")
        return None

# API Key de OpenWeather (se lee desde archivo)
OPENWEATHER_API_KEY = load_api_key()

# Cache de estado de alertas por ubicacion
alert_state: Dict[str, bool] = {}

# Mapeo de ubicaciones internas a ciudades reales (configurable)
LOCATION_MAPPING: Dict[str, str] = {}

# Flag para indicar si el usuario esta escribiendo en el menu
menu_activo = False

def get_ubicaciones_from_db() -> List[str]:
    """Obtiene ubicaciones unicas de CPs desde BD."""
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT ubicacion FROM CP")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[EV_W] Error leyendo ubicaciones de BD: {e}")
        return []

def get_temperature_openweather(city: str) -> float:
    """
    Consulta OpenWeather API para obtener temperatura.
    Retorna temperatura en Celsius o None si hay error.
    """
    if not OPENWEATHER_API_KEY:
        return None

    try:
        params = {
            'q': city,
            'appid': OPENWEATHER_API_KEY,
            'units': 'metric'  # Celsius
        }
        response = requests.get(OPENWEATHER_URL, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        temp = data['main']['temp']
        return temp
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f"[EV_W] API Key invalida o no configurada")
        elif e.response.status_code == 404:
            print(f"[EV_W] Ciudad no encontrada: {city}")
        else:
            print(f"[EV_W] Error HTTP consultando clima para {city}: {e}")
        return None
    except Exception as e:
        print(f"[EV_W] Error consultando clima para {city}: {e}")
        return None

def get_temperature_simulated(ubicacion: str) -> float:
    """
    Simula temperatura para demo/testing.
    Permite probar sin API key de OpenWeather.
    """
    import random

    # Simular diferentes temperaturas segun ubicacion
    base_temps = {
        "Madrid": 10,
        "Barcelona": 12,
        "Valencia": 15,
        "Sevilla": 18,
        "Bilbao": 8,
        "Zaragoza": 9,
        "Malaga": 16,
        "Murcia": 14,
    }

    base = base_temps.get(ubicacion, 10)
    # Variacion aleatoria de +/- 15 grados
    variation = random.uniform(-15, 10)
    return round(base + variation, 1)

def send_alert(ubicacion: str, alert: bool, temperatura: float):
    """Envia alerta a API_Central."""
    try:
        payload = {
            "ubicacion": ubicacion,
            "alert": alert,
            "temperatura": temperatura
        }
        response = requests.post(API_CENTRAL_URL, json=payload, timeout=5)
        response.raise_for_status()
        result = response.json()
        action = "ACTIVADA" if alert else "CANCELADA"
        print(f"[EV_W] Alerta {action} enviada: {ubicacion} = {temperatura}C")
        if result.get("affected_cps"):
            for cp in result["affected_cps"]:
                print(f"       CP {cp['idCP']}: {cp['action']}")
    except requests.exceptions.ConnectionError:
        print(f"[EV_W] Error: No se puede conectar a API_Central ({API_CENTRAL_URL})")
        print(f"[EV_W] Asegurese de que EV_Central esta ejecutandose")
    except Exception as e:
        print(f"[EV_W] Error enviando alerta: {e}")

def get_cps_from_db():
    """Obtiene lista de CPs con su ubicacion desde BD."""
    try:
        with sqlite3.connect(BBDD) as conn:
            cur = conn.cursor()
            cur.execute("SELECT idCP, ubicacion FROM CP ORDER BY idCP")
            return cur.fetchall()
    except Exception as e:
        print(f"[EV_W] Error leyendo CPs de BD: {e}")
        return []

def cambiar_ubicacion_cp(id_cp: str, nueva_ubicacion: str, ciudad_openweather: str = None):
    """Cambia la ubicacion de un CP via API_Central."""
    try:
        # Llamar a API_Central para cambiar ubicacion
        api_url = API_CENTRAL_URL.replace('/weather_alert', '')
        response = requests.put(
            f"{api_url}/cp/{id_cp}/ubicacion",
            json={"ubicacion": nueva_ubicacion},
            timeout=5
        )

        result = response.json()

        if result.get("success"):
            ubicacion_anterior = result.get("ubicacion_anterior", "desconocida")
            print(f"[EV_W] CP {id_cp}: ubicacion cambiada de '{ubicacion_anterior}' a '{nueva_ubicacion}'")

            # Añadir nueva ubicacion al monitoreo
            ciudad = ciudad_openweather if ciudad_openweather else nueva_ubicacion
            alert_state[nueva_ubicacion] = False
            LOCATION_MAPPING[nueva_ubicacion] = ciudad
            print(f"[EV_W] Ubicacion '{nueva_ubicacion}' -> Ciudad OpenWeather: '{ciudad}'")
            print(f"[EV_W] Central actualizada con la nueva ubicacion")

            # Comprobar temperatura inmediatamente
            print(f"[EV_W] Consultando temperatura de {ciudad}...")
            if OPENWEATHER_API_KEY:
                temp = get_temperature_openweather(ciudad)
            else:
                temp = get_temperature_simulated(nueva_ubicacion)

            if temp is not None:
                print(f"[EV_W] Temperatura actual en {ciudad}: {temp}C")
                if temp < 0:
                    print(f"[EV_W] ALERTA: Temperatura bajo cero detectada!")
                    send_alert(nueva_ubicacion, True, temp)
                    alert_state[nueva_ubicacion] = True
                else:
                    print(f"[EV_W] Temperatura normal, cancelando alerta si existia")
                    send_alert(nueva_ubicacion, False, temp)
                    alert_state[nueva_ubicacion] = False
            else:
                print(f"[EV_W] No se pudo obtener temperatura de {ciudad}")

            return True
        else:
            print(f"[EV_W] Error: {result.get('message', 'Error desconocido')}")
            return False

    except requests.exceptions.ConnectionError:
        print(f"[EV_W] Error: No se puede conectar a API_Central")
        print(f"[EV_W] Asegurese de que EV_Central esta ejecutandose")
        return False
    except Exception as e:
        print(f"[EV_W] Error cambiando ubicacion: {e}")
        return False

def list_locations():
    """Lista ubicaciones monitoreadas."""
    print("\n[EV_W] Ubicaciones monitoreadas:")
    for ub in alert_state.keys():
        city = LOCATION_MAPPING.get(ub, ub)
        status = "ALERTA ACTIVA" if alert_state.get(ub) else "Normal"
        print(f"   {ub} -> {city} [{status}]")

def menu_interactivo():
    """Menu para gestionar ubicaciones en runtime."""
    global menu_activo
    while True:
        print("\n--- MENU EV_W ---")
        print("1 - Cambiar ubicacion de CP")
        print("2 - Listar ubicaciones")
        print("3 - Forzar alerta (test)")
        print("4 - Cancelar alerta (test)")
        print("Q - Salir del menu")

        try:
            menu_activo = True
            opcion = input("Opcion: ").strip().upper()
            menu_activo = False
        except (EOFError, KeyboardInterrupt):
            menu_activo = False
            break

        if opcion == "1":
            # Mostrar CPs disponibles
            cps = get_cps_from_db()
            if not cps:
                print("[EV_W] No hay CPs registrados")
                continue

            print("\n[EV_W] CPs disponibles:")
            for cp_id, ubicacion in cps:
                city = LOCATION_MAPPING.get(ubicacion, ubicacion)
                print(f"   CP {cp_id} -> {ubicacion} (OpenWeather: {city})")

            menu_activo = True
            id_cp = input("\nNumero de CP a cambiar: ").strip()
            menu_activo = False
            if not id_cp:
                continue

            menu_activo = True
            nueva_ub = input("Nueva ubicacion: ").strip()
            menu_activo = False
            if not nueva_ub:
                continue

            menu_activo = True
            ciudad_ow = input(f"Ciudad para OpenWeather (Enter para usar '{nueva_ub}'): ").strip()
            menu_activo = False
            cambiar_ubicacion_cp(id_cp, nueva_ub, ciudad_ow if ciudad_ow else None)

        elif opcion == "2":
            list_locations()

        elif opcion == "3":
            menu_activo = True
            ub = input("Ubicacion para forzar alerta: ").strip()
            menu_activo = False
            if ub:
                send_alert(ub, True, -5.0)
                alert_state[ub] = True

        elif opcion == "4":
            menu_activo = True
            ub = input("Ubicacion para cancelar alerta: ").strip()
            menu_activo = False
            if ub:
                send_alert(ub, False, 5.0)
                alert_state[ub] = False

        elif opcion == "Q":
            break

def main():
    global API_CENTRAL_URL

    # Argumento opcional para URL de Central
    if len(sys.argv) > 1:
        API_CENTRAL_URL = sys.argv[1]

    print("[EV_W] Weather Control Office - Release 2")
    print(f"[EV_W] API Central: {API_CENTRAL_URL}")
    print(f"[EV_W] API Key leida de: {OPENWEATHER_API_KEY_FILE}")
    print(f"[EV_W] OpenWeather API Key: {'Configurada' if OPENWEATHER_API_KEY else 'NO CONFIGURADA (modo simulacion)'}")
    print(f"[EV_W] Polling cada {POLL_INTERVAL} segundos")
    print("[EV_W] Umbral de alerta: < 0C")

    if not OPENWEATHER_API_KEY:
        print("[EV_W] MODO SIMULACION: Las temperaturas seran aleatorias")
        print(f"[EV_W] Para usar OpenWeather, cree el archivo {OPENWEATHER_API_KEY_FILE} con su API key")

    # Cargar ubicaciones iniciales desde BD
    ubicaciones = get_ubicaciones_from_db()
    for ub in ubicaciones:
        alert_state[ub] = False
        # Mapear ubicaciones internas a ciudades reales
        if ub.startswith("Zona-"):
            # Usar Madrid como default para zonas genericas
            LOCATION_MAPPING[ub] = "Madrid"

    print(f"[EV_W] Ubicaciones cargadas de BD: {list(alert_state.keys())}")

    # Iniciar menu en hilo separado
    import threading
    threading.Thread(target=menu_interactivo, daemon=True).start()

    # Bucle principal de polling
    try:
        while True:
            # Si el usuario esta escribiendo en el menu, esperar sin imprimir
            if menu_activo:
                time.sleep(0.5)
                continue

            # Recargar ubicaciones de BD periodicamente
            ubicaciones_bd = set(get_ubicaciones_from_db())

            # Eliminar ubicaciones que ya no existen en la BD
            ubicaciones_a_eliminar = [ub for ub in alert_state.keys() if ub not in ubicaciones_bd]
            for ub in ubicaciones_a_eliminar:
                del alert_state[ub]
                if ub in LOCATION_MAPPING:
                    del LOCATION_MAPPING[ub]
                if not menu_activo:
                    print(f"[EV_W] Ubicacion eliminada: {ub}")

            # Añadir nuevas ubicaciones
            for ub in ubicaciones_bd:
                if ub not in alert_state:
                    alert_state[ub] = False
                    if not menu_activo:
                        print(f"[EV_W] Nueva ubicacion detectada: {ub}")

            # Consultar temperatura de cada ubicacion
            for ubicacion in list(alert_state.keys()):
                # Salir si el usuario empieza a escribir
                if menu_activo:
                    break

                # Obtener ciudad real para OpenWeather
                city = LOCATION_MAPPING.get(ubicacion, ubicacion)

                # Obtener temperatura
                if OPENWEATHER_API_KEY:
                    temp = get_temperature_openweather(city)
                else:
                    temp = get_temperature_simulated(ubicacion)

                if temp is None:
                    continue

                if not menu_activo:
                    print(f"[EV_W] {ubicacion} ({city}): {temp}C")

                # Logica de alertas
                current_alert = alert_state.get(ubicacion, False)

                if temp < 0 and not current_alert:
                    # Nueva alerta: temperatura bajo cero
                    send_alert(ubicacion, True, temp)
                    alert_state[ubicacion] = True

                elif temp >= 0 and current_alert:
                    # Cancelar alerta: temperatura normal
                    send_alert(ubicacion, False, temp)
                    alert_state[ubicacion] = False

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n[EV_W] Detenido por el usuario")

if __name__ == '__main__':
    main()
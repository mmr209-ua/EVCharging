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

# API Key de OpenWeather (obtener en https://openweathermap.org/api)
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', '')

# Cache de estado de alertas por ubicacion
alert_state: Dict[str, bool] = {}

# Mapeo de ubicaciones internas a ciudades reales (configurable)
LOCATION_MAPPING: Dict[str, str] = {}

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

def add_location(ubicacion: str, city: str = None):
    """Anade una ubicacion al monitoreo."""
    if city:
        LOCATION_MAPPING[ubicacion] = city
    alert_state[ubicacion] = False
    print(f"[EV_W] Ubicacion anadida: {ubicacion} -> {city or ubicacion}")

def remove_location(ubicacion: str):
    """Elimina una ubicacion del monitoreo."""
    if ubicacion in alert_state:
        del alert_state[ubicacion]
    if ubicacion in LOCATION_MAPPING:
        del LOCATION_MAPPING[ubicacion]
    print(f"[EV_W] Ubicacion eliminada: {ubicacion}")

def list_locations():
    """Lista ubicaciones monitoreadas."""
    print("\n[EV_W] Ubicaciones monitoreadas:")
    for ub in alert_state.keys():
        city = LOCATION_MAPPING.get(ub, ub)
        status = "ALERTA ACTIVA" if alert_state.get(ub) else "Normal"
        print(f"   {ub} -> {city} [{status}]")

def menu_interactivo():
    """Menu para gestionar ubicaciones en runtime."""
    while True:
        print("\n--- MENU EV_W ---")
        print("1 - Anadir ubicacion")
        print("2 - Eliminar ubicacion")
        print("3 - Listar ubicaciones")
        print("4 - Forzar alerta (test)")
        print("5 - Cancelar alerta (test)")
        print("Q - Salir del menu")

        try:
            opcion = input("Opcion: ").strip().upper()
        except (EOFError, KeyboardInterrupt):
            break

        if opcion == "1":
            ub = input("Nombre ubicacion (ej: Zona-1): ").strip()
            city = input("Ciudad real para OpenWeather (ej: Madrid): ").strip()
            if ub:
                add_location(ub, city if city else None)

        elif opcion == "2":
            ub = input("Ubicacion a eliminar: ").strip()
            if ub:
                remove_location(ub)

        elif opcion == "3":
            list_locations()

        elif opcion == "4":
            ub = input("Ubicacion para forzar alerta: ").strip()
            if ub:
                send_alert(ub, True, -5.0)
                alert_state[ub] = True

        elif opcion == "5":
            ub = input("Ubicacion para cancelar alerta: ").strip()
            if ub:
                send_alert(ub, False, 5.0)
                alert_state[ub] = False

        elif opcion == "Q":
            break

def main():
    global API_CENTRAL_URL, OPENWEATHER_API_KEY

    # Argumentos opcionales
    if len(sys.argv) > 1:
        API_CENTRAL_URL = sys.argv[1]
    if len(sys.argv) > 2:
        OPENWEATHER_API_KEY = sys.argv[2]

    print("[EV_W] Weather Control Office - Release 2")
    print(f"[EV_W] API Central: {API_CENTRAL_URL}")
    print(f"[EV_W] OpenWeather API Key: {'Configurada' if OPENWEATHER_API_KEY else 'NO CONFIGURADA (modo simulacion)'}")
    print(f"[EV_W] Polling cada {POLL_INTERVAL} segundos")
    print("[EV_W] Umbral de alerta: < 0C")

    if not OPENWEATHER_API_KEY:
        print("[EV_W] MODO SIMULACION: Las temperaturas seran aleatorias")
        print("[EV_W] Para usar OpenWeather, configure OPENWEATHER_API_KEY")

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
            # Recargar ubicaciones de BD periodicamente
            ubicaciones_bd = get_ubicaciones_from_db()
            for ub in ubicaciones_bd:
                if ub not in alert_state:
                    alert_state[ub] = False
                    print(f"[EV_W] Nueva ubicacion detectada: {ub}")

            # Consultar temperatura de cada ubicacion
            for ubicacion in list(alert_state.keys()):
                # Obtener ciudad real para OpenWeather
                city = LOCATION_MAPPING.get(ubicacion, ubicacion)

                # Obtener temperatura
                if OPENWEATHER_API_KEY:
                    temp = get_temperature_openweather(city)
                else:
                    temp = get_temperature_simulated(ubicacion)

                if temp is None:
                    continue

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

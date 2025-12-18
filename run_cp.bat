@echo off
cd /d %~dp0
title EVCharging - PC 2 (CP + Weather)
color 0A
echo ==========================================
echo        EVCharging - Release 2
echo        PC 2: Monitor + Engine + Weather
echo ==========================================
echo.

REM ==== CONFIGURACION ====
REM IP del PC 1 donde corre Central/Kafka
set PC1_IP=172.20.243.108
set CENTRAL_PORT=9098
set API_CENTRAL_PORT=5002

REM IP del PC 3 donde corre Registry
set PC3_IP=172.20.243.99
set REGISTRY_PORT=5001

REM Broker Kafka (en PC 1)
set BROKER=%PC1_IP%:9092

REM URL de API_Central (en PC 1) - para EV_W
set API_CENTRAL_URL=http://%PC1_IP%:%API_CENTRAL_PORT%

REM URL del Registry (en PC 3)
set REGISTRY_URL=https://%PC3_IP%:%REGISTRY_PORT%

REM ==== CONFIGURACION DE CPs ====
set CP1_ID=1
set CP1_ENGINE_PORT=7001

echo.
echo ==========================================
echo   Conexiones configuradas:
echo   - Broker Kafka: %BROKER%
echo   - Central TCP: %PC1_IP%:%CENTRAL_PORT%
echo   - API_Central: %API_CENTRAL_URL%
echo   - Registry HTTPS: %REGISTRY_URL%
echo ==========================================
echo.

REM ==== ARRANQUE CP ENGINE ====
echo [ENGINE %CP1_ID%] Iniciando Engine %CP1_ID% en puerto %CP1_ENGINE_PORT%...
start cmd /k "title CP_ENGINE_%CP1_ID% && color 0A && py EV_CP_E.py %BROKER% %CP1_ID% %CP1_ENGINE_PORT%"
timeout /t 2 >nul

REM ==== ARRANQUE CP MONITOR ====
echo.
echo [MONITOR %CP1_ID%] Iniciando Monitor %CP1_ID%...
echo [MONITOR %CP1_ID%] Conectará con Central en %PC1_IP%:%CENTRAL_PORT%
echo [MONITOR %CP1_ID%] Conectará con Registry en %REGISTRY_URL%
start cmd /k "title CP_MONITOR_%CP1_ID% && color 0A && py EV_CP_M.py %CP1_ID% 127.0.0.1 %CP1_ENGINE_PORT% %PC1_IP% %CENTRAL_PORT% %REGISTRY_URL%"
timeout /t 2 >nul

REM ==== ARRANQUE WEATHER CONTROL ====
echo.
echo [WEATHER] Iniciando Weather Control Office...
echo [WEATHER] Conectará con API_Central en %API_CENTRAL_URL%
start cmd /k "title WEATHER_CONTROL && color 0B && py EV_W.py %API_CENTRAL_URL%"
timeout /t 2 >nul

echo.
echo ==========================================
echo   PC 2 (CP + Weather) iniciado correctamente
echo ==========================================
echo   - Engine CP %CP1_ID%: puerto %CP1_ENGINE_PORT%
echo   - Monitor CP %CP1_ID%: conectado a Central
echo   - Weather Control: conectado a API_Central
echo.
echo   IMPORTANTE: Usa el menu del Monitor para:
echo   1. Registrarse en Registry (HTTPS)
echo   2. Autenticarse en Central
echo ==========================================
echo.

pause
exit

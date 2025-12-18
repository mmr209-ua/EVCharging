@echo off
setlocal EnableDelayedExpansion
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
set PC1_IP=192.168.24.1
set CENTRAL_IP=192.168.24.1
set CENTRAL_PORT=9098
set API_CENTRAL_PORT=5002

REM IP del PC 3 donde corre Registry
set PC3_IP=192.168.24.1
set REGISTRY_PORT=5001

REM Broker Kafka (en PC 1)
set BROKER=%PC1_IP%:9092

REM URL de API_Central (en PC 1) - para EV_W
set API_CENTRAL_URL=http://%PC1_IP%:%API_CENTRAL_PORT%

REM URL del Registry (en PC 3)
set REGISTRY_URL=https://%PC3_IP%:%REGISTRY_PORT%

REM ==== CONFIGURACION DE CPs ====
set NUM_CPS=3
set BASE_PORT=7000

for /L %%I in (1, 1, %NUM_CPS%) do (
	set /A ENGINE_PORT=BASE_PORT+%%I

    REM ==== ARRANQUE CP ENGINE ====
    echo [ENGINE %%I] Iniciando Engine %%I en puerto !ENGINE_PORT!...
    start cmd /k "title CP_ENGINE_%%I && color 0A && py EV_CP_E.py %BROKER% %%I !ENGINE_PORT!"
    timeout /t 2 >nul

    REM ==== ARRANQUE CP MONITOR ====
    echo.
    echo [MONITOR %%I] Iniciando Monitor %%I...
    echo [MONITOR %%I] Conectará con Central en %PC1_IP%:%CENTRAL_PORT%
    echo [MONITOR %%I] Conectará con Registry en %REGISTRY_URL%
    start cmd /k "title CP_MONITOR_%%I && color 0A && py EV_CP_M.py %%I 127.0.0.1 !ENGINE_PORT! %CENTRAL_IP% %CENTRAL_PORT% %REGISTRY_URL%"
    timeout /t 2 >nul
)

REM ==== ARRANQUE WEATHER CONTROL ====
echo.
echo [WEATHER] Iniciando Weather Control Office...
echo [WEATHER] Conectará con API_Central en %API_CENTRAL_URL%
start cmd /k "title WEATHER_CONTROL && color 0B && py EV_W.py %API_CENTRAL_URL%"
timeout /t 2 >nul

pause
exit

@echo off
cd /d %~dp0
title EVCharging
color 0A
echo ==========================================
echo        EVCharging - Release 2
echo ==========================================
echo.

setlocal enabledelayedexpansion

REM ==== CONFIGURACION GENERAL ====
set BROKER=192.168.24.1:9092
set CENTRAL_IP=192.168.24.1
set CENTRAL_PORT=9098
set REGISTRY_URL=https://localhost:5001

REM ==== CONFIGURACION DE CPs ====
set NUM_CPS=3
set BASE_PORT=7000

REM ==== ARRANQUE AUTOMATICO DE CPs ====
echo.
echo ==== Iniciando %NUM_CPS% CPs ====
echo.

for /L %%I in (1, 1, %NUM_CPS%) do (
    set /A ENGINE_PORT=!BASE_PORT!+%%I

    echo [ENGINE %%I] Iniciando en puerto !ENGINE_PORT! ...
    start cmd /k "title CP_ENGINE_%%I && color 0A && py EV_CP_E.py %BROKER% %%I !ENGINE_PORT!"
    timeout /t 3 >nul

    echo [MONITOR %%I] Conectando a CENTRAL y ENGINE...
    start cmd /k "title CP_MONITOR_%%I && color 0B && py EV_CP_M.py %%I 127.0.0.1 !ENGINE_PORT! %CENTRAL_IP% %CENTRAL_PORT% %REGISTRY_URL%"
    timeout /t 3 >nul
)

echo.
echo ==== Todos los CPs iniciados correctamente ====
echo.
echo Para registrar cada CP en su Monitor: R
echo Para autenticar cada CP en Central:   A
echo.
pause
exit

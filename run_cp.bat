@echo off
cd /d %~dp0
title EVCharging
color 0A
echo ==========================================
echo        EVCharging - Release 2
echo ==========================================
echo.

REM ==== CONFIGURACION GENERAL ====
set BROKER=192.168.24.1:9092
set CENTRAL_IP=192.168.24.1
set CENTRAL_PORT=9098
set DB_HOST=192.168.24.1

REM ==== CONFIGURACION API Y WEB ====
set REGISTRY_IP=192.168.1.10
set REGISTRY_URL=https://%REGISTRY_IP%:5001
set API_PORT=5002
set WEB_PORT=3000

REM ==== CONFIGURACION DE CPs ====
set CP1_ID=1
set CP1_ENGINE_PORT=7001

REM ==== CONFIGURACION DE DRIVERS ====
set DRIVER1_ID=101

REM ==== INICIALIZAR BASE DE DATOS ====
echo [DB] Inicializando Base de Datos...
py EV_DB.py
timeout /t 1 >nul

REM ==== ARRANQUE CP ENGINE ====
echo.
echo [ENGINE %CP1_ID%] Iniciando Engine %CP1_ID% en puerto %CP1_ENGINE_PORT% ...
start cmd /k "title CP_ENGINE_%CP1_ID% && color 0A && py EV_CP_E.py %BROKER% %CP1_ID% %CP1_ENGINE_PORT%"
timeout /t 2 >nul

REM ==== ARRANQUE CP MONITOR ====
echo.
echo [MONITOR %CP1_ID%] Iniciando Monitor %CP1_ID%...
start cmd /k "title CP_MONITOR_%CP1_ID% && color 0A && py EV_CP_M.py %CP1_ID% 127.0.0.1 %CP1_ENGINE_PORT% 127.0.0.1 %CENTRAL_PORT% %REGISTRY_URL%"
timeout /t 2 >nul

REM ==== ARRANQUE WEATHER CONTROL ====
echo.
echo [WEATHER] Iniciando Weather Control Office...
start cmd /k "title WEATHER_CONTROL_OFFICE && color 0B && py EV_W.py"
timeout /t 2 >nul

REM ==== ARRANQUE WEB DASHBOARD ====
echo.
echo [WEB] Iniciando Dashboard...
cd web_dashboard
if not exist "node_modules" (
    echo      Instalando dependencias npm...
    call npm install
)
start cmd /k "title WEB_DASHBOARD && color 07 && npm start"
cd ..
timeout /t 2 >nul

choice /C SN /M "Abrir Dashboard en navegador?"
if %errorlevel%==1 start http://localhost:%WEB_PORT%

pause
exit
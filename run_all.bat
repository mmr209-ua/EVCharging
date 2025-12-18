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
set CENTRAL_PORT=9098
set DB_HOST=192.168.24.1

REM ==== CONFIGURACION API Y WEB ====
set REGISTRY_URL=https://localhost:5001
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

REM ==== ARRANQUE REGISTRY ====
echo.
echo [REGISTRY] Iniciando Registry...
start cmd /k "title REGISTRY && color 0D && py EV_Registry.py"
timeout /t 3 >nul

REM ==== ARRANQUE CENTRAL ====
echo.
echo [CENTRAL] Iniciando Central en puerto %CENTRAL_PORT% (API en %API_PORT%)...
start cmd /k "title CENTRAL && color 0E && py EV_Central.py %CENTRAL_PORT% %BROKER% %DB_HOST%"
timeout /t 3 >nul

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

REM ==== ARRANQUE DRIVER ====
echo.
echo [DRIVER %DRIVER1_ID%] Iniciando...
start cmd /k "title DRIVER_%DRIVER1_ID% && color 03 && py EV_Driver.py %BROKER% %DRIVER1_ID%"

REM ==== ARRANQUE API CENTRAL ====
echo.
echo [API_CENTRAL] Iniciando API CENTRAL...
start cmd /k "title API_CENTRAL && color 0D && py API_Central.py"
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


echo.
echo ==========================================
echo        SISTEMA RELEASE 2 INICIADO
echo ==========================================
echo.
echo Servicios activos:
echo   - Registry HTTPS: https://localhost:5001
echo   - Central TCP:    localhost:%CENTRAL_PORT%
echo   - API REST:       http://localhost:%API_PORT%
echo   - Weather:        Monitoreando clima
echo   - Dashboard:      http://localhost:%WEB_PORT%
echo   - Kafka broker:   %BROKER%
echo   - CP %CP1_ID%:          Engine + Monitor
echo   - Driver %DRIVER1_ID%
echo.
echo Para registrar CP en el Monitor: 1
echo Para autenticar CP en Central:   2
echo ==========================================
echo.

choice /C SN /M "Abrir Dashboard en navegador?"
if %errorlevel%==1 start http://localhost:%WEB_PORT%

pause
exit
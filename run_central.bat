@echo off
cd /d %~dp0
title EVCharging - PC 1 (CENTRAL)
color 0A
echo ==========================================
echo        EVCharging - Release 2
echo        PC 1: Kafka + Central + API + Web
echo ==========================================
echo.

REM ==== CONFIGURACION ====
set BROKER=192.168.24.1:9092
set CENTRAL_IP=192.168.24.1
set CENTRAL_PORT=9098
set DB_HOST=192.168.24.1
set API_PORT=5002
set WEB_PORT=3000

REM ==== INICIALIZAR BASE DE DATOS ====
echo [DB] Inicializando Base de Datos...
py EV_DB.py
timeout /t 1 >nul

REM ==== ARRANQUE CENTRAL (incluye API_Central internamente) ====
echo.
echo [CENTRAL] Iniciando Central en puerto %CENTRAL_PORT%...
echo [CENTRAL] API_Central se inicia automaticamente en puerto %API_PORT%
start cmd /k "title CENTRAL && color 0E && py EV_Central.py %CENTRAL_PORT% %BROKER% %DB_HOST%"
timeout /t 5 >nul

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

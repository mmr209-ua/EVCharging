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
set API_PORT=5002

REM ==== INICIALIZAR BASE DE DATOS ====
echo [DB] Inicializando Base de Datos...
py EV_DB.py
timeout /t 1 >nul

REM ==== ARRANQUE REGISTRY (HTTPS) ====
echo.
echo [REGISTRY] Iniciando en puerto 5001 (HTTPS)...
start cmd /k "title REGISTRY && color 0B && py EV_Registry.py"
timeout /t 3 >nul

REM ==== ARRANQUE CENTRAL + API ====
echo.
echo [CENTRAL] Iniciando en puerto %CENTRAL_PORT% (API en %API_PORT%)...
start cmd /k "title CENTRAL && color 0E && py EV_Central.py %CENTRAL_PORT% %BROKER% %DB_HOST%"
timeout /t 2 >nul

echo.
echo ==== Central y Registry iniciados ====
echo   - Registry HTTPS: https://localhost:5001
echo   - Central TCP:    localhost:%CENTRAL_PORT%
echo   - API REST:       http://localhost:%API_PORT%
echo.
pause
exit
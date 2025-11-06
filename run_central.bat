@echo off
cd /d %~dp0
title EVCharging
color 0A
echo ==========================================
echo                 EVCharging 
echo ==========================================
echo.

REM ==== CONFIGURACION GENERAL ====
set BROKER=192.168.24.1:9092
set CENTRAL_PORT=9098
set DB_HOST=192.168.24.1

REM ==== ARRANQUE CENTRAL ====
echo.
echo [CENTRAL] Iniciando en puerto %CENTRAL_PORT% ...
start cmd /k "title CENTRAL && py EV_Central.py %CENTRAL_PORT% %BROKER% %DB_HOST%"
timeout /t 2 >nul

exit
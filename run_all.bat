@echo off
cd /d %~dp0
title EVCharging
color 0A
echo ==========================================
echo                 EVCharging 
echo ==========================================
echo.

REM ==== CONFIGURACION GENERAL ====
set BROKER=127.0.0.1:9092
set CENTRAL_PORT=9098
set DB_HOST=127.0.0.1

REM ==== CONFIGURACION DE CPs ====
set CP1_ID=1
set CP1_ENGINE_PORT=7001

REM ==== CONFIGURACION DE DRIVERS ====
set DRIVER1_ID=101

REM ==== ARRANQUE DE KAFKA ====
echo Iniciando Kafka...
timeout /t 2 >nul

REM ==== ARRANQUE CENTRAL ====
echo.
echo [CENTRAL] Iniciando en puerto %CENTRAL_PORT% ...
start cmd /k "title CENTRAL && py EV_Central.py %CENTRAL_PORT% %BROKER% %DB_HOST%"
timeout /t 2 >nul

REM ==== ARRANQUE CP ENGINE ====
echo.
echo [ENGINE %CP1_ID%] Iniciando en puerto %CP1_ENGINE_PORT% ...
start cmd /k "title CP_ENGINE_%CP1_ID% && py EV_CP_E.py %BROKER% %CP1_ID% %CP1_ENGINE_PORT%"
timeout /t 2 >nul

REM ==== ARRANQUE CP MONITOR ====
echo.
echo [MONITOR %CP1_ID%] Conectando a CENTRAL y ENGINE...
start cmd /k "title CP_MONITOR_%CP1_ID% && py EV_CP_M.py %CP1_ID% 127.0.0.1 %CP1_ENGINE_PORT% 127.0.0.1 %CENTRAL_PORT% "
timeout /t 2 >nul

REM ==== ARRANQUE DRIVER ====
echo.
echo [DRIVER %DRIVER1_ID%] Iniciando...
start cmd /k "title DRIVER_%DRIVER1_ID% && py EV_Driver.py %BROKER% %DRIVER1_ID%"

echo.
echo ==========================================
echo Iniciando sistema...
echo - CENTRAL escuchando en %CENTRAL_PORT%
echo - Kafka broker en %BROKER%
echo - CP %CP1_ID% (engine y monitor)
echo - DRIVER %DRIVER1_ID%
echo ==========================================
echo.

pause
exit
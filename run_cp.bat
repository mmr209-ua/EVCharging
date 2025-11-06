@echo off
cd /d %~dp0
title EVCharging
color 0A
echo ==========================================
echo                 EVCharging 
echo ==========================================
echo.

@echo off
REM ==== CONFIGURACION GENERAL ====
setlocal enabledelayedexpansion
set BROKER=192.168.24.1:9092
set CENTRAL_IP=192.168.24.1
set CENTRAL_PORT=9098

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
    	start cmd /k "title CP_ENGINE_%%I && py EV_CP_E.py %BROKER% %%I !ENGINE_PORT!"
    	timeout /t 3 >nul

    	echo [MONITOR %%I] Conectando a CENTRAL y ENGINE...
    	start cmd /k "title CP_MONITOR_%%I && py EV_CP_M.py %%I 127.0.0.1 !ENGINE_PORT! %CENTRAL_IP% %CENTRAL_PORT%"
    	timeout /t 3 >nul
)

echo.
echo ==== Todos los CPs iniciados correctamente ====
exit

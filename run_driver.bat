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

REM ==== CONFIGURACION DE Drivers ====
set NUM_DRIVERS=3

REM ==== ARRANQUE AUTOMATICO DE drivers ====
echo.
echo ==== Iniciando %NUM_DRIVERS% drivers ====
echo.

for /L %%I in (1, 1, %NUM_DRIVERS%) do (
	echo [DRIVER %%I] Iniciando...
	start cmd /k "title DRIVER_%%I && py EV_Driver.py %BROKER% %%I"
	timeout /t 3 >nul    
)

echo.
echo ==== Todos los drivers iniciados correctamente ====
exit
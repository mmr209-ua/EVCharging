@echo off
cd /d %~dp0
title EVCharging - PC 3 (Driver + Registry)
color 0A
echo ==========================================
echo        EVCharging - Release 2
echo        PC 3: Driver + Registry
echo ==========================================
echo.

REM ==== CONFIGURACION ====
REM IP del PC 1 donde corre Central/Kafka/API
set PC1_IP=192.168.24.1
set API_CENTRAL_PORT=5002

REM Broker Kafka (en PC 1)
set BROKER=%PC1_IP%:9092

REM URL de API_Central (en PC 1) - para Registry
set API_CENTRAL_URL=http://%PC1_IP%:%API_CENTRAL_PORT%

REM ==== CONFIGURACION DE DRIVERS ====
set NUM_DRIVERS=3

echo.
echo ==========================================
echo   Conexiones configuradas:
echo   - Broker Kafka: %BROKER%
echo   - API_Central: %API_CENTRAL_URL%
echo ==========================================
echo.

REM ==== ARRANQUE REGISTRY ====
echo [REGISTRY] Iniciando Registry (HTTPS en puerto 5001)...
echo [REGISTRY] Conectara con API_Central en %API_CENTRAL_URL%
start cmd /k "title REGISTRY && color 0D && py EV_Registry.py %API_CENTRAL_URL%"
timeout /t 3 >nul

for /L %%I in (1, 1, %NUM_DRIVERS%) do (

	REM ==== ARRANQUE DRIVER ====
	echo.
	echo [DRIVER %%I] Iniciando Driver %%I...
	echo [DRIVER %%I] Conectará con Kafka en %BROKER%
	start cmd /k "title DRIVER_%%I && color 03 && py EV_Driver.py %BROKER% %%I"
	timeout /t 2 >nul
)

pause
exit

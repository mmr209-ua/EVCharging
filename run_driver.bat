@echo off
cd /d %~dp0
title EVCharging - PC 3 (Driver + Registry)
color 0A
echo ==========================================
echo        EVCharging - Release 2
echo        PC 3: Driver + Registry
echo ==========================================
echo.

REM ==== CONFIGURACION - MODIFICAR SEGUN TU RED ====
REM IP del PC 1 donde corre Central/Kafka/API
set PC1_IP=192.168.24.1
set API_CENTRAL_PORT=5002

REM Broker Kafka (en PC 1)
set BROKER=%PC1_IP%:9092

REM URL de API_Central (en PC 1) - para Registry
set API_CENTRAL_URL=http://%PC1_IP%:%API_CENTRAL_PORT%

REM ==== CONFIGURACION DE DRIVERS ====
set DRIVER1_ID=101

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

REM ==== ARRANQUE DRIVER ====
echo.
echo [DRIVER %DRIVER1_ID%] Iniciando Driver %DRIVER1_ID%...
echo [DRIVER %DRIVER1_ID%] Conectara con Kafka en %BROKER%
start cmd /k "title DRIVER_%DRIVER1_ID% && color 03 && py EV_Driver.py %BROKER% %DRIVER1_ID%"
timeout /t 2 >nul

echo.
echo ==========================================
echo   PC 3 (Driver + Registry) iniciado
echo ==========================================
echo   - Registry HTTPS: puerto 5001
echo   - Driver ID: %DRIVER1_ID%
echo.
echo   Registry conecta con API_Central en PC 1
echo   Driver conecta con Kafka en PC 1
echo ==========================================
echo.

pause
exit

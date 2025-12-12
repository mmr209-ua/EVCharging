@echo off
cd /d %~dp0
title EVCharging
color 0D
echo ==========================================
echo        EVCharging - Release 2
echo ==========================================
echo.

REM ==== ARRANQUE WEATHER CONTROL ====
echo [WEATHER] Iniciando Weather Control Office...
echo.
echo Modo: Simulacion (sin API key de OpenWeather)
echo Umbral de alerta: temperatura menor 0C
echo.
start cmd /k "title WEATHER && color 0D && py EV_W.py"

echo.
echo ==== Weather Control iniciado ====
echo.
pause
exit

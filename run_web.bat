@echo off
cd /d %~dp0
title EVCharging
color 0C
echo ==========================================
echo        EVCharging - Release 2
echo ==========================================
echo.

set WEB_PORT=3000

REM ==== ARRANQUE WEB DASHBOARD ====
echo [WEB] Iniciando Dashboard en puerto %WEB_PORT%...
cd web_dashboard
if not exist "node_modules" (
    echo      Instalando dependencias npm...
    call npm install
)
start cmd /k "title WEB_DASHBOARD && color 0C && npm start"
cd ..
timeout /t 3 >nul

echo.
echo ==== Web Dashboard iniciado ====
echo.
echo Dashboard disponible en: http://localhost:%WEB_PORT%
echo.

choice /C SN /M "Abrir en navegador?"
if %errorlevel%==1 start http://localhost:%WEB_PORT%

pause
exit

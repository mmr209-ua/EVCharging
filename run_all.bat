@echo off
cd /d %~dp0
REM === Demo de despliegue local (ajusta rutas, bootstrap y DB host) ===
set PYTHON=py
set BOOTSTRAP=localhost:9092
set CENTRAL_PORT=7000
set DBHOST=localhost

REM Crear topics base (opcional si el cluster los crea automáticamente)
%PYTHON% -c "import EV_Topics as T; T.ensure_topics(r'%BOOTSTRAP%')"

REM Iniciar CENTRAL en nueva ventana
start "EV_Central" %PYTHON% EV_Central.py --port %CENTRAL_PORT% --bootstrap %BOOTSTRAP% --dbhost %DBHOST%
timeout /t 3 >nul

REM Iniciar CP #1 Engine y Monitor
start "EV_CP_E CP01" %PYTHON% EV_CP_E.py --cp_id CP01 --price 0.35 --bootstrap %BOOTSTRAP% --port 7101
timeout /t 3 >nul
start "EV_CP_M CP01" %PYTHON% EV_CP_M.py --central_host localhost --central_port %CENTRAL_PORT% --engine_host localhost --engine_port 7101 --cp_id CP01 --price 0.35 --location "Centro"
timeout /t 3 >nul

REM Iniciar CP #2 Engine y Monitor
start "EV_CP_E CP02" %PYTHON% EV_CP_E.py --cp_id CP02 --price 0.40 --bootstrap %BOOTSTRAP% --port 7102
timeout /t 3 >nul
start "EV_CP_M CP02" %PYTHON% EV_CP_M.py --central_host localhost --central_port %CENTRAL_PORT% --engine_host localhost --engine_port 7102 --cp_id CP02 --price 0.40 --location "Puerto"
timeout /t 3 >nul

REM Iniciar Driver #101 solicitando en CP01 y CP02 (en ese orden)
echo CP01>services.txt
echo CP02>>services.txt
start "EV_Driver 101" %PYTHON% EV_Driver.py --bootstrap %BOOTSTRAP% --driver_id 101 --services_file services.txt

echo.
echo Demo lanzada. Recuerda tener Kafka corriendo y la BBDD MySQL (EVCharging) creada.
echo En las ventanas de CP_E pulsa 'p' para simular enchufado, 'k' para simular KO.
echo.
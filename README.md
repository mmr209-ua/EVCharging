# EVCharging

## 1. Instalar la biblioteca kafka-python, que permite conectarse y trabajar con Kafka desde python 
pip install kafka-python

## 2. Modificar los siguientes archivos
- server properties → modificar las IPs en las propiedades listeners y advertised.listeners
- archivos *.bat → modificar las IPs de la configuración general

## EXTRA: Crear carpeta logs y configurar metadatos de los logs
- \[guid\]::NewGuid().ToString()
- .\bin\windows\kafka-storage.bat format -t e82e4dd0-7aa6-426a-a2ac-e52f25a957ef -c .\config\server.properties

## 3. Poner en marcha kafka
bin\windows\kafka-server-start.bat .\config\server.properties

## 4. Ejecutar los archivos .bat (dándole doble click) en el siguiente orden
run_central.bat
run_cp.bat
run_driver.bat

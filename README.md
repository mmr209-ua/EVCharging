# EVCharging

## Instalar
pip install kafka-python

## Configurar metadatos de los logs
- \[guid\]::NewGuid().ToString()
- .\bin\windows\kafka-storage.bat format -t e82e4dd0-7aa6-426a-a2ac-e52f25a957ef -c .\config\server.properties

## Ejecutar kafka
bin\windows\kafka-server-start.bat .\config\server.properties

## Parametrizar IPs en los ficheros
- server.properties
- *.bat

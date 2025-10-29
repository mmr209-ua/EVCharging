# Topics de comunicación CP -> Central
CP_REGISTER = "CP_REGISTER"                         # Registro de nuevos CPs
CP_HEALTH = "CP_HEALTH"                             # Monitorización de salud para detectar averías
CP_STATUS = "CP_STATUS"                             # Estados de los CPs (activo, suministrando, etc.)
CP_CONSUMPTION = "CP_CONSUMPTION"                   # Información acerca del CP suminstrando en tiempo real (consumo, importe, id conductor)
CP_SUPPLY_COMPLETE = "CP_SUPPLY_COMPLETE"           # Fin de suministro y ticket

# Topics de comunicación Central -> CP
CP_CONTROL = "CP_CONTROL"                           # Parar/Reanudar CP

# Topics de comunicación Central -> CP y Driver
CP_AUTHORIZE_SUPPLY = "CP_AUTHORIZE_SUPPLY"	        # Autorización de suministro

# Topics de comunicación Central -> Driver
SUPPLY_STATUS = "SUPPLY_STATUS"	                    # Estado del suministro
DRIVER_SUPPLY_COMPLETE = "DRIVER_SUPPLY_COMPLETE"   # Fin de suministro y ticket

# Topics de comunicación Driver -> Central
SUPPLY_REQUEST_TO_CENTRAL = "SUPPLY_REQUEST_TO_CENTRAL" 

# Topics de comunicación Driver -> CP
SUPPLY_REQUEST_VIA_CP = "SUPPLY_REQUEST_VIA_CP" 


CHARGING_REQUESTS = "CHARGING_REQUESTS"
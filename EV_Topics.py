# Topics de comunicación CP -> Central
CP_REGISTER = "cp_register"                         # Registro de nuevos CPs
CP_HEALTH = "cp_health"                             # Monitorización de salud para detectar averías
CP_STATUS = "cp_status"                             # Estados de los CPs (activo, suministrando, etc.)
CP_CONSUMPTION = "cp_consumption"                   # Información acerca del CP suminstrando en tiempo real (consumo, importe, id conductor)
CP_SUPPLY_COMPLETE = "cp_supply_complete"           # Fin de suministro y ticket

# Topics de comunicación Central -> CP
CP_CONTROL = "cp_control"                           # Parar/Reanudar CP

# Topics de comunicación Central -> CP y Driver
CP_AUTHORIZE_SUPPLY = "cp_authorize_supply"	        # Autorización de suministro

# Topics de comunicación Central -> Driver
SUPPLY_STATUS = "supply_status"	                    # Estado del suministro
DRIVER_SUPPLY_COMPLETE = "driver_supply_complete"   # Fin de suministro y ticket

# Topics de comunicación Driver -> Central
SUPPLY_REQUEST_TO_CENTRAL = "supply_request_to_central" 

# Topics de comunicación Driver -> CP
SUPPLY_REQUEST_TO_CP = "supply_request_to_cp"           
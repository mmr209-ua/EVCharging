# Topics de comunicación CP -> Central
CP_REGISTER = "cp_register"         # Registro de nuevos CPs
CP_STATUS = "cp_status"             # Estados de los CPs (activo, suministrando, etc.)
CP_ALERTS = "cp_alerts"             # Alertas/averías/consumo
CP_HEALTH = "cp_health"             # Monitorización de salud
CP_CONSUMPTION = "cp_consumption"   # Consumo en tiempo real
CP_SUPPLY_COMPLETE = "cp_supply_complete"	# Fin de suministro y ticket

# Topics de comunicación Central -> CP
CP_CONTROL = "cp_control"	                # Parar/Reanudar CP
CP_AUTHORIZE_SUPPLY = "cp_authorize_supply"	# Autorización de suministro

# Topics de comunicación Driver -> Central
CHARGING_REQUESTS = "charging_requests" # Peticiones de recarga

# Topics de comunicación Central -> Driver
SUPPLY_STATUS = "supply_status"	                    # Estado del suministro
DRIVER_SUPPLY_COMPLETE = "driver_supply_complete"	# Fin de suministro y ticket

# Más topics 
FILE_REQUESTS = "file_requests" # Peticiones desde archivo (Sistema → CENTRAL)
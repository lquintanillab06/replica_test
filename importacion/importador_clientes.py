from .importador_audit import ImportadorAudit
from database import LocalDataBaseConnection
from src import get_sucursales_activas, get_conexion_remota, get_sucursal_replica
from datetime import datetime
from simple_chalk import chalk

def importar_clientes():
    localDB = LocalDataBaseConnection()
    importador = ImportadorAudit(localDB, table_name='cliente',dispersar=True)
    importador.importar()
        
        


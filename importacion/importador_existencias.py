from database import LocalDataBaseConnection
from .importador_audit import ImportadorAudit

def importar_existencia():   
    localDB = LocalDataBaseConnection()
    importador = ImportadorAudit(localDB, table_name='existencia', dispersar=True)
    importador.importar()
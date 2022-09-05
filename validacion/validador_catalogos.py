from datetime import datetime
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query,obtener_diferencias
from database import LocalDataBaseConnection

def validar_catalogo(table_name,columnas):

    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()

    print(chalk.cyan(f"Iniciando la  Validacion de {table_name.upper()} {datetime.now()}......."))
    columnas_str = ",".join(columnas)
    sql_catalogo = f" Select {columnas_str} from {table_name}"
    sql_elemento = f"Select * from {table_name} where id = %(id)s"
    sql_update =resolver_update_query(localDB,table_name,localDB.database)
    sql_insert =resolver_insert_query(localDB,table_name,localDB.database)

    localDB.cursor.execute(sql_catalogo)
    catalogo_cen = localDB.cursor.fetchall()

    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)
        if remoteDB:
            remoteDB.cursor.execute(sql_catalogo)
            catalogo_suc = remoteDB.cursor.fetchall()
            diferencias = obtener_diferencias(catalogo_cen,catalogo_suc)
            print(f"Diferencias {len(diferencias)} ")
            for diferencia in diferencias:
        
                localDB.cursor.execute(sql_elemento,{"id": diferencia['id']})
                elemento_cen = localDB.cursor.fetchone()
                if elemento_cen:
                    remoteDB.cursor.execute(sql_elemento,{"id": diferencia['id']})
                    elemento_suc = remoteDB.cursor.fetchone()
                    if elemento_suc:
                        print("Actualizando el registo...")
                        try:
                            remoteDB.cursor.execute(sql_update,elemento_cen)   
                            remoteDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo actualizar el registro"))
                            print(chalk.red(e))
                            remoteDB.database.rollback()
                    else:
                        print("Insertando El Registro...")
                        try:
                            remoteDB.cursor.execute(sql_insert,elemento_cen)   
                            remoteDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo insertar el registro"))
                            print(chalk.red(e))
                            remoteDB.database.rollback()
            
    

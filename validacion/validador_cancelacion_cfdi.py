from datetime import datetime
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query
from database import LocalDataBaseConnection


def validar_cfdis():
    print(chalk.cyan(f"Iniciando la  Validacion de Cfdis cancelados {datetime.now()}......."))

    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()
    sql_cfdi_cancelados = "select * from cfdi where date(last_updated) = CURRENT_DATE and status = 'CANCELACION_PENDIENTE'"
    sql_cfdi = """Select * from cfdi where id = %(id)s"""
    sql_update =resolver_update_query(localDB,'cfdi',localDB.database)
    sql_insert =resolver_insert_query(localDB,'cfdi',localDB.database)

    print("_"* 100)
    print(sql_insert)
    print("_"* 100)
    print(sql_update)
    
    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)
        if remoteDB :
            remoteDB.cursor.execute(sql_cfdi_cancelados)
            cfdis_cancelados = remoteDB.cursor.fetchall()
            for cfdi_cancelado in cfdis_cancelados:
                print("*"* 100)
                print(cfdi_cancelado)
                localDB.cursor.execute(sql_cfdi,{"id":cfdi_cancelado["id"]})
                cfdi_cen = localDB.cursor.fetchone()
                if cfdi_cen:
                    print("+"* 100)
                    print(cfdi_cen)
                    if not cfdi_cen['status']:
                        try:
                            localDB.cursor.execute(sql_update,cfdi_cancelado)
                            localDB.db.commit()
                            print("actualizar")
                        except Exception as e:
                            mensaje = "No se pudo actualizar el registro"
                            print(chalk.red(mensaje))
                            print(chalk.red(e))
                            localDB.db.rollback()
                else:
                    try:
                        localDB.cursor.execute(sql_insert,cfdi_cancelado)
                        localDB.db.commit()
                        print("insertar")
                    except Exception as e:
                        mensaje = "No se pudo insertar el registro"
                        print(chalk.red(mensaje))
                        print(chalk.red(e))
                        localDB.db.rollback()
                print("*"* 100)
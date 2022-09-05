from datetime import datetime
from simple_chalk import chalk
from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query,obtener_diferencias,obtener_sucursal
from database import LocalDataBaseConnection



def sincronizar_existencia_central():
    print("Ejecutando la sincronizacion de las existencias en oficinas ...")
    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()
    sql_existencias = """ Select id,cantidad from existencia where sucursal_id = %(sucursal_id)s and mes = %(mes)s and anio = %(anio)s"""
    sql_existencia = """Select * from existencia where id = %(id)s"""
    sql_update =resolver_update_query(localDB,'existencia',localDB.database)
    sql_insert =resolver_insert_query(localDB,'existencia',localDB.database)

    hoy = datetime.now()
    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)

        if remoteDB:       
            suc = obtener_sucursal(sucursal['server'])
            remoteDB.cursor.execute(sql_existencias,{"sucursal_id":suc['id'],"mes": hoy.month, "anio": hoy.year})
            existencias_suc = remoteDB.cursor.fetchall()
            localDB.cursor.execute(sql_existencias,{"sucursal_id":suc['id'],"mes": hoy.month, "anio": hoy.year})
            existencias_cen = localDB.cursor.fetchall()
            diferencias = obtener_diferencias(existencias_suc, existencias_cen)
            print(f"Diferencias: {len(diferencias)}")
            for diferencia in diferencias:
                print(diferencia)
                remoteDB.cursor.execute(sql_existencia,{"id": diferencia['id']})
                existencia_suc = remoteDB.cursor.fetchone()
                if existencia_suc :
                    localDB.cursor.execute(sql_existencia,{"id": diferencia['id']})
                    existencia_cen = localDB.cursor.fetchone()
                    if existencia_cen :
                        try:
                            localDB.cursor.execute(sql_update,existencia_suc)
                            localDB.db.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo actualizar el registro"))
                            print(chalk.red(e))
                            localDB.db.rollback()
                    else:
                        try:
                            localDB.cursor.execute(sql_insert,existencia_suc)
                            localDB.db.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo ingresar el registro"))
                            print(chalk.red(e))
                        



def sincronizar_existencia_sucursal():
    print("Ejecutando la sincronizacion de las existencias en sucursal ...")
    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()
    sql_existencias = """ Select id, cantidad from existencia where sucursal_id <> %(sucursal_id)s and mes = %(mes)s and anio = %(anio)s"""
    sql_existencia = """Select * from existencia where id = %(id)s"""
    sql_update =resolver_update_query(localDB,'existencia',localDB.database)
    sql_insert =resolver_insert_query(localDB,'existencia',localDB.database)

    hoy = datetime.now()
    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)

        if remoteDB:

            suc = obtener_sucursal(sucursal['server'])
            localDB.cursor.execute(sql_existencias,{"sucursal_id":suc['id'],"mes": hoy.month, "anio": hoy.year})
            existencias_cen = localDB.cursor.fetchall()
            remoteDB.cursor.execute(sql_existencias,{"sucursal_id":suc['id'],"mes": hoy.month, "anio": hoy.year})
            existencias_suc = remoteDB.cursor.fetchall()
            
            diferencias = obtener_diferencias(existencias_cen, existencias_suc)
            print(f"Diferencias: {len(diferencias)}")

            for diferencia in diferencias:
                localDB.cursor.execute(sql_existencia,{"id": diferencia['id']})
                existencia_cen = localDB.cursor.fetchone()
                if existencia_cen :
                    remoteDB.cursor.execute(sql_existencia,{"id": diferencia['id']})
                    existencia_suc = remoteDB.cursor.fetchone()
                    if existencia_suc:
                        try:
                            remoteDB.cursor.execute(sql_update,existencia_cen)
                            remoteDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo actualizar el registro"))
                            print(chalk.red(e))
                            remoteDB.database.rollback()
                    else:
                        try:
                            remoteDB.cursor.execute(sql_insert,existencia_cen)
                            remoteDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo ingresar el registro"))
                            print(chalk.red(e))
                            remoteDB.database.rollback()


                    
                    



def sincronizar_existencia():
    sincronizar_existencia_central()
    sincronizar_existencia_sucursal()
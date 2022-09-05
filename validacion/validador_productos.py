from datetime import datetime
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query,obtener_diferencias
from database import LocalDataBaseConnection



def validar_productos():

    print(chalk.cyan(f"Iniciando la  Validacion de Productos {datetime.now()}......."))

    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()

    sql_productos = """ Select id,clave,descripcion,precio_contado,precio_credito,precio_tarjeta from producto"""
    sql_producto = """Select * from producto where id = %(id)s"""
    sql_update =resolver_update_query(localDB,'producto',localDB.database)
    sql_insert =resolver_insert_query(localDB,'producto',localDB.database)

    localDB.cursor.execute(sql_productos)
    productos_cen = localDB.cursor.fetchall()

    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)
        if remoteDB :
            remoteDB.cursor.execute(sql_productos)
            productos_suc = remoteDB.cursor.fetchall()
            diferencias = obtener_diferencias(productos_cen,productos_suc)
            for diferencia in diferencias:
                localDB.cursor.execute(sql_producto,{"id": diferencia['id']})
                producto_cen = localDB.cursor.fetchone()
                if producto_cen:
                    remoteDB.cursor.execute(sql_producto,{"id": diferencia['id']})
                    producto_suc = remoteDB.cursor.fetchone()
                    if producto_suc:
                        print("Actualizando Producto ...")
                        try:
                            remoteDB.cursor.execute(sql_update,producto_cen)   
                            remoteDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo actualizar el registro"))
                            print(chalk.red(e))
                            remoteDB.database.rollback()
                    else:
                        print("Insertando Producto ...")
                        try:
                            remoteDB.cursor.execute(sql_insert,producto_cen)   
                            remoteDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo ingresar el registro"))
                            print(chalk.red(e))
                            remoteDB.database.rollback()

                
            




    
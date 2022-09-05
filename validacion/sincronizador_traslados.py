from datetime import datetime,date
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query,obtener_diferencias, obtener_sucursal
from database import LocalDataBaseConnection

def sincronizar_traslados():
    print(chalk.cyan(f"Iniciando la validacion de Traslados {datetime.now()}......."))

    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()

    sql_trds = "select id, fecha_inventario, cfdi_id from traslado where date(fecha) = %(fecha)s and sucursal_id = %(sucursal_id)s"
    sql_trd_dets = "select  d.id, d.inventario_id from traslado_det d join traslado t on (d.traslado_id = t.id) where date(t.fecha) = %(fecha)s "

    sql_trd = "select * from traslado where id = %(id)s"
    sql_trd_det = "select * from traslado_det where traslado_id = %(id)s"
    sql_cfdi = "select * from cfdi where id = %(id)s"
    sql_inventario = "select * from inventario where id = %(id)s"

    fecha = date.today()

    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)
        if remoteDB :
            print(chalk.magenta(f"Validando la sucursal {sucursal['server']} del día {fecha}" ))
            suc = obtener_sucursal(sucursal['server'])
            
            remoteDB.cursor.execute(sql_trds,{"fecha": fecha, "sucursal_id": suc['id']})

            traslados_sucursal = remoteDB.cursor.fetchall()
            localDB.cursor.execute(sql_trds,{"fecha": fecha, "sucursal_id": suc['id']})
            traslados_central = localDB.cursor.fetchall()

            diferencias_vs_oficinas = obtener_diferencias(traslados_sucursal,traslados_central)
            for dif in diferencias_vs_oficinas:
                print(dif)

            print(f"Diferencias 1: {len(diferencias_vs_oficinas)}")
            print("*"*100)

            diferencias_vs_sucursal = obtener_diferencias(traslados_central,traslados_sucursal)
            for dif in diferencias_vs_sucursal:
                print(dif)
            
            print(f"Diferencias 2: {len(diferencias_vs_sucursal)}")
            print("+"*100)

            print(f"Traslados en Sucursal {len(traslados_sucursal)}")
            print(f"Traslados en Central {len(traslados_central)}")


from datetime import datetime
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query
from database import LocalDataBaseConnection

def exportar_vales():
    print(chalk.yellow(f"Iniciando la exportacion de Vales {datetime.now()}......."))
    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()

    sql_insert_sol =resolver_insert_query(localDB,'solicitud_de_traslado','siipapx_tacuba')
    sql_insert_sol_det =resolver_insert_query(localDB,'solicitud_de_traslado_det','siipapx_tacuba')

    sql_update_sol =resolver_update_query(localDB,'solicitud_de_traslado','siipapx_tacuba')
    sql_update_sol_det =resolver_update_query(localDB,'solicitud_de_traslado_det','siipapx_tacuba')

    sql = "select * from audit_log where date_replicated is null and table_name = 'solicitud_de_traslado' and target = %(target)s order by date_created"
    sql_sol = "select * from solicitud_de_traslado where id = %(id)s"
    sql_sol_det = "select * from solicitud_de_traslado_det where solicitud_de_traslado_id = %(id)s"


    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)
        if remoteDB :
            localDB.cursor.execute(sql,{'target': sucursal['server']})
            audits = localDB.cursor.fetchall()
            for audit in audits:
                localDB.cursor.execute(sql_sol,{'id': audit['persisted_object_id']})
                sol = localDB.cursor.fetchone()
                if audit['event_name'] == 'DELETE' or sol:
                    print(chalk.yellow(audit))
                    print(sol)
                    if audit['event_name'] == 'INSERT':
                        try:
                            #remoteDB.cursor.execute(sql_insert_sol,sol)
                            localDB.cursor.execute(sql_sol_det,{'id': audit['persisted_object_id']})
                            dets = localDB.cursor.fetchall()
                            for det in dets:
                                #remoteDB.cursor.execute(sql_insert_sol_det,det)                            
                                print(chalk.yellow(det))
                            #localDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo insertar el registro"))
                            print(chalk.red(e))
                            #localDB.database.rollback()
                    if audit['event_name'] == 'UPDATE':
                        try:
                            #remoteDB.cursor.execute(sql_update_sol,sol)
                            localDB.cursor.execute(sql_sol_det,{'id': audit['persisted_object_id']})
                            dets = localDB.cursor.fetchall()
                            for det in dets:
                                print(chalk.yellow(det))
                                #localDB.cursor.execute(sql_update_sol_det,det)   
                            #localDB.database.commit()
                        except Exception as e:
                            print(chalk.red("No se pudo actualizar el registro"))
                            print(chalk.red(e))
                           #localDB.database.rollback()
                    if audit['event_name'] == 'DELETE':
                        sql_delete_sol = "delete from solicitud_de_traslado where id = %(id)s"
                        sql_delete_sol_det = "delete from solicitud_de_traslado_det where id = %(id)s"
                        try: 
                            localDB.cursor.execute(sql_sol_det,{'id': audit['persisted_object_id']})
                            dets = localDB.cursor.fetchall()
                            for det in dets:
                                print(chalk.yellow(det))
                                #remoteDB.cursor.execute(sql_delete_sol_det,{"id":det['id']}) 
                            #remoteDB.cursor.execute(sql_delete_sol,{"id": audit['persisted_object_id']}) 
                            #remoteDB.database.commit() 
                        except Exception as e:
                            print(chalk.red("No se pudo eliminar el registro"))
                            print(chalk.red(e))
                            localDB.database.rollback()
            print( f"Audits: {len(audits)}")
            remoteDB.cursor.close()


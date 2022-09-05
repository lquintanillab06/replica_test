from datetime import datetime
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query
from database import LocalDataBaseConnection

def importar_vales():
    print(chalk.cyan(f"Iniciando la importacion de Vales {datetime.now()}......."))

    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()

    sql_insert_sol =resolver_insert_query(localDB,'solicitud_de_traslado',localDB.database)
    sql_insert_sol_det =resolver_insert_query(localDB,'solicitud_de_traslado_det',localDB.database)

    sql_update_sol =resolver_update_query(localDB,'solicitud_de_traslado',localDB.database)
    sql_update_sol_det =resolver_update_query(localDB,'solicitud_de_traslado_det',localDB.database)

    sql = "select * from audit_log where date_replicated is null and table_name = 'solicitud_de_traslado' order by date_created"
    sql_sol = "select * from solicitud_de_traslado where id = %(id)s"
    sql_sol_det = "select * from solicitud_de_traslado_det where solicitud_de_traslado_id = %(id)s"
    

    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)
        if remoteDB :
            remoteDB.cursor.execute(sql)
            audits = remoteDB.cursor.fetchall()
            for audit in audits:
                remoteDB.cursor.execute(sql_sol,{'id': audit['persisted_object_id']})
                sol = remoteDB.cursor.fetchone()
                if audit['event_name'] == 'DELETE' or sol:
                    if audit['event_name'] == 'INSERT':
                        try:
                            localDB.cursor.execute(sql_insert_sol,sol)
                            remoteDB.cursor.execute(sql_sol_det,{'id': audit['persisted_object_id']})
                            dets = remoteDB.cursor.fetchall()
                            for det in dets:
                                localDB.cursor.execute(sql_insert_sol_det,det)                            
                            localDB.db.commit()
                            after_process_vale(localDB,audit,sol)
                            mensaje = "Insert realizado desde Replica Python"
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                        except Exception as e:
                            mensaje = "No se pudo insertar el registro"
                            print(chalk.red(mensaje))
                            print(chalk.red(e))
                            localDB.db.rollback()
                            actualizar_audit(audit['id'], mensaje, remoteDB)

                    if audit['event_name'] == 'UPDATE':
                        try:
                            localDB.cursor.execute(sql_update_sol,sol)
                            remoteDB.cursor.execute(sql_sol_det,{'id': audit['persisted_object_id']})
                            dets = remoteDB.cursor.fetchall()
                            for det in dets:
                                localDB.cursor.execute(sql_update_sol_det,det)   
                            localDB.db.commit()
                            after_process_vale(localDB,audit,sol)
                            mensaje = "Update realizado desde Replica Python"
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                        except Exception as e:
                            mensaje = "No se pudo actualizar el registro"
                            print(chalk.red(mensaje))
                            print(chalk.red(e))
                            localDB.db.rollback()
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                        
                    if audit['event_name'] == 'DELETE':
                        sql_delete_sol = "delete from solicitud_de_traslado where id = %(id)s"
                        sql_delete_sol_det = "delete from solicitud_de_traslado_det where id = %(id)s"
                        try: 
                            remoteDB.cursor.execute(sql_sol_det,{'id': audit['persisted_object_id']})
                            dets = remoteDB.cursor.fetchall()
                            for det in dets:
                                localDB.cursor.execute(sql_delete_sol_det,{"id":det['id']}) 
                            localDB.cursor.execute(sql_delete_sol,{"id": audit['persisted_object_id']}) 
                            localDB.db.commit()
                            after_process_vale(localDB,audit,sol)
                            mensaje = "Delete realizado desde Replica Python"
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                        except Exception as e:
                            mensaje = "No se pudo eliminar el registro"
                            print(chalk.red(mensaje))
                            print(chalk.red(e))
                            localDB.db.rollback()
                            actualizar_audit(audit['id'], mensaje, remoteDB)

            print( f"Audits: {len(audits)}")
            remoteDB.cursor.close()
        
def actualizar_audit(audit_id, mensaje, remoteDB):
    sql_update = "update audit_log set message = %(mensaje)s , date_replicated = %(replicado)s where id = %(id)s"
    try:
        remoteDB.cursor.execute(sql_update,{"mensaje": mensaje,"replicado": datetime.now(), "id": audit_id})
        remoteDB.database.commit()
    except Exception as e:
        print(chalk.red("No se pudo insertar el registro de audit_log"))
        print(chalk.red(e))
        remoteDB.rollback()  

def after_process_vale(localDB,audit_origen,sol):

    localDB.cursor.execute("select * from sucursal where id = %(sucursal_id)s",{"sucursal_id": sol['sucursal_atiende_id']}) 
    sucursal_atiende = localDB.cursor.fetchone()
    localDB.cursor.execute("select * from sucursal where id = %(sucursal_id)s",{"sucursal_id": sol['sucursal_solicita_id']}) 
    sucursal_solicita = localDB.cursor.fetchone()
    audit_atiende = {
        'version': audit_origen['version'],
        'persisted_object_id': audit_origen['persisted_object_id'],
        'target':sucursal_atiende['nombre'],
        'date_created': datetime.now(),
        'last_updated': datetime.now(),
        'name':audit_origen['name'],
        'event_name': audit_origen['event_name'],
        'table_name': audit_origen['table_name'],
        'source': 'CENTRAL',
    }
    sql_insert_audit = """insert into audit_log (version,persisted_object_id,target,date_created,last_updated,name,event_name,table_name,source)
                        values (%(version)s, %(persisted_object_id)s,%(target)s,%(date_created)s,%(last_updated)s,%(name)s,%(event_name)s,%(table_name)s,%(source)s)"""

    audit_solicita = {
        'version': audit_origen['version'],
        'persisted_object_id': audit_origen['persisted_object_id'],
        'target':sucursal_solicita['nombre'],
        'date_created': datetime.now(),
        'last_updated': datetime.now(),
        'name':audit_origen['name'],
        'event_name': audit_origen['event_name'],
        'table_name': audit_origen['table_name'],
        'source': 'CENTRAL',
    }

    try:
        localDB.cursor.execute(sql_insert_audit,audit_atiende) 
        localDB.cursor.execute(sql_insert_audit,audit_solicita) 
        localDB.db.commit()
    except Exception as e:
        print(chalk.red("No se pudo insertar el registro de audit_log"))
        print(chalk.red(e))
        localDB.rollback()


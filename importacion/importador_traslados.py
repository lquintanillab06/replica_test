from datetime import datetime
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, resolver_insert_query, resolver_update_query
from database import LocalDataBaseConnection

def importar_traslados():
    print(chalk.cyan(f"Iniciando la importacion de Traslados {datetime.now()}......."))

    localDB = LocalDataBaseConnection()
    sucursales = get_sucursales_activas()

    sql_insert_trd =resolver_insert_query(localDB,'traslado',localDB.database)
    sql_insert_trd_det =resolver_insert_query(localDB,'traslado_det',localDB.database)
    sql_insert_cfdi =resolver_insert_query(localDB,'cfdi',localDB.database)
    sql_insert_inventario =resolver_insert_query(localDB,'inventario',localDB.database)

    sql_update_trd =resolver_update_query(localDB,'traslado',localDB.database)
    sql_update_trd_det =resolver_update_query(localDB,'traslado_det',localDB.database)
    #sql_update_cfdi =resolver_update_query(localDB,'cfdi',localDB.database)


    sql = "select * from audit_log where date_replicated is null and table_name = 'traslado' order by date_created"
    sql_trd = "select * from traslado where id = %(id)s"
    sql_trd_det = "select * from traslado_det where traslado_id = %(id)s"
    sql_cfdi = "select * from cfdi where id = %(id)s"
    sql_inventario = "select * from inventario where id = %(id)s"
    

    for sucursal in sucursales:
        remoteDB = get_conexion_remota(sucursal)
        if remoteDB :
            remoteDB.cursor.execute(sql)
            audits = remoteDB.cursor.fetchall()
            for audit in audits:
                remoteDB.cursor.execute(sql_trd,{'id': audit['persisted_object_id']})
                trd = remoteDB.cursor.fetchone()
                if audit['event_name'] == 'DELETE' or trd:

                    if audit['event_name'] == 'INSERT':
                
                        if trd['cfdi_id']:
                            remoteDB.cursor.execute(sql_cfdi,{'id': trd['cfdi_id']})
                            cfdi = remoteDB.cursor.fetchone()
                            if cfdi:
                                try:
                                    localDB.cursor.execute(sql_insert_cfdi,cfdi)                         
                                    localDB.db.commit()
                                except Exception as e:
                                    mensaje = "No se pudo insertar el registro de cfdi"
                                    print(chalk.red(mensaje))
                                    print(chalk.red(e))
                                    localDB.db.rollback()
                        try:
                            localDB.cursor.execute(sql_insert_trd,trd)
                            remoteDB.cursor.execute(sql_trd_det,{'id': audit['persisted_object_id']})
                            dets = remoteDB.cursor.fetchall()
                            for det in dets:
                                if det['inventario_id']:
                                    remoteDB.cursor.execute(sql_inventario,{'id': det['inventario_id']})
                                    inventario = remoteDB.cursor.fetchone()
                                    if inventario:
                                        try:
                                            localDB.cursor.execute(sql_insert_inventario,inventario)                         
                                            localDB.db.commit()
                                        except Exception as e:
                                            mensaje = "No se pudo insertar el registro de inventario"
                                            print(chalk.red(mensaje))
                                            print(chalk.red(e))
                                            localDB.db.rollback()
                                localDB.cursor.execute(sql_insert_trd_det,det)                            
                            localDB.db.commit()
                            mensaje = "Insert realizado desde Replica Python"
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                            redireccionar_a_sucursal(localDB, audit, trd['sucursal_id'])
                        except Exception as e:
                            mensaje = "No se pudo insertar el registro"
                            print(chalk.red(mensaje))
                            print(chalk.red(e))
                            localDB.db.rollback()
                            actualizar_audit(audit['id'], mensaje, remoteDB)

                    if audit['event_name'] == 'UPDATE':
                        try:
                            if trd['cfdi_id']:
                                remoteDB.cursor.execute(sql_cfdi,{'id': trd['cfdi_id']})
                                cfdi = remoteDB.cursor.fetchone()
                                if cfdi:
                                    localDB.cursor.execute(sql_cfdi,{'id': trd['cfdi_id']})
                                    cfdi_cen = localDB.cursor.fetchone()
                                    if not cfdi_cen:
                                        try:
                                            localDB.cursor.execute(sql_insert_cfdi,cfdi)                         
                                            localDB.db.commit()
                                        except Exception as e:
                                            mensaje = "No se pudo insertar el registro de cfdi"
                                            print(chalk.red(mensaje))
                                            print(chalk.red(e))
                                            localDB.db.rollback()
                            localDB.cursor.execute(sql_update_trd,trd)
                            remoteDB.cursor.execute(sql_trd_det,{'id': audit['persisted_object_id']})
                            dets = remoteDB.cursor.fetchall()
                            for det in dets:

                                if det['inventario_id']:
                                    remoteDB.cursor.execute(sql_inventario,{'id': det['inventario_id']})
                                    inventario = remoteDB.cursor.fetchone()
                                    if inventario:
                                        localDB.cursor.execute(sql_inventario,{'id': det['inventario_id']})
                                        inventario_cen = localDB.cursor.fetchone()
                                        if not inventario_cen:
                                            try:
                                                localDB.cursor.execute(sql_insert_inventario,inventario)                         
                                                localDB.db.commit()
                                            except Exception as e:
                                                mensaje = "No se pudo insertar el registro de inventario"
                                                print(chalk.red(mensaje))
                                                print(chalk.red(e))
                                                localDB.db.rollback()
                                localDB.cursor.execute(sql_update_trd_det,det)   
                            localDB.db.commit()
                            mensaje = "Update realizado desde Replica Python"
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                            redireccionar_a_sucursal(localDB, audit, trd['sucursal_id'])
                        except Exception as e:
                            mensaje = "No se pudo actualizar el registro"
                            print(chalk.red(mensaje))
                            print(chalk.red(e))
                            localDB.db.rollback()
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                            
                    if audit['event_name'] == 'DELETE':

                        sql_delete_trd = "delete from traslado where id = %(id)s"
                        sql_delete_trd_det = "delete from traslado_det where id = %(id)s"
                        try: 
                            remoteDB.cursor.execute(sql_trd_det,{'id': audit['persisted_object_id']})
                            dets = remoteDB.cursor.fetchall()
                            for det in dets:
                                localDB.cursor.execute(sql_delete_trd_det,{"id":det['id']}) 
                            localDB.cursor.execute(sql_delete_trd,{"id": audit['persisted_object_id']}) 
                            localDB.db.commit()
                            mensaje = "Delete realizado desde Replica Python"
                            actualizar_audit(audit['id'], mensaje, remoteDB)
                            redireccionar_a_sucursal(localDB, audit, trd['sucursal_id'])
                        except Exception as e:
                            mensaje = "No se pudo eliminar el registro"
                            print(chalk.red(mensaje))
                            print(chalk.red(e))
                            localDB.db.rollback()
                            actualizar_audit(audit['id'], mensaje, remoteDB)



def actualizar_audit(audit_id, mensaje, remoteDB):
    sql_update = "update audit_log set message = %(mensaje)s , date_replicated = %(replicado)s where id = %(id)s"
    try:
        remoteDB.cursor.execute(sql_update,{"mensaje": mensaje,"replicado": datetime.now(), "id": audit_id})
        remoteDB.database.commit()
    except Exception as e:
        print(chalk.red("No se pudo insertar el registro de audit_log"))
        print(chalk.red(e))
        remoteDB.rollback() 

def redireccionar_a_sucursal(localDB, audit_origen, sucursal_id):
        localDB.cursor.execute("select * from sucursal where id = %(sucursal_id)s",{"sucursal_id": sucursal_id}) 
        sucursal = localDB.cursor.fetchone()
        crear_audit_log(localDB, sucursal['nombre'],audit_origen)


def crear_audit_log(localDB,sucursal_name, audit_origen):
    audit = {
            'version': audit_origen['version'],
            'persisted_object_id': audit_origen['persisted_object_id'],
            'target':sucursal_name,
            'date_created': datetime.now(),
            'last_updated': datetime.now(),
            'name':audit_origen['name'],
            'event_name': audit_origen['event_name'],
            'table_name': audit_origen['table_name'],
            'source': 'CENTRAL',
        }
    sql_insert_audit = """insert into audit_log (version,persisted_object_id,target,date_created,last_updated,name,event_name,table_name,source)
                values (%(version)s, %(persisted_object_id)s,%(target)s,%(date_created)s,%(last_updated)s,%(name)s,%(event_name)s,%(table_name)s,%(source)s)"""
    try:
        localDB.cursor.execute(sql_insert_audit,audit) 
        localDB.db.commit()
    except Exception as e:
        print(chalk.red("No se pudo insertar el registro de audit_log"))
        print(chalk.red(e))
        localDB.rollback()
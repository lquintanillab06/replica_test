from datetime import datetime
from simple_chalk import chalk
from .importador import Importador

from src import get_conexion_remota, resolver_insert_query, resolver_update_query,get_sucursales_replica



class ImportadorAudit(Importador):

    def __init__(self, localDB, table_name=None, table_name_detalle=None, dispersar=False, redireccionar = False):
        self.table_name = table_name
        self.table_name_detalle = table_name_detalle
        self.dispersar = dispersar
        self.redireccionar = redireccionar

        self.sql_row = f"select * from {self.table_name} where id = %(id)s"   
        self.sql_insert =resolver_insert_query(localDB,self.table_name,localDB.database)
        self.sql_update =resolver_update_query(localDB,self.table_name,localDB.database)
        self.sql_delete = f"delete from {self.table_name} where id = %(id)s"
        
        if  self.table_name_detalle:
            self.sql_insert_det =resolver_insert_query(localDB,self.table_name_detalle,localDB.database)
            self.sql_update_det =resolver_update_query(localDB,self.table_name_detalle,localDB.database)
            self.sql_delete_det = f"delete from {self.table_name_detalle} where id = %(id)s"
            self.sql_row_det = f"select * from {self.table_name_detalle} where {self.table_name}_id = %(id)s"
        super().__init__(localDB)

    def importar(self):
            print(chalk.magenta(f"Importando  {self.table_name.upper()}"))
            if self.table_name_detalle:
                self.importar_maestro_detalle()
            else:
                self.importar_operacion()

    def obtener_audits(self,remoteDB):
        sql = f"select * from audit_log where date_replicated is null and table_name like %(table_name)s order by date_created"
        remoteDB.cursor.execute(sql,{'table_name': self.table_name})
        audits = remoteDB.cursor.fetchall()
        return audits


    def importar_maestro_detalle(self):

        print(chalk.cyan(f"Importando {self.table_name}"))
        for sucursal in self.sucursales:
            remoteDB = get_conexion_remota(sucursal)
            if remoteDB:
                audits = self.obtener_audits(remoteDB)
                for audit in audits:
                    remoteDB.cursor.execute(self.sql_row,{'id': audit['persisted_object_id']})
                    operacion = remoteDB.cursor.fetchone()

                    if audit['event_name'] == 'DELETE' or operacion:

                        if audit['event_name'] == 'INSERT':
                            try:
                                self.localDB.cursor.execute(self.sql_insert,operacion)   
                                remoteDB.cursor.execute(self.sql_row_det,{'id': audit['persisted_object_id']})
                                dets = remoteDB.cursor.fetchall()
                                for det in dets:
                                    self.localDB.cursor.execute(self.sql_insert_det,det)                       
                                self.localDB.db.commit()

                                if self.dispersar:
                                    self.dispersar_sucursales(audit, sucursal)
                                if self.redireccionar:
                                    self.redireccionar_a_sucursal(audit,operacion['sucursal_id'])
                                mensaje = "Insert realizado desde Replica Python"
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)

                            except Exception as e:
                                mensaje = "No se pudo insertar el registro"
                                print(chalk.red(mensaje))
                                print(chalk.red(e))
                                self.localDB.db.rollback()
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)
    
                        if audit['event_name'] == 'UPDATE':
                            try:
                                self.localDB.cursor.execute(self.sql_update,operacion)
                                remoteDB.cursor.execute(self.sql_row_det,{'id': audit['persisted_object_id']})
                                dets = remoteDB.cursor.fetchall()
                                for det in dets:
                                    self.localDB.cursor.execute(self.sql_update_det,det) 
                                self.localDB.db.commit()

                                if self.dispersar:
                                    self.dispersar_sucursales(audit, sucursal)
                                if self.redireccionar:
                                    self.redireccionar_a_sucursal(audit,operacion['sucursal_id'])

                                mensaje = "Update realizado desde Replica Python"
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)

                            except Exception as e:
                                mensaje = "No se pudo actualizar el registro"
                                print(chalk.red(mensaje))
                                print(chalk.red(e))
                                self.localDB.db.rollback()
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)

                        if audit['event_name'] == 'DELETE':
                            try: 
                                remoteDB.cursor.execute(self.sql_row_det,{'id': audit['persisted_object_id']})
                                dets = remoteDB.cursor.fetchall()
                                for det in dets:
                                    self.localDB.cursor.execute(self.sql_delete_det,{"id":det['id']}) 
                                self.localDB.cursor.execute(self.sql_delete,{"id": audit['persisted_object_id']}) 
                                self.localDB.db.commit()

                                if self.dispersar:
                                    self.dispersar_sucursales(audit, sucursal)
                                if self.redireccionar:
                                    self.redireccionar_a_sucursal(audit,operacion['sucursal_id'])
                                mensaje = "Delete realizado desde Replica Python"
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)
                            except Exception as e:
                                mensaje = "No se pudo eliminar el registro"
                                print(chalk.red(mensaje))
                                print(chalk.red(e))
                                self.localDB.db.rollback()
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)
           

    def importar_operacion(self):
        for sucursal in self.sucursales:
            remoteDB = get_conexion_remota(sucursal)
            if remoteDB:
                audits = self.obtener_audits(remoteDB)
                for audit in audits:
                    remoteDB.cursor.execute(self.sql_row,{'id': audit['persisted_object_id']})
                    operacion = remoteDB.cursor.fetchone()
                    if audit['event_name'] == 'DELETE' or operacion:
                        if audit['event_name'] == 'INSERT':
                            try:
                                self.localDB.cursor.execute(self.sql_insert,operacion)                         
                                self.localDB.db.commit()

                                if self.dispersar:
                                    self.dispersar_sucursales(audit, sucursal)
                                if self.redireccionar:
                                    self.redireccionar_a_sucursal(audit,operacion['sucursal_id'])
                                mensaje = "Insert realizado desde Replica Python"
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)
                            except Exception as e:
                                mensaje = "No se pudo insertar el registro"
                                print(chalk.red(mensaje))
                                print(chalk.red(e))
                                self.localDB.db.rollback()
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)
                            
                        if audit['event_name'] == 'UPDATE':
                            try:
                                self.localDB.cursor.execute(self.sql_update,operacion)
                                self.localDB.db.commit()
                                if self.dispersar:
                                    self.dispersar_sucursales(audit, sucursal)
                                if self.redireccionar:
                                    self.redireccionar_a_sucursal(audit,operacion['sucursal_id'])
                                mensaje = "Update realizado desde Replica Python"
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)
                            except Exception as e:
                                mensaje = "No se pudo actualizar el registro"
                                print(chalk.red(mensaje))
                                print(chalk.red(e))
                                self.localDB.db.rollback()
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)

                        if audit['event_name'] == 'DELETE':
                            try: 
                                self.localDB.cursor.execute(self.sql_delete,{"id": audit['persisted_object_id']}) 
                                self.localDB.db.commit()
                                if self.dispersar:
                                    self.dispersar_sucursales(audit, sucursal)
                                if self.redireccionar:
                                    self.redireccionar_a_sucursal(audit,operacion['sucursal_id'])
                                mensaje = "Delete realizado desde Replica Python"
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)
                            except Exception as e:
                                mensaje = "No se pudo eliminar el registro"
                                print(chalk.red(mensaje))
                                print(chalk.red(e))
                                self.localDB.db.rollback()
                                self.actualizar_audit(audit['id'], mensaje, remoteDB)


    def dispersar_sucursales(self, audit_origen, sucursal):
        sucursales = list(filter( lambda x: (not x['id'] == sucursal['id'])  ,get_sucursales_replica()))
        for suc in sucursales:
            self.crear_audit_log(suc['server'],audit_origen)

    def redireccionar_a_sucursal(self, audit_origen, sucursal_id):
        self.localDB.cursor.execute("select * from sucursal where id = %(sucursal_id)s",{"sucursal_id": sucursal_id}) 
        sucursal = self.localDB.cursor.fetchone()
        self.crear_audit_log(sucursal['nombre'],audit_origen)

    def actualizar_audit(self, audit_id, mensaje, remoteDB):
        sql_update = "update audit_log set message = %(mensaje)s , date_replicated = %(replicado)s where id = %(id)s"
        try:
            remoteDB.cursor.execute(sql_update,{"mensaje": mensaje,"replicado": datetime.now(), "id": audit_id})
            remoteDB.database.commit()
        except Exception as e:
            print(chalk.red("No se pudo insertar el registro de audit_log"))
            print(chalk.red(e))
            remoteDB.database.rollback()
  
    def crear_audit_log(self,sucursal_name, audit_origen):
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
            self.localDB.cursor.execute(sql_insert_audit,audit) 
            self.localDB.db.commit()
        except Exception as e:
            print(chalk.red("No se pudo insertar el registro de audit_log"))
            print(chalk.red(e))
            self.localDB.db.rollback()


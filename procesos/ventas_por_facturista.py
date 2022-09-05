from datetime import datetime, date
from simple_chalk import chalk

from src import get_sucursales_activas, get_conexion_remota, get_mes_anterior,get_dias_mes,get_sucursal_replica ,obtener_sucursal,resolver_insert_query_sin_id,get_dias_mes_hoy
from database import LocalDataBaseConnection


class VentaPorFacturista:
    
    def __init__(self):
        self.localDB = LocalDataBaseConnection()
        self.sql_insert = resolver_insert_query_sin_id(self.localDB,'venta_por_facturista',self.localDB.database)
        self.sql = """
                  SELECT 0 as version, fecha,sucursal_id,sucursalNom as sucursal_nom, create_user ,facturista
        ,sum(con) con,sum(cod) cod,sum(cre) as cre,sum(canc) canc,sum(devs) devs,sum(facs) facs,sum(importe) importe,sum(partidas) partidas,sum(ped_fact) ped_fact
        ,sum(ped) ped,sum(pedMosCON) ped_moscon,sum(pedMosCOD) ped_moscod,sum(pedMosCRE) ped_moscre,sum(pedTelCON) ped_telcon,sum(pedTelCOD) ped_telcod,sum(pedTelCRE) ped_telcre
        FROM (
        SELECT 'PED' as tipo,fecha,sucursal_id,(SELECT s.nombre FROM sucursal s WHERE v.sucursal_id=s.id) as sucursalNom,create_user
        ,ifnull((SELECT u.nombre FROM user u where v.create_user=u.username),create_user) as facturista
        ,0 con,0 cod,0 cre,0 canc,0 devs,0 facs,0 importe,0 partidas
        ,SUM(case when v.facturar is not null and v.facturar_usuario <> v.update_user then 1 else 0 end) as ped_fact
        ,count(*) as ped
        ,SUM(case when v.tipo = 'CON' and v.atencion not like 'TEL%' then 1 else 0 end) as pedMosCON
        ,SUM(case when v.tipo = 'CON' and v.cod is true and v.atencion not like 'TEL%' then 1 else 0 end) as pedMosCOD
        ,SUM(case when v.tipo = 'CRE' and v.atencion not like 'TEL%' then 1 else 0 end) as pedMosCRE
        ,SUM(case when v.tipo = 'CON' and v.atencion like 'TEL%' then 1 else 0 end) as pedTelCON
        ,SUM(case when v.tipo = 'CON' and v.cod is true and v.atencion like 'TEL%' then 1 else 0 end) as pedTelCOD
        ,SUM(case when v.tipo = 'CRE' and v.atencion like 'TEL%' then 1 else 0 end) as pedTelCRE
         FROM VENTA V WHERE FECHA= %(fecha)s and sucursal_id not in ('402880fc5e4ec411015e4ec652710139')
         GROUP BY fecha,sucursal_id,create_user
        union
        SELECT 'FAC' as tipo,fecha,sucursal_id,(SELECT s.nombre FROM sucursal s WHERE v.sucursal_id=s.id) as sucursalNom,update_user
          ,ifnull((SELECT u.nombre FROM user u where v.create_user=u.username),create_user) as facturista
        ,SUM(case when v.tipo = 'CON' then 1 else 0 end) as con
        ,SUM(case when v.tipo = 'COD' then 1 else 0 end) as cod
        ,SUM(case when v.tipo = 'CRE' then 1 else 0 end) as cre
        ,SUM(case when v.cancelada is not null then 1 else 0 end) as canc
        ,(SELECT count(*) FROM devolucion_de_venta d join venta x on(d.venta_id=x.id) where d.parcial is false and x.cuenta_por_cobrar_id is not null and x.update_user = v.update_user and date(d.fecha)= %(fecha)s) as devs
        ,count(*) as facs,sum(subtotal * tipo_de_cambio) as importe
        ,(SELECT count(*) FROM venta_det d join venta x on(d.venta_id=x.id) join cuenta_por_cobrar z on(x.cuenta_por_cobrar_id=z.id) where z.cfdi_id is not null and z.cancelada is null and x.update_user = v.update_user and date(z.fecha)= %(fecha)s) as partidas
        ,0 as ped_fact,0 ped,0 pedMosCON,0 pedMosCOD,0 pedMosCRE,0 pedTelCON,0 pedTelCOD,0 pedTelCRE
        FROM cuenta_por_cobrar V WHERE cfdi_id is not null and sw2 is null and FECHA= %(fecha)s and sucursal_id not in ('402880fc5e4ec411015e4ec652710139')
        GROUP BY fecha,sucursal_id,create_user
        ) AS A
        GROUP BY
        fecha,sucursal_id,facturista
        """


    def actualizar(self):
        dia = date.today()
        self.actualizar_dia(dia)


    def actualizar_dia(self, fecha):
        sucursales = get_sucursales_activas()
        print(chalk.cyan(f"Iniciando la Integracion de Ventas Por Facturista {fecha}......."))
        for sucursal in sucursales:
        #sucursal = get_sucursal_replica('TACUBA')
            self.borrar_registros(fecha,sucursal)
            remoteDB = get_conexion_remota(sucursal)
            if remoteDB :
                remoteDB.cursor.execute(self.sql,{"fecha":fecha})
                ventas = remoteDB.cursor.fetchall()
                for venta in ventas:
                    try:
                        self.localDB.cursor.execute(self.sql_insert,venta)
                        self.localDB.db.commit()
                    except Exception as e:
                        mensaje = "Hubo un Error"
                        print(chalk.red(mensaje))
                        print(chalk.red(e))
                        self.localDB.db.rollback()
                

    def actualizar_mtd(self):
        dias = get_dias_mes_hoy()
        for dia in dias:
            self.actualizar_dia(dia)


    def actualizar_mes_anterior(self):
        mes_anterior = get_mes_anterior()
        dias = get_dias_mes(mes_anterior[0], mes_anterior[1])
        for dia in dias:
            self.actualizar_dia(dia)

    def borrar_registros(self,fecha,sucursal):
        print(chalk.yellow(f"Borrando Registros del  {fecha}.......")) 
        sucursal_del = obtener_sucursal(sucursal['server'])
        try:
            self.localDB.cursor.execute("delete from venta_por_facturista where fecha = %(fecha)s and sucursal_id = %(sucursal_id)s",{"fecha": fecha, "sucursal_id": sucursal_del['id']})
            self.localDB.db.commit()
        except Exception as e:
            mensaje = "Hubo un Error"
            print(chalk.red(mensaje))
            print(chalk.red(e))
            self.localDB.db.rollback()



        
    



    
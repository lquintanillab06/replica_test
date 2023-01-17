from apscheduler.schedulers.background import BackgroundScheduler
from simple_chalk import chalk

from validacion import validar_productos, validar_catalogo,validar_cfdis


def start_validacion():
    print("Arrancando el validador job ...")
    scheduler = BackgroundScheduler()
    #scheduler.add_job(validar_catalogo, 'interval',['proveedor',['id','clave','nombre']], seconds=600, id='valid_proveedor_producto_id')
    #scheduler.add_job(validar_catalogo, 'interval',['proveedor_producto',['id','clave_proveedor','codigo_proveedor','descripcion_proveedor']], seconds=600, id='valid_proveedor_id')
    #scheduler.add_job(validar_productos, 'interval', seconds=120, id='valid_productos_id')
    #scheduler.add_job(validar_catalogo, 'interval',['cliente',['id','permite_cheque']], seconds=180, id='valid_cliente_id')
    #scheduler.add_job(validar_catalogo, 'interval',['cliente_credito',['id','credito_activo','descuento_fijo','atraso_maximo','linea_de_credito','postfechado','saldo']], seconds=120, id='valid_catalogo_id')
    scheduler.add_job(validar_cfdis, 'interval', seconds=30, id='valid_cancelacion_cfdi_id')
    scheduler.start() 
from apscheduler.schedulers.background import BackgroundScheduler
from importacion import importar_vales, importar_clientes, importar_traslados, importar_existencia




def start_importacion():
    print("Arrancando el importador job ...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(importar_vales, 'interval', seconds=120, id='import_vales_id')
    scheduler.add_job(importar_traslados, 'interval', seconds=180, id='import_traslados_id')
    scheduler.add_job(importar_existencia, 'interval', seconds=60, id='import_existencias_id')
    scheduler.add_job(importar_clientes, 'interval', seconds=60, id='import_clientes_id')
    scheduler.start() 
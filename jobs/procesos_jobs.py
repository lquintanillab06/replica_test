from apscheduler.schedulers.background import BackgroundScheduler
from procesos import VentaPorFacturista




def start_procesos():
    print("Arrancando los procesos job ...")
    scheduler = BackgroundScheduler()
    #scheduler.add_job(venta_por_facturista, 'interval', seconds=30, id='venta_facturista_id')
    scheduler.start() 
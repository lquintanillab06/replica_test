from apscheduler.schedulers.background import BackgroundScheduler
from simple_chalk import chalk

from exportacion import exportar_vales



def start_exportacion():
    print("Arrancando el exportador job ...")
    scheduler = BackgroundScheduler()
    #scheduler.add_job(exportar_vales, 'interval', seconds=30, id='export_vales_id')
    scheduler.start() 
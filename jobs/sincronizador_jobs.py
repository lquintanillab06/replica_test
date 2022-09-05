from apscheduler.schedulers.background import BackgroundScheduler
from simple_chalk import chalk

from validacion import sincronizar_existencia



def start_sincronizacion():
    print("Arrancando el sincronizador job ...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(sincronizar_existencia, 'interval', seconds=120, id='sinc_existencias_id')
    scheduler.start() 
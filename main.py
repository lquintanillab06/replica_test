from jobs import start_importacion, start_exportacion, start_validacion, start_sincronizacion, start_procesos
from src import Periodo
from src import  get_dias_periodo,get_dias_fecha_periodo, get_mes_anterior, get_dias_mes
from procesos import VentaPorFacturista


from datetime import date, timedelta
from calendar import Calendar
def main():
    #start_importacion()
    #start_exportacion()
    start_validacion()
    #start_sincronizacion()         
    #start_procesos()

   # srv = VentaPorFacturista()
   # srv.actualizar_mes_anterior()
   

  
if __name__== '__main__':
    # execute only if run as the entry point into the program
    main()  
    run = True 
    while run :
        sent = input("S para salir:  \n") 
        if(sent.upper() =='S'):
            run = False  

  


  
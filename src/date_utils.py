from datetime import datetime, date, timedelta
import calendar

from .periodo import Periodo

def get_dias_periodo(periodo):
    d1 = datetime.strptime(str(periodo.fecha_ini), "%Y-%m-%d")
    d2 = datetime.strptime(str(periodo.fecha_fin), "%Y-%m-%d")
    return abs((d2 - d1).days)

def get_periodo_mes(mes, anio):
    fecha_ini = date(anio,mes,1)
    dia_fin = calendar.monthrange(anio, mes)[1]
    fecha_fin = date(anio,mes, dia_fin)
    return Periodo(fecha_ini,fecha_fin)

def get_dias_mes(mes, anio):
    dia_ini = 1
    dia_fin = calendar.monthrange(anio, mes)[1]
    dias = [date(anio, mes,dia) for dia in range(dia_ini,dia_fin +1)]
    return dias

def get_periodo_mes_actual_hoy():
    fecha_fin = date.today()
    fecha_ini = date(fecha_fin.year,fecha_fin.month, 1)
    return Periodo(fecha_ini,fecha_fin)

def get_dias_mes_hoy():
    dia_ini = 1
    dia_fin = date.today().day
    dias = [date(date.today().year, date.today().month,dia) for dia in range(dia_ini,dia_fin +1)]
    return dias

def get_dias_fecha_periodo(periodo):
    dias_periodo = get_dias_periodo(periodo)
    dias = [periodo.fecha_ini + timedelta(days = dia) for dia in range(dias_periodo+1) ]
    return dias

def get_mes_anterior():
    hoy = date.today()
    mes = hoy.month-1
    anio = hoy.year
    if mes == 0:
        mes=12
        anio = anio - 1
    return (mes, anio)


    

        


        
        


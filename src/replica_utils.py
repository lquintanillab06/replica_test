from cmath import pi
import platform    # For getting the operating system name
import subprocess  # For executing a shell command
from datetime import datetime
from database import LocalDataBaseConnection, RemoteDataBaseConnection
from simple_chalk import chalk


def get_sucursales_activas():
    sql = "select * from data_source_replica where activa is true and central is false and sucursal is true"
    localDB = LocalDataBaseConnection()
    localDB.cursor.execute(sql)
    return localDB.cursor.fetchall()

def get_sucursales_replica():
    sql = "select * from data_source_replica where central is false"
    localDB = LocalDataBaseConnection()
    localDB.cursor.execute(sql)
    return localDB.cursor.fetchall()

def get_sucursal_replica(sucursal):
    sql = "select * from data_source_replica where server = %(sucursal)s and activa is true and central is false and sucursal is true"
    localDB = LocalDataBaseConnection()
    localDB.cursor.execute(sql,{'sucursal': sucursal})
    return localDB.cursor.fetchone()
    

def get_conexion_remota(data_source_replica):
    print(chalk.cyan(f"Conectando con {data_source_replica['server']}... "))
    toc = ping(data_source_replica['ip'])
    try:
        if toc: 
            remoteDB = RemoteDataBaseConnection(data_source_replica)
            return remoteDB
        else:
            print(chalk.red(f"No hay conexion con {data_source_replica['ip']}"))
    except Exception as e:
        print(f"Error al conectarse al la BD de {data_source_replica['server']}   /n {e} ")


def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """
    # Option for the number of packets as a function of
    param = '-n' if platform.system().lower()=='windows' else '-c'
    # Building the command. Ex: "ping -c 1 google.com"
    command = ['ping', param, '1', host]
    return subprocess.call(command) == 0

def obtener_diferencias(lista_a, lista_b):
    diferencias = [i for i in lista_a if i not in lista_b]
    return diferencias

def obtener_sucursal(sucursal_name):
    sql = "select * from sucursal where  nombre = %(sucursal_name)s"
    localDB = LocalDataBaseConnection()
    localDB.cursor.execute(sql,{"sucursal_name": sucursal_name})
    return localDB.cursor.fetchone()


 





    

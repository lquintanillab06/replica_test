import mysql.connector
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker




class LocalDataBaseConnection(object):

    ''' def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(LocalDataBaseConnection, cls).__new__(cls)
        return cls.instance '''

   
    def __init__(self):
        with open("conf/config.json") as f:
            configuracion = json.loads(f.read())
            datasource = configuracion["datasource"]

        self.db = mysql.connector.connect(
                host = datasource['host'],
                user = datasource['user'],
                passwd = datasource['passwd'] ,
                database = datasource['database'],
                charset='utf8'
            )
        self.host = datasource['host']
        self.database = datasource['database']
        self.cursor = self.db.cursor(dictionary=True, buffered=True)

class RemoteDataBaseConnection():
    def __init__(self,data_source_replica):
        self.database = mysql.connector.connect(
                host = data_source_replica['ip'],
                user = data_source_replica['username'],
                passwd = data_source_replica['password'] ,
                database = data_source_replica['data_base'],
                 charset='utf8'
            )
        self.cursor = self.database.cursor(dictionary=True, buffered=True)
    

class RemoteDataBaseConnectionOLD():
    def __init__(self):
        with open("conf/config.json") as f:
            configuracion = json.loads(f.read())
            datasource = configuracion["datasource_remote"]
        self.database = mysql.connector.connect(
                host = datasource['host'],
                user = datasource['user'],
                passwd = datasource['passwd'] ,
                database = datasource['database'],
                 charset='utf8'
            )
        self.cursor = self.database.cursor(dictionary=True, buffered=True)

class RemoteDataBaseConnectionConfig():
    def __init__(self, configuracion):
            datasource = configuracion
            self.database = mysql.connector.connect(
                host = datasource['host'],
                user = datasource['user'],
                passwd = datasource['passwd'] ,
                database = datasource['database'],
                 charset='utf8'
            )
            self.cursor = self.database.cursor(dictionary=True, buffered=True)


class DataBaseConnection():
    """
        Clase para crear conexiones a las bases de datos.
    """

    @staticmethod
    def getConnectionSqlAlchemy(datasource):
        """
            Metodo para crear una conexion sql usando un datasource de config.json  y regresa un engine de sql Alchemy
        """
        with open("conf/config.json") as f: 
            configuracion = json.loads(f.read())
            datasource = configuracion[datasource]

        engine = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s'
                    %(datasource['user'], datasource['passwd'],datasource['host'], 3306, datasource['database']), echo=False)
        
        Session = sessionmaker(bind=engine)
        session = Session()

        return session




 

            


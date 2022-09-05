from abc import ABC, abstractmethod
from src import get_sucursales_activas



class Importador(ABC):
    def __init__(self,localDB ):
        self.localDB = localDB
        self.sucursales = get_sucursales_activas()

    @abstractmethod
    def importar(self):
        pass

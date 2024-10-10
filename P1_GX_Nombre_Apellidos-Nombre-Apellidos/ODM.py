__author__ = 'Pablo Ramos Criado'
__students__ = 'Pedro Roig && Jorge Valero'


from geopy.geocoders import Nominatim # type: ignore
from geopy.exc import GeocoderTimedOut
import time
from typing import Generator, Any, Self
from geojson import Point
import pymongo
import yaml
from pymongo.collection import Collection
from pymongo import command_cursor
import json

def getLocationPoint(address: str) -> Point:
    """ 
    Obtiene las coordenadas de una dirección en formato geojson.Point
    Utilizar la API de geopy para obtener las coordenadas de la direccion
    Cuidado, la API es publica tiene limite de peticiones, utilizar sleeps.

    Parameters
    ----------
        address : str
            direccion completa de la que obtener las coordenadas
    Returns
    -------
        geojson.Point
            coordenadas del punto de la direccion
    """
    geolocator = Nominatim(user_agent=f"ODM_ApiPyMongo")
    location = None

    while location is None:
        try:
            time.sleep(1)
            location = geolocator.geocode(address)
        except GeocoderTimedOut:
            print(f"Error: geocode failed for address {address}. Retrying...")
            continue

    return Point((location.longitude, location.latitude)) if location else None

def errorFunction(msg: str):
    print(f"Error: {msg}")
class Model:
    """ 
    Clase de modelo abstracta
    Crear tantas clases que hereden de esta clase como  
    colecciones/modelos se deseen tener en la base de datos.

    Attributes
    ----------
        required_vars : set[str]
            conjunto de variables requeridas por el modelo
        admissible_vars : set[str]
            conjunto de variables admitidas por el modelo
        db : pymongo.collection.Collection
            conexion a la coleccion de la base de datos
    
    Methods
    -------
        __setattr__(name: str, value: str | dict) -> None
            Sobreescribe el metodo de asignacion de valores a las
            variables del objeto con el fin de controlar las variables
            que se asignan al modelo y cuando son modificadas.
        save()  -> None
            Guarda el modelo en la base de datos
        delete() -> None
            Elimina el modelo de la base de datos
        find(filter: dict[str, str | dict]) -> ModelCursor
            Realiza una consulta de lectura en la BBDD.
            Devuelve un cursor de modelos ModelCursor
        No hacer para esta practica -> aggregate(pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor
            Devuelve el resultado de una consulta aggregate.
        No hacer para esta practica -> find_by_id(id: str) -> dict | None
            Busca un documento por su id utilizando la cache y lo devuelve.
            Si no se encuentra el documento, devuelve None.
        init_class(db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None
            Inicializa las variables de clase en la inicializacion del sistema.

    """
    required_vars: set[str]
    admissible_vars: set[str]
    indexes: set[str]
    db: pymongo.collection.Collection

    """Cantidad de argumentos indefinidos: diccionario de string, string | string, diccionario | string, lista"""
    def __init__(self, **kwargs: dict[str, str | dict | list]):
        """
        Inicializa el modelo con los valores proporcionados en kwargs
        Comprueba que los valores proporcionados en kwargs son admitidos
        por el modelo y que las variables requeridas son proporcionadas.

        Parameters
        ----------
            kwargs : dict[str, str | dict]
                diccionario con los valores de las variables del modelo
        """
        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        
        # Asigna todos los valores en kwargs a las variables con 
        # nombre las claves en kwargs
        missing_vars = self.required_vars - kwargs.keys()
        
        if missing_vars:
            errorFunction(f"falta información necesaria [{missing_vars}]")
            return
        
        not_admissible_vars = kwargs.keys() - (self.required_vars | self.admissible_vars | self.indexes)
        
        if not_admissible_vars:
            errorFunction(f"no se admiten estas entradas [{not_admissible_vars}]")
        
        if self.indexes:
            while self.indexes:
                self.db.create_index(self.indexes.pop(), unique=True)
        self.__dict__.update(kwargs) #para actualizar todos los valores de golpe

    def __setattr__(self, name: str, value: str | dict) -> None:
        """ Sobreescribe el metodo de asignacion de valores a las 
        variables del objeto con el fin de controlar las variables
        que se asignan al modelo y cuando son modificadas.
        """
        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        
        if name not in self.admissible_vars and name not in self.required_vars:
            errorFunction(f"[{name}] no está admitida")
            return
        if 'flags' not in self.__dict__:
            self.__dict__['flags'] = []
        self.flags.append(name)
        # Asigna el valor value a la variable name
        self.__dict__[name] = value
        
    def save(self) -> None:
        """
        Guarda el modelo en la base de datos
        Si el modelo no existe en la base de datos, se crea un nuevo
        documento con los valores del modelo. En caso contrario, se
        actualiza el documento existente con los nuevos valores del
        modelo.
        """
        #TODO
        ## insert_one si no tiene _id
        if 'direccion' in self.__dict__:
            point = getLocationPoint(self.__dict__['direccion'])
            if point:
                self.__dict__['location'] = {
                    'type': 'Point',
                    'coordinates': list(point['coordinates'])
                }
        
        if '_id' not in self.__dict__:
            self.db.insert_one(self.__dict__)
        else:
            if 'flags' in self.__dict__:
                newValues = {}
                for key in self.flags:
                    newValues[key] = self.__dict__[key]
            update_msg = self.db.update_one({'_id': self._id}, { "$set": newValues})
            print(f"Acknowledged: {update_msg.acknowledged}")
    def delete(self) -> None:
        """
        Elimina el modelo de la base de datos
        """
        #TODO
        self.db.delete_one({"_id" : self._id})
    
    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        """ 
        Utiliza el metodo find de pymongo para realizar una consulta
        de lectura en la BBDD.
        find debe devolver un cursor de modelos ModelCursor

        Parameters
        ----------
            filter : dict[str, str | dict]
                diccionario con el criterio de busqueda de la consulta
        Returns
        -------
            ModelCursor
                cursor de modelos
        """ 
        #TODO
        return ModelCursor(cls, cls.db.find(filter))

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor:
        """ 
        Devuelve el resultado de una consulta aggregate. 
        No hay nada que hacer en esta funcion.
        Se utilizara para las consultas solicitadas
        en el segundo proyecto de la practica.

        Parameters
        ----------
            pipeline : list[dict]
                lista de etapas de la consulta aggregate 
        Returns
        -------
            pymongo.command_cursor.CommandCursor
                cursor de pymongo con el resultado de la consulta
        """ 
        return cls.db.aggregate(pipeline)
    
    @classmethod
    def find_by_id(cls, id: str) -> Self | None:
        """ 
        NO IMPLEMENTAR HASTA EL TERCER PROYECTO
        Busca un documento por su id utilizando la cache y lo devuelve.
        Si no se encuentra el documento, devuelve None.

        Parameters
        ----------
            id : str
                id del documento a buscar
        Returns
        -------
            Self | None
                Modelo del documento encontrado o None si no se encuentra
        """ 
        #TODO
        pass

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str], indexes: set[str]) -> None:
        """ 
        Inicializa las variables de clase en la inicializacion del sistema.
        En principio nada que hacer aqui salvo que se quieran realizar
        alguna otra inicialización/comprobaciones o cambios adicionales.

        Parameters
        ----------
            db_collection : pymongo.collection.Collection
                Conexion a la collecion de la base de datos.
            required_vars : set[str]
                Set de variables requeridas por el modelo
            admissible_vars : set[str] 
                Set de variables admitidas por el modelo
        """
        cls.db = db_collection
        cls.required_vars = required_vars
        cls.admissible_vars = admissible_vars
        cls.indexes = indexes

class ModelCursor:
    """ 
    Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.

    Attributes
    ----------
        model_class : Model
            Clase para crear los modelos de los documentos que se iteran.
        cursor : pymongo.cursor.Cursor
            Cursor de pymongo a iterar

    Methods
    -------
        __iter__() -> Generator
            Devuelve un iterador que recorre los elementos del cursor
            y devuelve los documentos en forma de objetos modelo.
    """

    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):
        self.model = model_class
        self.cursor = cursor
    
    def __iter__(self) -> Generator:
        """
        Devuelve un iterador que recorre los elementos del cursor
        y devuelve los documentos en forma de objetos modelo.
        Utilizar yield para generar el iterador
        Utilizar la funcion next para obtener el siguiente documento del cursor
        Utilizar alive para comprobar si existen mas documentos.
        """
        #TODO
        while self.cursor.alive:  # Comprueba si el cursor sigue teniendo elementos
            try:
                # Obtiene el siguiente documento del cursor y lo convierte a un objeto modelo
                document = next(self.cursor)
                # Convierte el documento en un objeto de modelo y lo devuelve
                yield self.model(**document)
            except StopIteration:
                # Si no hay más documentos, sale del bucle
                break
            


def initApp(definitions_path: str = "models.yml", mongodb_uri="mongodb://localhost:27017/", db_name="ProyectBasesDeDatos") -> None:
    #TODO
    # Inicializar base de datos
    cliente = pymongo.MongoClient(mongodb_uri)
    db = cliente[db_name]
    
    with open(definitions_path, 'r') as modelos:
        doc=yaml.safe_load(modelos)
    
    #TODO
    for name, vars in doc.items():
        globals()[name] = type(name, (Model,), {})
        globals()[name].init_class(db_collection=db[name], 
                            required_vars=set(vars.get("required_vars", [])),
                            admissible_vars=set(vars.get("admissible_vars", [])),
                            indexes=set(vars.get("indexes", []))
                            )

        
    print("Clases instanciadas")



# TODO 
# PROYECTO 2
# Almacenar los pipelines de las consultas en Q1, Q2, etc. 
# EJEMPLO
# Q0: Listado de todas las personas con nombre determinado
nombre = "Quijote"
Q0 = [{'$match': {'nombre': nombre}}]

# Q1: 
Q2 = []

# Q2: 
Q2 = []

# Q3:
Q3 = []

# Q4: etc.

def initData():
    with open('./Data/Clientes.json') as json_file:
        data = json.load(json_file)
        for client in data:
            newClient = Cliente(**client)
            newClient.save()
    with open('./Data/Compra.json') as json_file:
        data = json.load(json_file)
        for compra in data:
            newCompra = Compra(**compra)
            newCompra.save()
    with open('./Data/Productos.json') as json_file:
        data = json.load(json_file)
        for producto in data:
            newProduct = Producto(**producto)
            newProduct.save()
    with open('./Data/Proveedor.json') as json_file:
        data = json.load(json_file)
        for prov in data:
            newProv = Proveedor(**prov)
            newProv.save()
if __name__ == '__main__':
    initApp()
    #TODO    
    initData()
    for search in Cliente.find({"dni":"11223344A"}):
        client = search
    # Asignar nuevo valor a variable admitida del objeto 
    client.nombre = "Jose"
    # Asignar nuevo valor a variable no admitida del objeto 
    try:
        client.segundoApellido = "Gonzalez"
    except:
        print("No se ha posido completar la operación")
    # Guardar
    print(client.__dict__)
    client.save()
    # Asignar nuevo valor a variable admitida del objeto
    client.edad = 21
    # Guardar
    client.save()
    # Buscar nuevo documento con find
    
    for modelo in Cliente.find({"nombre":"Jose"}):
        print(modelo.__dict__)
        foundModel = modelo

    
    # Obtener primer documento

    # Modificar valor de variable admitida
    foundModel.nombre = "Pedro"
    # Guardar
    foundModel.save()

    # PROYECTO 2
    # Ejecutar consultas Q1, Q2, etc. y mostrarlo
    #TODO
    #Ejemplo
    #Q1_r = MiModelo.aggregate(Q1)






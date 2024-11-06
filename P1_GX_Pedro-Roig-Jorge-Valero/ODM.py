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
from datetime import datetime
import unicodedata
import redis

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
    geolocator = Nominatim(user_agent=f"ODM_ApiPyMongo") ## Inicializo el geolocalizador con un user_agent
    location = None

    while location is None:
        try:
            time.sleep(1)
            #TODO
            location = geolocator.geocode(address) ## Obtengo la localización de la dirección
        except GeocoderTimedOut:
            print(f"Error: geocode failed for address {address}. Retrying...")
            continue

    return Point((location.longitude, location.latitude)) if location else None ## Devuelvo las coordenadas en formato geojson.Point

    
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
        global_cache : redis.Redis
            conexion a la cache global
    
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
    indexes: set[str] ## Para crear los indices en la base de datos
    db: pymongo.collection.Collection
    global_cache: redis.Redis

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
        missing_vars = self.required_vars - kwargs.keys() ## Compruebo que las variables requeridas estén en kwargs
        
        if missing_vars: ## Si faltan variables requeridas
            errorFunction(f"falta información necesaria [{missing_vars}]") ## Muestro un mensaje de error
            return ## Salgo de la función
        
        not_admissible_vars = kwargs.keys() - (self.required_vars | self.admissible_vars | self.indexes) ## Compruebo que haya variables no admitidas
        
        if not_admissible_vars: ## Si hay variables no admitidas
            errorFunction(f"no se admiten estas entradas [{not_admissible_vars}]") ## Muestro un mensaje de error
            return ## Salgo de la función
        
        if self.indexes: ## Si hay índices
            while self.indexes: ## Mientras haya índices
                self.db.create_index(self.indexes.pop(), unique=True) ## Creo un índice único en la base de datos
        for key, value in kwargs.items():
            if 'fecha' in key and isinstance(value, str):
                try:
                    kwargs[key] = datetime.strptime(value, '%Y-%m-%d')
                except ValueError:
                    errorFunction(f"Formato de fecha incorrecto para {key}: {value}")
                    return
        
        self.__dict__.update(kwargs) #para actualizar todos los valores de golpe

    def __setattr__(self, name: str, value: str | dict) -> None:
        """ Sobreescribe el metodo de asignacion de valores a las 
        variables del objeto con el fin de controlar las variables
        que se asignan al modelo y cuando son modificadas.
        """
        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        
        if name not in self.admissible_vars and name not in self.required_vars: ## Si la variable no está admitida
            errorFunction(f"[{name}] no está admitida") ## Muestro un mensaje de error
            return ## Salgo de la función
        if 'flags' not in self.__dict__: ## Si la variable falgs no está inicializada
            self.__dict__['flags'] = [] ## Inicializo la variable flags como una lista vacía
        self.flags.append(name) ## Añado la variable a la lista de flags
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
        # if 'direccion' in self.__dict__:
        #     self.db.create_index([("location", pymongo.GEOSPHERE)])
        #     point = getLocationPoint(self.__dict__['direccion'])
        #     if point:
        #         self.__dict__['location'] = {
        #             'type': 'Point',
        #             'coordinates': list(point['coordinates'])
        #         }

        ## insert_one si no tiene _id
        if '_id' not in self.__dict__:
            self.db.insert_one(self.__dict__)
        else:
            if 'flags' in self.__dict__:
                newValues = {}
                for key in self.flags:
                    newValues[key] = self.__dict__[key]
            
                update_msg = self.db.update_one({'_id': self._id}, { "$set": newValues})
                print(f"Acknowledged: {update_msg.acknowledged}")
                self.__dict__.pop('flags') ## Elimino la variable flags ya que ya se han actualizado las variables

        ## SE ACTUALIZA LA CACHE PARA ESTE VALOR ##
        copy = self.__dict__.copy()
            
        del copy['_id']
        ## GUARDA EN REDIS ##
        for key  in copy.keys():
            if 'fecha' in key:
                copy[key] = str(copy[key])
        self.global_cache.set(str(self.__dict__["_id"]), json.dumps(copy), ex=86400)
            
    def delete(self) -> None:
        """
        Elimina el modelo de la base de datos
        """
        #TODO
        ## SE ACTUALIZA LA CACHE PARA ESTE VALOR ##
        if self.global_cache.exists(str(self.__dict__['_id'])):
            self.global_cache.delete(str(self.__dict__['_id']))
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
        ## SE ACTUALIZA LA CACHE PARA ESTE VALOR ##
        # Si no está en cache, buscar en MongoDB y guardar en cache
        
        return ModelCursor(cls, cls.db.find(filter)) ## Devuelvo un cursor de modelos ModelCursor

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
        ## SE ACTUALIZA LA CACHE PARA ESTE VALOR ##
        # Si no está en cache, buscar en MongoDB y guardar en cache
        document = cls.db.find_one({"_id": id})
        cached_model = cls.global_cache.get(str(document._id))
        if cached_model:
            cls.global_cache.expire(str(document._id), 86400)
        else:
            copy = document.__dict__.copy()
            
            del copy['_id']
            ## GUARDA EN REDIS ##
            for key  in copy.keys():
                if 'fecha' in key:
                    copy[key] = str(copy[key])
            document.global_cache.set(str(document.__dict__["_id"]), json.dumps(copy), ex=86400)

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
        cached_model = cls.global_cache.get(id)
        if cached_model:
            cls.global_cache.expire(id, 86400)
            return cls(**json.loads(cached_model))
        
        # Si no está en cache, buscar en MongoDB y guardar en cache
        document = cls.db.find_one({"_id": id})
        if document:
            cls.global_cache.set(id, json.dumps(document), ex=86400)
            return cls(**document)
        return None

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, global_cache: redis.Redis, required_vars: set[str], admissible_vars: set[str], indexes: set[str]) -> None:
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
        cls.global_cache = global_cache

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
                # Obtiene el siguiente documento del cursor y lo convierte a un objeto Model
                document = next(self.cursor)
                # Convierte el documento en un objeto de Model y lo devuelve
                yield self.model(**document)
            except StopIteration:
                # Si no hay más documentos, sale del bucle
                break
            


def initApp(definitions_path: str = "models.yml", mongodb_uri="mongodb://localhost:27017/", db_name="ProyectBasesDeDatos", ext_globals=globals()) -> None:
    #TODO
    # Inicializar base de datos
    cliente = pymongo.MongoClient(mongodb_uri) ## Inicializo el cliente de MongoDB
    
    db = cliente[db_name] ## Inicializo la base de datos
    
    print("Conectando con la caché")
    cache = redis.Redis(host='localhost', port=6379, db=0)
    cache.flushall()
    print("Conexión establecida con la caché")
    with open(definitions_path, 'r') as modelos: ## Abro el archivo yml con los modelos
        doc=yaml.safe_load(modelos)
    
    #TODO
    for name, vars in doc.items(): ## Recorro los modelos
        db[name].delete_many({}) ## Borro los datos de la colección
        ext_globals[name] = type(name, (Model,), {}) ## Inicializo una clase global con el nombre del modelo
        ext_globals[name].init_class(db_collection=db[name], 
                            global_cache=cache,
                            required_vars=set(vars.get("required_vars", [])),
                            admissible_vars=set(vars.get("admissible_vars", [])),
                            indexes=set(vars.get("indexes", []))
                            ) ## Inicializo la clase con los valores del yml
        
    print("Clases instanciadas")

def initData(): ## Inicializo los datos de los modelos
    with open('./Data/Clientes.json', encoding='utf-8') as json_file:
        data = json.load(json_file)
        for client in data:
            newClient = Cliente(**client) # type: ignore
            newClient.save()
    with open('./Data/Compra.json', encoding='utf-8') as json_file:
        data = json.load(json_file)
        for compra in data:
            newCompra = Compra(**compra) # type: ignore
            newCompra.save()
    with open('./Data/Productos.json', encoding='utf-8') as json_file:
        data = json.load(json_file)
        for producto in data:
            newProduct = Producto(**producto) # type: ignore
            newProduct.save()
    with open('./Data/Proveedor.json', encoding='utf-8') as json_file:
        data = json.load(json_file)
        for prov in data:
            newProv = Proveedor(**prov) # type: ignore
            newProv.save()
    with open('./Data/Almacenes.json', encoding='utf-8') as json_file:
        data = json.load(json_file)
        for almacen in data:
            newAlmacen = Almacen(**almacen) # type: ignore
            newAlmacen.save()
    with open('./Data/Envios.json', encoding='utf-8') as json_file:
        data = json.load(json_file)
        for envio in data:
            newEnvio = Envio(**envio) #type: ignore
            newEnvio.save()
            
if __name__ == '__main__':
    initApp() ## Inicializo la aplicación
    
    #TODO    
    initData() ## Inicializo los datos
    print("End initData")
    ## Busco un cliente con el dni 11223344A
    client = None
    for search in Cliente.find({"email":"juan@example.com"}):  # type: ignore 
        client = search
    # Asignar nuevo valor a variable admitida del objeto 
    if client:
    
        client.nombre = "Jose"
        # Asignar nuevo valor a variable no admitida del objeto 

        client.segundoApellido = "Gonzalez" ## Intento añadir una variable no admitida,
                                            ## no me va a dejar y va a mostrar un mensaje de error
        # Guardar
        print(client.__dict__)
        client.save()
        # Asignar nuevo valor a variable admitida del objeto
        client.edad = 21
        # Guardar
        client.save()
    else:
        print("Cliente no encontrado")
    # Buscar nuevo documento con find
    
    foundModel = None
    for modelo in Cliente.find({"nombre":"Carmen Ruiz"}): # type: ignore
        print(modelo.__dict__)
        foundModel = modelo
    
    # Obtener primer documento
    for modelo in Cliente.find({}): # type: ignore
        firstDocument = modelo
        break
    if foundModel:
        # Modificar valor de variable admitida
        foundModel.edad = 22
        # Guardar
        foundModel.save()
        
        # Encontrar el valor por su id en la caché:
        print("Ahora encontraremos el modelo por su id en la caché")
        foundModel = Cliente.find_by_id(str(foundModel._id)) # type: ignore
        if foundModel:
            print(foundModel.__dict__)
        else:
            print("Cliente no encontrado")
    else:
        print("Cliente no encontrado")






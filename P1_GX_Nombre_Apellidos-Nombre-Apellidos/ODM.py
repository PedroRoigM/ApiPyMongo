__author__ = 'Pablo Ramos Criado'
__students__ = 'Pedro Roig Morera'


from geopy.geocoders import Nominatim # type: ignore
from geopy.exc import GeocoderTimedOut
import time
from typing import Generator, Any, Self
from geojson import Point
import pymongo
import yaml
from pymongo.collection import Collection
from pymongo import command_cursor

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
    location = None
    while location is None:
        try:
            time.sleep(1)
            #TODO
            # Es necesario proporcionar un user_agent para utilizar la API
            # Utilizar un nombre aleatorio para el user_agent
            location = Nominatim(user_agent="Mi-Nombre-Aleatorio").geocode(address)
        except GeocoderTimedOut:
            # Puede lanzar una excepcion si se supera el tiempo de espera
            # Volver a intentarlo
            continue
    #TODO
    # Devolver un GeoJSON de tipo punto con la latitud y longitud almacenadas

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
        
        if len(missing_vars) != 0:
            print(f"Valores no recibidos: {missing_vars}")
            return
        
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
        if '_id' not in self.__dict__:
            self.db.insert_one(self.__dict__)
        else:
            if 'flags' in self.__dict__:
                newValues = {}
                for key in self.flags:
                    newValues[key] = self.__dict__[key]
            self.db.update_one({'_id': self._id}, { "$set": newValues})
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
        """
        Inicializa el cursor con la clase de modelo y el cursor de pymongo

        Parameters
        ----------
            model_class : Model
                Clase para crear los modelos de los documentos que se iteran.
            cursor: pymongo.cursor.Cursor
                Cursor de pymongo a iterar
        """
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
        while self.cursor.alive:
            data = self.cursor.get_data()
            yield data
            


def initApp(definitions_path: str = "models.yml", mongodb_uri="mongodb://localhost:27017/", db_name="ProyectBasesDeDatos") -> None:
    """ 
    Declara las clases que heredan de Model para cada uno de los 
    modelos de las colecciones definidas en definitions_path.
    Inicializa las clases de los modelos proporcionando las variables 
    admitidas y requeridas para cada una de ellas y la conexión a la
    collecion de la base de datos.
    
    Parameters
    ----------
        definitions_path : str
            ruta al fichero de definiciones de modelos
        mongodb_uri : str
            uri de conexion a la base de datos
        db_name : str
            nombre de la base de datos
    """
    #TODO
    # Inicializar base de datos
    cliente = pymongo.MongoClient(mongodb_uri)
    db = cliente[db_name]
    
    
    #print(db.list_collection_names())
    ################################################################
    with open(definitions_path, 'r') as modelos:
        doc=yaml.safe_load(modelos)
    
    
    #TODO
    # Declarar tantas clases modelo colecciones existan en la base de datos
    # Leer el fichero de definiciones de modelos para obtener las colecciones
    # y las variables admitidas y requeridas para cada una de ellas.
    # Ejemplo de declaracion de modelo para colecion llamada MiModelo
    
    # coleccion_cliente = db["Cliente"]
    # coleccion_cliente.insert_one({"name":"Jose", "edad": 19})
    
    # Ignorar el warning de Pylance sobre MiModelo, es incapaz de detectar
    # que se ha declarado la clase en la linea anterior ya que se hace
    # en tiempo de ejecucion.
    #MiModelo.init_class(db_collection=doc["MiModelo"], required_vars=doc["MiModelo"]["required_vars"], admissible_vars=doc["MiModelo"]["admissible_vars"]) # type: ignore
    for name, vars in doc.items():
        globals()[name] = type(name, (Model,), {})
        print(name)
        print(f"Required vars: {vars.get("required_vars", [])}")
        print(f"Admissible vars: {vars.get("admissible_vars", [])}\n\n")
        globals()[name].init_class(db_collection=db[name], 
                            required_vars=set(vars.get("required_vars", [])),
                            admissible_vars=set(vars.get("admissible_vars", [])),
                            indexes=set(vars.get("indexes", []))
                            )

        
    print("Clases instanciadas")
        
        #globals()[modal_name].init_class(db_collection=db[modal_name], required_vars=required_vars, admissible_vars=admissible_vars)
    


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


if __name__ == '__main__':
    
    # Inicializar base de datos y modelos con initApp
    #TODO
    initApp()

    #Ejemplo
    #m = Model(nombre="Pablo", apellido="Ramos", edad=18)
    #m.save()
    #m.nombre="Pedro"
    #print(m.nombre)

    # Hacer pruebas para comprobar que funciona correctamente el modelo
    #TODO
    # Crear modelo
    #modelo = Model(nombre="Pablo", apellido="Ramos", edad=18)
    
    ### CREAR INDICES: db.students.createIndex({name:1}, {unique:true})
    cliente = Cliente(nombre="Pablo", apellido="Ramos", dni="49480836Q")
    print(cliente.nombre)
    print(cliente.__dict__)
    cliente.save()
    cliente.nombre = "Pedro"
    print(cliente.__dict__)
    cliente.save()
    
    print(cliente.find({nombre:'Pedro'}).__iter__())
    #cliente.delete()
    #diccionario = dict(_id='1',nombre="Jose", apellido="Ramos")
    #cliente.db.insert_one(diccionario)

    # Asignar nuevo valor a variable admitida del objeto 
    
    # Asignar nuevo valor a variable no admitida del objeto 

    # Guardar

    # Asignar nuevo valor a variable admitida del objeto

    # Guardar

    # Buscar nuevo documento con find

    # Obtener primer documento

    # Modificar valor de variable admitida

    # Guardar

    # PROYECTO 2
    # Ejecutar consultas Q1, Q2, etc. y mostrarlo
    #TODO
    #Ejemplo
    #Q1_r = MiModelo.aggregate(Q1)






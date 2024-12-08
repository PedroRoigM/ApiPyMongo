from neo4j import GraphDatabase
from datetime import datetime, timedelta

# Configuración de conexión
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "ltCEWANsdUbIdPywi_FOLB4agT992rRUf3Yh_i9Es18"

class Neo4jDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def cargar_datos(self):
        # Eliminar datos existentes
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        
        # Ciudades y plataformas
        ciudades = [
            {"nombre": 'Madrid', "lat": 40.4168, "lon": -3.7038},
            {"nombre": 'Barcelona', "lat": 41.3879, "lon": 2.16992},
            {"nombre": 'Palma de Mallorca', "lat": 39.5696, "lon": 2.6502},
            {"nombre": 'Valencia', "lat": 39.4699, "lon": -0.3763},
            {"nombre": 'Sevilla', "lat": 37.3886, "lon": -5.9823},
            {"nombre": 'Granada', "lat": 37.1773, "lon": -3.5986},
            {"nombre": 'Zaragoza', "lat": 41.649, "lon": -0.8877},
            {"nombre": 'Bilbao', "lat": 43.263, "lon": -2.935},
            {"nombre": 'Málaga', "lat": 36.7213, "lon": -4.4214},
            {"nombre": 'Santander', "lat": 43.4623, "lon": -3.8099},
            {"nombre": 'Valladolid', "lat": 41.6523, "lon": -4.7245},
            {"nombre": 'Alicante', "lat": 38.3452, "lon": -0.481},
            {"nombre": 'A Coruña', "lat": 43.3623, "lon": -8.4115}
        ]
        
        # Crear ciudades
        with self.driver.session() as session:
            
            query = """
                WITH [
                    {nombre: "Madrid", lat: 40.4168, lon: -3.7038},
                    {nombre: "Barcelona", lat: 41.3879, lon: 2.16992},
                    {nombre: "Palma de Mallorca", lat: 39.5696, lon: 2.6502},
                    {nombre: "Valencia", lat: 39.4699, lon: -0.3763},
                    {nombre: "Sevilla", lat: 37.3886, lon: -5.9823},
                    {nombre: "Granada", lat: 37.1773, lon: -3.5986},
                    {nombre: "Zaragoza", lat: 41.649, lon: -0.8877},
                    {nombre: "Bilbao", lat: 43.263, lon: -2.935}
                ] AS ciudades
                UNWIND ciudades AS ciudad
                CREATE (:Ciudad {nombre: ciudad.nombre, lat: ciudad.lat, lon: ciudad.lon});

                """
            session.run(query)
            
            query_plataformas = """
            WITH [
                {nombre: "Barajas", tipo: "aeropuerto", lat: 40.4719, lon: -3.5626},
                {nombre: "Estación de Atocha", tipo: "estacion", lat: 40.4067, lon: -3.6919},
                {nombre: "El Prat", tipo: "aeropuerto", lat: 41.2974, lon: 2.0833},
                {nombre: "Puerto de Barcelona", tipo: "puerto", lat: 41.3518, lon: 2.1685},
                {nombre: "Estación de Sants", tipo: "estacion", lat: 41.3809, lon: 2.1405},
                {nombre: "Son Sant Joan", tipo: "aeropuerto", lat: 39.5517, lon: 2.7388},
                {nombre: "Puerto de Palma", tipo: "puerto", lat: 39.5696, lon: 2.6496},
                {nombre: "Manises", tipo: "aeropuerto", lat: 39.4893, lon: -0.4816},
                {nombre: "Puerto de Valencia", tipo: "puerto", lat: 39.4521, lon: -0.3163},
                {nombre: "Estación del Norte", tipo: "estacion", lat: 39.4665, lon: -0.3781},
                {nombre: "San Pablo", tipo: "aeropuerto", lat: 37.418, lon: -5.8931},
                {nombre: "Puerto de Sevilla", tipo: "puerto", lat: 37.3624, lon: -6.0039},
                {nombre: "Estación de Santa Justa", tipo: "estacion", lat: 37.3918, lon: -5.9758}
            ] AS plataformas
            UNWIND plataformas AS plataforma
            CREATE (:Plataforma {nombre: plataforma.nombre, tipo: plataforma.tipo, lat: plataforma.lat, lon: plataforma.lon});
            """
            session.run(query_plataformas)
            conexiones_queries = [
            # Madrid - Barcelona
            """
            MATCH (madrid:Ciudad {nombre: "Madrid"}), (barcelona:Ciudad {nombre: "Barcelona"}), 
                (barajas:Plataforma {nombre: "Barajas"}), (el_prat:Plataforma {nombre: "El Prat"}),
                (estacion_atocha:Plataforma {nombre: "Estación de Atocha"}), 
                (estacion_sants:Plataforma {nombre: "Estación de Sants"})
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 540, costo: 9}]->(barcelona)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 30, costo: 10}]->(barajas)
            MERGE (barajas)-[:CONEXION {modo: "avion", tiempo: 190, costo: 175}]->(el_prat)
            MERGE (el_prat)-[:CONEXION {modo: "coche", tiempo: 25, costo: 8}]->(barcelona)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(estacion_atocha)
            MERGE (estacion_atocha)-[:CONEXION {modo: "ferrocarril", tiempo: 60, costo: 64}]->(estacion_sants)
            MERGE (estacion_sants)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(barcelona)
            """,
            # Madrid - Palma de Mallorca
            """
            MATCH (madrid:Ciudad {nombre: "Madrid"}), (palma:Ciudad {nombre: "Palma de Mallorca"}), 
                (barajas:Plataforma {nombre: "Barajas"}), (son_sant_joan:Plataforma {nombre: "Son Sant Joan"}), 
                (puerto_valencia:Plataforma {nombre: "Puerto de Valencia"}), 
                (puerto_palma:Plataforma {nombre: "Puerto de Palma"})
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 30, costo: 10}]->(barajas)
            MERGE (barajas)-[:CONEXION {modo: "avion", tiempo: 115, costo: 140}]->(son_sant_joan)
            MERGE (son_sant_joan)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(palma)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 210, costo: 35}]->(puerto_valencia)
            MERGE (puerto_valencia)-[:CONEXION {modo: "barco", tiempo: 600, costo: 60}]->(puerto_palma)
            MERGE (puerto_palma)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(palma)
            """,
            # Madrid - Valencia
            """
            MATCH (madrid:Ciudad {nombre: "Madrid"}), (valencia:Ciudad {nombre: "Valencia"}), 
                (barajas:Plataforma {nombre: "Barajas"}), (manises:Plataforma {nombre: "Manises"}), 
                (estacion_atocha:Plataforma {nombre: "Estación de Atocha"}), 
                (estacion_del_norte:Plataforma {nombre: "Estación del Norte"})
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 210, costo: 35}]->(valencia)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 30, costo: 10}]->(barajas)
            MERGE (barajas)-[:CONEXION {modo: "avion", tiempo: 90, costo: 80}]->(manises)
            MERGE (manises)-[:CONEXION {modo: "coche", tiempo: 20, costo: 7}]->(valencia)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(estacion_atocha)
            MERGE (estacion_atocha)-[:CONEXION {modo: "ferrocarril", tiempo: 50, costo: 32}]->(estacion_del_norte)
            MERGE (estacion_del_norte)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(valencia)
            """,
            # Madrid - Sevilla
            """
            MATCH (madrid:Ciudad {nombre: "Madrid"}), (sevilla:Ciudad {nombre: "Sevilla"}), 
                (barajas:Plataforma {nombre: "Barajas"}), (san_pablo:Plataforma {nombre: "San Pablo"}), 
                (estacion_atocha:Plataforma {nombre: "Estación de Atocha"}), 
                (estacion_santa_justa:Plataforma {nombre: "Estación de Santa Justa"})
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 300, costo: 35}]->(sevilla)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 30, costo: 10}]->(barajas)
            MERGE (barajas)-[:CONEXION {modo: "avion", tiempo: 140, costo: 150}]->(san_pablo)
            MERGE (san_pablo)-[:CONEXION {modo: "coche", tiempo: 20, costo: 8}]->(sevilla)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 10, costo: 3}]->(estacion_atocha)
            MERGE (estacion_atocha)-[:CONEXION {modo: "ferrocarril", tiempo: 70, costo: 56}]->(estacion_santa_justa)
            MERGE (estacion_santa_justa)-[:CONEXION {modo: "coche", tiempo: 10, costo: 3}]->(sevilla)
            """,
            # Madrid - Granada
            """
            MATCH (madrid:Ciudad {nombre: "Madrid"}), (granada:Ciudad {nombre: "Granada"}), 
                (barajas:Plataforma {nombre: "Barajas"}), 
                (estacion_atocha:Plataforma {nombre: "Estación de Atocha"})
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 300, costo: 35}]->(granada)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 30, costo: 10}]->(barajas)
            MERGE (barajas)-[:CONEXION {modo: "avion", tiempo: 110, costo: 105}]->(granada)
            MERGE (madrid)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(estacion_atocha)
            MERGE (estacion_atocha)-[:CONEXION {modo: "ferrocarril", tiempo: 95, costo: 56}]->(granada)
            """,
            # Barcelona - Palma de Mallorca
            """
            MATCH (barcelona:Ciudad {nombre: "Barcelona"}), (palma:Ciudad {nombre: "Palma de Mallorca"}), 
                (el_prat:Plataforma {nombre: "El Prat"}), (son_sant_joan:Plataforma {nombre: "Son Sant Joan"}), 
                (puerto_barcelona:Plataforma {nombre: "Puerto de Barcelona"}), 
                (puerto_palma:Plataforma {nombre: "Puerto de Palma"})
            MERGE (barcelona)-[:CONEXION {modo: "coche", tiempo: 25, costo: 8}]->(el_prat)
            MERGE (el_prat)-[:CONEXION {modo: "avion", tiempo: 95, costo: 140}]->(son_sant_joan)
            MERGE (son_sant_joan)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(palma)
            MERGE (barcelona)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(puerto_barcelona)
            MERGE (puerto_barcelona)-[:CONEXION {modo: "barco", tiempo: 600, costo: 39}]->(puerto_palma)
            MERGE (puerto_palma)-[:CONEXION {modo: "coche", tiempo: 15, costo: 5}]->(palma)
            """,
            # Barcelona - Sevilla
            """                    
            MATCH (barcelona:Ciudad {nombre: "Barcelona"}), (sevilla:Ciudad {nombre: "Sevilla"}), 
                (el_prat:Plataforma {nombre: "El Prat"}), (san_pablo:Plataforma {nombre: "San Pablo"}), 
                (estacion_sants:Plataforma {nombre: "Estación de Sants"}), (estacion_santa_justa:Plataforma {nombre: "Estación de Santa Justa"})
            MERGE (barcelona)-[:CONEXION {modo: "coche", tiempo: 1020, costo: 15}]->(sevilla)
            MERGE (barcelona)-[:CONEXION {modo: "coche", tiempo: 30, costo: 12}]->(el_prat)
            MERGE (el_prat)-[:CONEXION {modo: "avion", tiempo: 145, costo: 210}]->(san_pablo)
            MERGE (san_pablo)-[:CONEXION {modo: "coche", tiempo: 25, costo: 10}]->(sevilla)
            MERGE (barcelona)-[:CONEXION {modo: "coche", tiempo: 20, costo: 6}]->(estacion_sants)
            MERGE (estacion_sants)-[:CONEXION {modo: "ferrocarril", tiempo: 270, costo: 428}]->(estacion_santa_justa)
            MERGE (estacion_santa_justa)-[:CONEXION {modo: "coche", tiempo: 20, costo: 6}]->(sevilla)
            """,
        # Barcelona - Valencia 
        """
        MATCH (barcelona:Ciudad {nombre: "Barcelona"}), (valencia:Ciudad {nombre: "Valencia"}), 
            (el_prat:Plataforma {nombre: "El Prat"}), (manises:Plataforma {nombre: "Manises"}), 
            (estacion_sants:Plataforma {nombre: "Estación de Sants"}), 
            (estacion_valencia:Plataforma {nombre: "Estación del Norte"})
        MERGE 
            // Coche directo
            (barcelona)-[:CONEXION {modo: "coche", tiempo: 210, costo: 21}]->(valencia)
        MERGE    
            // Avión
            (barcelona)-[:CONEXION {modo: "coche", tiempo: 20, costo: 5}]->(el_prat)
        MERGE    (barcelona)-[:CONEXION {modo:"coche",tiempo: 20, costo: 5}]->(el_prat)
        MERGE    (el_prat)-[:CONEXION {modo:"avion",tiempo: 90, costo: 140}]->(manises)
        MERGE    (manises)-[:CONEXION {modo:"coche",tiempo: 15, costo: 5}]->(valencia)
        MERGE    
            // Tren
            (barcelona)-[:CONEXION {modo:"coche",tiempo: 10, costo: 3}]->(estacion_sants)
        MERGE    (estacion_sants)-[:CONEXION {modo:"ferrocarril",tiempo: 90, costo: 144}]->(estacion_valencia)
        MERGE    (estacion_valencia)-[:CONEXION {modo:"coche",tiempo: 5, costo: 2}]->(valencia)
        """ ,
        # Palma de Mallorca - Sevilla 
        """
        MATCH (palma:Ciudad {nombre: "Palma de Mallorca"}), (sevilla:Ciudad {nombre: "Sevilla"}), 
            (son_sant_joan:Plataforma {nombre: "Son Sant Joan"}), (san_pablo:Plataforma {nombre: "San Pablo"}), 
            (puerto_palma:Plataforma {nombre: "Puerto de Palma"}), (puerto_sevilla:Plataforma {nombre: "Puerto de Sevilla"})
        MERGE (son_sant_joan)-[:CONEXION {modo:"avion", tiempo: 120, costo: 240}]->(san_pablo)
        MERGE (san_pablo)-[:CONEXION {modo:"coche", tiempo: 20, costo: 8}]->(sevilla)
        MERGE    
            // Barco
            (puerto_palma)-[:CONEXION {modo:"barco", tiempo: 1800, costo: 21}]->(puerto_sevilla)
        MERGE    (puerto_sevilla)-[:CONEXION {modo:"coche", tiempo: 30, costo: 10}]->(sevilla)
        """ ,
        # Valencia - Sevilla 
        """MATCH (valencia:Ciudad {nombre: "Valencia"}), (sevilla:Ciudad {nombre: "Sevilla"}), 
            (manises:Plataforma {nombre: "Manises"}), (san_pablo:Plataforma {nombre: "San Pablo"}), 
            (estacion_valencia:Plataforma {nombre: "Estación de Valencia"}), 
            (estacion_santa_justa:Plataforma {nombre: "Estación de Santa Justa"})
        MERGE 
            // Avión
            (valencia)-[:CONEXION {modo:"coche", tiempo: 20, costo: 5}]->(manises)
        MERGE (manises)-[:CONEXION {modo:"avion", tiempo: 150, costo: 245}]->(san_pablo)
        MERGE (san_pablo)-[:CONEXION {modo:"coche", tiempo: 20, costo: 8}]->(sevilla)
        MERGE 
            // Tren
            (valencia)-[:CONEXION {modo:"coche", tiempo: 15, costo: 4}]->(estacion_valencia)
        MERGE (estacion_valencia)-[:CONEXION {modo:"ferrocarril", tiempo: 150, costo: 240}]->(estacion_santa_justa)
        MERGE (estacion_santa_justa)-[:CONEXION {modo:"coche", tiempo: 10, costo: 3}]->(sevilla)
        MERGE 
            // Coche directo
            (valencia)-[:CONEXION {modo:"coche", tiempo: 420, costo: 21}]->(sevilla)
        """,
        # Granada - Sevilla 
        """MATCH (granada:Ciudad {nombre: "Granada"}), (sevilla:Ciudad {nombre: "Sevilla"}), 
            (estacion_santa_justa:Plataforma {nombre: "Estación de Santa Justa"})
        MERGE 
            // Tren
            (granada)-[:CONEXION {modo:"ferrocarril", tiempo: 75, costo: 120}]->(estacion_santa_justa)
        MERGE (estacion_santa_justa)-[:CONEXION {modo:"coche", tiempo: 10, costo: 3}]->(sevilla)
        MERGE 
            // Coche directo
            (granada)-[:CONEXION {modo:"coche", tiempo: 180, costo: 14}]->(sevilla)""",

        # Barcelona - Granada
        """MATCH (barcelona:Ciudad {nombre: "Barcelona"}), (granada:Ciudad {nombre: "Granada"}), 
            (el_prat:Plataforma {nombre: "El Prat"}), 
            (estacion_sants:Plataforma {nombre: "Estación de Sants"})
        MERGE 
            // Avión
            (barcelona)-[:CONEXION {modo:"coche", tiempo: 30, costo: 12}]->(el_prat)
        MERGE (el_prat)-[:CONEXION {modo:"avion", tiempo: 115, costo: 158}]->(granada)
        MERGE 
            // Tren
            (barcelona)-[:CONEXION {modo:"coche", tiempo: 20, costo: 6}]->(estacion_sants)
        MERGE (estacion_sants)-[:CONEXION {modo:"ferrocarril", tiempo: 265, costo: 200}]->(granada)
        MERGE 
            // Coche directo
            (barcelona)-[:CONEXION {modo:"coche", tiempo: 900, costo: 18}]->(granada)""",

        # Zaragoza - Barcelona
        """MATCH (zaragoza:Ciudad {nombre: "Zaragoza"}), (barcelona:Ciudad {nombre: "Barcelona"}),  
            (estacion_sants:Plataforma {nombre: "Estación de Sants"}),
            (el_prat:Plataforma {nombre: "El Prat"})
        MERGE 
            // Coche directo
            (zaragoza)-[:CONEXION {modo:"coche", tiempo: 180, costo: 21}]->(barcelona)
        MERGE 
            // Tren
            (zaragoza)-[:CONEXION {modo:"ferrocarril", tiempo: 45, costo: 72}]->(estacion_sants)
        MERGE (estacion_sants)-[:CONEXION {modo:"coche", tiempo: 15, costo: 5}]->(barcelona)
        MERGE 
            // Avión
            (zaragoza)-[:CONEXION {modo:"avion", tiempo: 95, costo: 123}]->(el_prat)
        MERGE (el_prat)-[:CONEXION {modo:"coche", tiempo: 25, costo: 8}]->(barcelona)""",

        # Zaragoza - Madrid
        """MATCH (zaragoza:Ciudad {nombre: "Zaragoza"}), (madrid:Ciudad {nombre: "Madrid"}),  
            (estacion_atocha:Plataforma {nombre: "Estación de Atocha"}),
            (barajas:Plataforma {nombre: "Barajas"})
        MERGE 
            // Coche directo
            (zaragoza)-[:CONEXION {modo:"coche", tiempo: 200, costo: 18}]->(madrid)
        MERGE 
            // Tren
            (zaragoza)-[:CONEXION {modo:"ferrocarril", tiempo: 45, costo: 80}]->(estacion_atocha)
        MERGE (estacion_atocha)-[:CONEXION {modo:"coche", tiempo: 15, costo: 5}]->(madrid)
        MERGE 
            // Avión
            (zaragoza)-[:CONEXION {modo:"avion", tiempo: 105, costo: 140}]->(barajas)
        MERGE (barajas)-[:CONEXION {modo:"coche", tiempo: 30, costo: 10}]->(madrid)""",

        """MATCH (zaragoza:Ciudad {nombre: "Zaragoza"}), (valencia:Ciudad {nombre: "Valencia"}), 
            (estacion_norte:Plataforma {nombre: "Estación del Norte"}),
            (manises:Plataforma {nombre: "Manises"})
        MERGE 
            // Coche directo
            (zaragoza)-[:CONEXION {modo:"coche", tiempo: 180, costo: 21}]->(valencia)
        MERGE 
            // Tren
            (zaragoza)-[:CONEXION {modo:"ferrocarril", tiempo: 75, costo: 108}]->(estacion_norte)
        MERGE (estacion_norte)-[:CONEXION {modo:"coche", tiempo: 15, costo: 5}]->(valencia)
        MERGE 
            // Avión
            (zaragoza)-[:CONEXION {modo:"avion", tiempo: 90, costo: 105}]->(manises)
        MERGE (manises)-[:CONEXION {modo:"coche", tiempo: 20, costo: 6}]->(valencia)""",
        # Zaragoza - Sevilla
        """MATCH (zaragoza:Ciudad {nombre: "Zaragoza"}), (sevilla:Ciudad {nombre: "Sevilla"}), 
            (estacion_santa_justa:Plataforma {nombre: "Estación de Santa Justa"}),
            (san_pablo:Plataforma {nombre: "San Pablo"})
        MERGE (zaragoza)-[:CONEXION {modo:"coche", tiempo: 420, costo: 21}]->(sevilla)
        MERGE (zaragoza)-[:CONEXION {modo:"ferrocarril", tiempo: 105, costo: 240}]->(estacion_santa_justa)
        MERGE (estacion_santa_justa)-[:CONEXION {modo:"coche", tiempo: 10, costo: 4}]->(sevilla)
        MERGE (zaragoza)-[:CONEXION {modo:"avion", tiempo: 180, costo: 210}]->(san_pablo)
        MERGE (san_pablo)-[:CONEXION {modo:"coche", tiempo: 15, costo: 5}]->(sevilla)""",

        # Bilbao - Barcelona
        """MATCH (bilbao:Ciudad {nombre: "Bilbao"}), (barcelona:Ciudad {nombre: "Barcelona"}), 
            (sants:Plataforma {nombre: "Estación de Sants"}), 
            (el_prat:Plataforma {nombre: "Aeropuerto de Barcelona-El Prat"})
        MERGE (bilbao)-[:CONEXION {modo:"coche", tiempo: 420, costo: 21}]->(barcelona)
        MERGE (bilbao)-[:CONEXION {modo:"ferrocarril", tiempo: 225, costo: 432}]->(sants)
        MERGE (sants)-[:CONEXION {modo:"coche", tiempo: 15, costo: 5}]->(barcelona)
        MERGE (bilbao)-[:CONEXION {modo:"avion", tiempo: 195, costo: 175}]->(el_prat)
        MERGE (el_prat)-[:CONEXION {modo:"coche", tiempo: 20, costo: 6}]->(barcelona)""",

        # Bilbao - Palma
        """MATCH (bilbao:Ciudad {nombre: "Bilbao"}), (palma:Ciudad {nombre: "Palma"}), 
            (son_sant_joan:Plataforma {nombre: "Aeropuerto de Palma de Mallorca"})
        MERGE (bilbao)-[:CONEXION {modo:"avion", tiempo: 200, costo: 210}]->(son_sant_joan)
        MERGE (son_sant_joan)-[:CONEXION {modo:"coche", tiempo: 20, costo: 6}]->(palma)""",

        # Bilbao - Zaragoza
        """MATCH (bilbao:Ciudad {nombre: "Bilbao"}), (zaragoza:Ciudad {nombre: "Zaragoza"})
        MERGE (bilbao)-[:CONEXION {modo:"coche", tiempo: 180, costo: 21}]->(zaragoza)
        MERGE (bilbao)-[:CONEXION {modo:"ferrocarril", tiempo: 120, costo: 240}]->(zaragoza)
        MERGE (bilbao)-[:CONEXION {modo:"avion", tiempo: 100, costo: 123}]->(zaragoza)""",

        # Bilbao - Madrid
        """MATCH (bilbao:Ciudad {nombre: "Bilbao"}), (madrid:Ciudad {nombre: "Madrid"}), 
            (atocha:Plataforma {nombre: "Estación de Atocha"}), 
            (barajas:Plataforma {nombre: "Aeropuerto de Madrid-Barajas"})
        MERGE (bilbao)-[:CONEXION {modo:"coche", tiempo: 240, costo: 21}]->(madrid)
        MERGE (bilbao)-[:CONEXION {modo:"ferrocarril", tiempo: 150, costo: 288}]->(atocha)
        MERGE (atocha)-[:CONEXION {modo:"coche", tiempo: 10, costo: 3}]->(madrid)
        MERGE (bilbao)-[:CONEXION {modo:"avion", tiempo: 150, costo: 140}]->(barajas)
        MERGE (barajas)-[:CONEXION {modo:"coche", tiempo: 20, costo: 6}]->(madrid)
        """
         ]
            for query in conexiones_queries:
                session.run(query)
            query_check = """
            CALL gds.graph.exists('citiesGraph')
            YIELD graphName, exists
            RETURN exists;
            """
            result = session.run(query_check).single()
            
            if result and result["exists"]:
                # Si el grafo existe, eliminarlo
                query_drop = """
                CALL gds.graph.drop('citiesGraph') YIELD graphName;
                """
                session.run(query_drop)

            # Crear el grafo de ciudades
            query_create = """
            CALL gds.graph.project(
                'citiesGraph',
                ['Ciudad', 'Plataforma'],
                {
                    CONEXION: {
                        orientation: 'UNDIRECTED',  
                        properties: ['costo', 'tiempo']
                    }
                }
            );
            """
            
            result = session.run(query_create)
            
            print("Datos cargados correctamente.")
            
    def calculate_time_until(self, days, hours):
        now = datetime.now()
        deadline = (now + timedelta(days)).replace(hour=hours, minute=0, second=0, microsecond=0)
        if now > deadline:
            return 0  
        return int((deadline - now).total_seconds() / 60)
    
    def sendPackageTodayBefore19(self, origin, destination, packageId):
        print(f"Enviando paquete {packageId} desde {origin} hasta {destination} antes de las 19:00.")
        maxTime = self.calculate_time_until(days=0, hours=19)
        if maxTime == 0:
            print("No hay tiempo suficiente para enviar el paquete hoy.")
            return
        self.sendPackageBefore(origin, destination, packageId, maxTime)

    def sendPackageTodayBeforeTomorrow14(self, origin, destination, packageId):
        print(f"Enviando paquete {packageId} desde {origin} hasta {destination} antes de mañana a las 14:00.")
        maxTime = self.calculate_time_until(days=1, hours=14)
        if maxTime == 0:
            print("No hay tiempo suficiente para enviar el paquete hoy.")
            return
        self.sendPackageBefore(origin, destination, packageId, maxTime)

    def sendPackageBefore(self, origin, destination, packageId, maxTime):
        with self.driver.session() as session:
            print(f"Calculando la ruta más rápida y económica para el paquete {packageId} desde {origin} hasta {destination} en {maxTime} minutos.")
            # Buscar el camino más económico y rápido que cumpla con el tiempo máximo
            query_shortest_path = """
            MATCH (source:Ciudad {nombre: $sourceName}), (target:Ciudad {nombre: $targetName})
            CALL gds.allShortestPaths.dijkstra.stream('citiesGraph', {
                sourceNode: source,  
                relationshipWeightProperty: 'tiempo'
            })
            YIELD index, sourceNode AS sourceNodeC, targetNode AS targetNodeC, totalCost AS totalCostC, nodeIds, costs, path
            WITH
                index,
                gds.util.asNode(sourceNodeC).nombre AS sourceNodeName,
                gds.util.asNode(targetNodeC).nombre AS targetNodeName,
                totalCostC,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).nombre] AS nodeNames,
                costs AS costos,
                path
            WHERE gds.util.asNode(targetNodeC).nombre = $targetName
            UNWIND relationships(path) AS rel
            WITH
                index,
                sourceNodeName,
                targetNodeName,
                totalCostC,
                nodeNames,
                costos
            WHERE totalCostC <= $maxTime
            RETURN
                sourceNodeName,
                targetNodeName,
                totalCostC,
                nodeNames,
                costos
            ORDER BY totalCostC
            LIMIT 1;
            """

            result = session.run(query_shortest_path, sourceName=origin, targetName=destination, maxTime=maxTime).single()
            
            if result:
                print(f"Ruta encontrada con tiempo {result['totalCostC']} minutos.")
                for i in range(1, len(result["costos"])): # Calcular los tiempos de cada trayecto ya que costos son los valores acumulados
                    for j in range(i):
                        result["costos"][i] -= result["costos"][j]
                # Ahora obtenemos los costes del trayecto para así poder utilizar la misma metodología que el otro método
                for i in range(0, len(result["costos"]) - 1):
                    query = """
                    MATCH path = (source:Ciudad | Plataforma {nombre: $origin}) - [r:CONEXION] - (target:Ciudad | Plataforma {nombre: $target})
                    WITH  path, r, 
                        reduce(totalTime = 0, r IN relationships(path) | totalTime + r.tiempo) AS totalTime
                    WHERE totalTime = $time
                    RETURN r.costo AS totalPrice
                    ORDER BY totalPrice ASC
                    LIMIT 1;
                    """
                    price = session.run(query, origin=result["nodeNames"][i], target=result["nodeNames"][i+1], time=result["costos"][i + 1]).single()
                    result["costos"][i] = price["totalPrice"]

                self.manage_package_creation(result, origin, destination, packageId)
            else:
                print(f"No se ha encontrado ninguna ruta que cumpla con el tiempo máximo de {maxTime} minutos. El paquete se entregará lo antes posible.")
                self.sendPackage(origin, destination, packageId)


    def sendPackage(self, origin, destination, packageId):
        with self.driver.session() as session:
            print(f"Calculando la ruta más económica para el paquete {packageId} desde {origin} hasta {destination}.")
            # Consulta para calcular el camino más corto usando los nombres de los nodos
            query_shortest_path = """
            MATCH (source:Ciudad {nombre: $sourceName}), (target:Ciudad {nombre: $targetName})
            CALL gds.allShortestPaths.dijkstra.stream('citiesGraph', {
                sourceNode: source,  
                relationshipWeightProperty: 'costo'
            })
            YIELD index, sourceNode AS sourceNodeC, targetNode AS targetNodeC, totalCost AS totalCostC, nodeIds, costs, path
            WITH
                index,
                gds.util.asNode(sourceNodeC).nombre AS sourceNodeName,
                gds.util.asNode(targetNodeC).nombre AS targetNodeName,
                totalCostC,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).nombre] AS nodeNames,
                costs AS costos,
                path
            WHERE gds.util.asNode(targetNodeC).nombre = $targetName
            UNWIND relationships(path) AS rel
            WITH
                index,
                sourceNodeName,
                targetNodeName,
                totalCostC,
                nodeNames,
                costos
            RETURN
                sourceNodeName,
                targetNodeName,
                totalCostC,
                nodeNames,
                costos
            ORDER BY totalCostC
            LIMIT 1;
            """

            result = session.run(query_shortest_path, sourceName=origin, targetName=destination).single()
            
            if result:
                for i in range(1, len(result["costos"])): # Calcular los costos de cada trayecto ya que costs son los valores acumulados
                    for j in range(i):
                        result["costos"][i] -= result["costos"][j]
                
                result['costos'].pop(0) # Eliminar el primer elemento de la lista de costos ya que no es necesario
                self.manage_package_creation(result, origin, destination, packageId)
            
    def manage_package_creation(self, path_result, origin, destination, packageId):
        with self.driver.session() as session:
            # Extraer datos del resultado
            node_names = path_result["nodeNames"]
            source = node_names.pop(0)
            target = node_names[0]
            costs = path_result["costos"] 
            print(costs)
            
            
            print(f"Paquete: {packageId}")
            print(f"El paquete va a seguir esta ruta: {node_names}")
            print(f"Y va a tener estos costos de transporte: {costs}")
            initial_cost = costs[0] # Obtener el costo del primer trayecto
            # Crear el paquete con la ruta obtenida
            query_create_package = """
            CREATE (p:Paquete {id: $packageId, origen: $origin, destino: $destination, route: $route, costs: $costs})	
            """
            session.run(query_create_package, packageId=packageId, origin=origin, destination=destination, route=node_names, costs=costs)
            self.search_create_vehicle(source, target, packageId, initial_cost, node_names, costs)
            
    def search_create_vehicle(self, source, target, packageId, initial_cost, route, costs):
        with self.driver.session() as session:
            # Consulta para buscar el camino más corto que tenga el costo inicial
            print(f"Buscando vehículo para el paquete {packageId} desde {source} hasta {target} con un costo de {initial_cost}.")
            query = """
                    MATCH path = (source:Ciudad | Plataforma {nombre: $origin}) - [r:CONEXION] - (target:Ciudad | Plataforma {nombre: $target})
                    WITH  path, r, 
                        reduce(totalPrice = 0, r IN relationships(path) | totalPrice + r.costo) AS totalPrice
                    WHERE totalPrice = $cost
                    RETURN path, totalPrice, r.modo AS modo
                    ORDER BY totalPrice ASC
                    """
            path = session.run(query, origin=source, target=target, cost=initial_cost).single()
                    
                    # Intentar asignar a un vehículo existente
            search_vehicle_query = """
                    MATCH (v:Vehiculo)-[:UBICADO_EN]->(c:Ciudad | Plataforma {nombre: $origin})
                    WHERE v.tipo = $method AND v.destino = $target
                    RETURN v.id AS vehicle_id
                    """
                    
            vehicle = session.run(search_vehicle_query, origin=source, target=target, method=path['modo']).single()
                    
            if vehicle:
                vehicle_id = vehicle["vehicle_id"]
                assign_query = """
                        MATCH (v:Vehiculo {id: $vehicle_id})
                        MATCH (p:Paquete {id: $packageId})
                        SET p.route = $route, p.costs = $costs
                        MERGE (p)-[:ASIGNADO_A]->(v)
                        """
                session.run(assign_query, vehicle_id=vehicle_id, packageId=packageId, route=route, costs=costs)
                print(f"Paquete {packageId} asignado al {path['modo']} existente {vehicle_id}.")
            else:
                     
                create_vehicle_query = """
                        CREATE (v:Vehiculo {id: $vehicle_id, tipo: $method, destino: $target, packages: [$packageId]})
                        WITH v
                        MATCH (c:Ciudad | Plataforma {nombre: $origin}), (p:Paquete {id: $packageId})
                        SET p.route = $route, p.costs = $costs
                        MERGE (p)-[:ASIGNADO_A]->(v)-[:UBICADO_EN]->(c)
                        """
                session.run(create_vehicle_query, vehicle_id=datetime.now().isoformat(), origin=source, method=path['modo'], target=target, packageId=packageId, route=route, costs=costs)
                print(f"Nuevo {path['modo']} creado para el paquete {packageId}.")
                    
    def move_all_vehicles_to_next_location(self):
        """
        Mueve todos los vehículos a la siguiente ciudad en su lista de destinos.
        Saca los paquetes de cada vehículo y los asigna a nuevos vehículos,
        usando el origen del nuevo vehículo como el destino del actual, 
        y el destino según la información del paquete.
        """
        with self.driver.session() as session:
            query = """
            MATCH (v:Vehiculo)-[r:UBICADO_EN]->(c:Ciudad)
            DELETE r
            WITH v
            MATCH (c_destino:Ciudad {nombre: v.destino})  
            MERGE (v)-[:UBICADO_EN]->(c_destino)
            """
            session.run(query)
            self.change_vehicle()

    def change_vehicle(self):
        with self.driver.session() as session:
            query = """
            MATCH (p:Paquete) - [r:ASIGNADO_A] -> (v:Vehiculo)
            DELETE r
            WITH p
            RETURN p
            """
            result = session.run(query)
            
            for record in result:
                package = record["p"]
                packageId = package["id"]
                route = package["route"]
                costs = package["costs"]
                
                if len(route) == 1:
                    # Eliminar el paquete y marcarlo como entregado
                    delete_package_query = """
                    MATCH (p:Paquete {id: $packageId})
                    DETACH DELETE p
                    """
                    session.run(delete_package_query, packageId=packageId)
                    print(f"Paquete {packageId} ha sido entregado en {route[0]}.")
                else:
                    source = route.pop(0)
                    target = route[0]
                    costs.pop(0) #Eliminar el costo anterior
                    next_cost = costs[0]  # Acceder al proximo costo
                    print(f"Enviando paquete {packageId} desde {source} hasta {target}. Costo: {next_cost}")
                    db.search_create_vehicle(source, target, packageId, next_cost, route, costs)
            #Eliminar vehículos que no tengan paquetes asignados
            query = """
            MATCH (v:Vehiculo)
            WHERE NOT (v)<-[:ASIGNADO_A]-(:Paquete)
            DETACH DELETE v
            """
            session.run(query)




if __name__ == "__main__":
    db = Neo4jDatabase(URI, USER, PASSWORD)
    try:
        db.cargar_datos()
        # Algunos ejemplos de paquetes
        paquetes = [
            {"id": "P1", "origen": "Madrid", "destino": "Valencia"},
            {"id": "P2", "origen": "Bilbao", "destino": "Granada"},
            {"id": "P3", "origen": "Zaragoza", "destino": "Madrid"},
            {"id": "P4", "origen": "Valencia", "destino": "Bilbao"},
            {"id": "P5", "origen": "Sevilla", "destino": "Barcelona"},
            {"id": "P6", "origen": "Alicante", "destino": "Madrid"},
            {"id": "P7", "origen": "Granada", "destino": "Sevilla"},
            {"id": "P8", "origen": "Barcelona", "destino": "Madrid"}
        ]
        # Ejemplos de distintos tipos de envios
        for p in range(len(paquetes) // 2):
            db.sendPackageTodayBeforeTomorrow14(paquetes[p]["origen"], paquetes[p]["destino"], paquetes[p]["id"])
        for p in range(len(paquetes) // 2, len(paquetes)):
            db.sendPackageTodayBefore19(paquetes[p]["origen"], paquetes[p]["destino"], paquetes[p]["id"])
        # Mover todos los vehículos a la siguiente ubicación
        db.move_all_vehicles_to_next_location()
        
        
        
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

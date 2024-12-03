from neo4j import GraphDatabase
from datetime import datetime

# Configuración de conexión
URI = "bolt://localhost:7687"  # Protocolo Bolt
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
            for ciudad in ciudades:
                query = """
                    CREATE (c:Ciudad {nombre: $nombre, lat: $lat, lon: $lon})
                """
                session.run(query, nombre=ciudad["nombre"], lat=ciudad["lat"], lon=ciudad["lon"])
        
            conexiones_queries = [
                """
                    MATCH (madrid:Ciudad {nombre: "Madrid"}), (barcelona:Ciudad {nombre: "Barcelona"})
                    MERGE (madrid)-[:CONEXION {modo: "carretera", distancia: 626, duracion: 626*0.6, carga_descarga: 5, costo: 6.26}]->(barcelona)
                    MERGE (madrid)-[:CONEXION {modo: "ferrocarril", distancia: 626, duracion: 626*0.5, carga_descarga: 10, costo: 5.01}]->(barcelona)
                    MERGE (madrid)-[:CONEXION {modo: "aire", distancia: 505, duracion: 505*0.1, carga_descarga: 40, costo: 17.68}]->(barcelona);
                    """,
                    """
                    MATCH (madrid:Ciudad {nombre: "Madrid"}), (sevilla:Ciudad {nombre: "Sevilla"})
                    MERGE (madrid)-[:CONEXION {modo: "carretera", distancia: 538, duracion: 538*0.6, carga_descarga: 5, costo: 5.38}]->(sevilla)
                    MERGE (madrid)-[:CONEXION {modo: "ferrocarril", distancia: 538, duracion: 538*0.5, carga_descarga: 10, costo: 4.3}]->(sevilla)
                    MERGE (madrid)-[:CONEXION {modo: "aire", distancia: 390, duracion: 390*0.1, carga_descarga: 40, costo: 13.65}]->(sevilla);
                    """,
                    """
                    MATCH (madrid:Ciudad {nombre: "Madrid"}), (valencia:Ciudad {nombre: "Valencia"})
                    MERGE (madrid)-[:CONEXION {modo: "carretera", distancia: 352, duracion: 352*0.6, carga_descarga: 5, costo: 3.52}]->(valencia)
                    MERGE (madrid)-[:CONEXION {modo: "ferrocarril", distancia: 352, duracion: 352*0.5, carga_descarga: 10, costo: 2.82}]->(valencia);
                    """,
                    """
                    MATCH (madrid:Ciudad {nombre: "Madrid"}), (alicante:Ciudad {nombre: "Alicante"})
                    MERGE (madrid)-[:CONEXION {modo: "carretera", distancia: 419, duracion: 419*0.6, carga_descarga: 5, costo: 4.19}]->(alicante)
                    MERGE (madrid)-[:CONEXION {modo: "ferrocarril", distancia: 419, duracion: 419*0.5, carga_descarga: 10, costo: 3.35}]->(alicante);
                    """,
                    """
                    MATCH (palma:Ciudad {nombre: "Palma de Mallorca"}), (barcelona:Ciudad {nombre: "Barcelona"})
                    MERGE (palma)-[:CONEXION {modo: "agua", distancia: 250, duracion: 250*1.2, carga_descarga: 20, costo: 0.75}]->(barcelona)
                    MERGE (palma)-[:CONEXION {modo: "aire", distancia: 210, duracion: 210*0.1, carga_descarga: 40, costo: 7.35}]->(barcelona);
                    """,
                    """
                    MATCH (palma:Ciudad {nombre: "Palma de Mallorca"}), (valencia:Ciudad {nombre: "Valencia"})
                    MERGE (palma)-[:CONEXION {modo: "agua", distancia: 260, duracion: 260*1.2, carga_descarga: 20, costo: 0.78}]->(valencia)
                    MERGE (palma)-[:CONEXION {modo: "aire", distancia: 260, duracion: 260*0.1, carga_descarga: 40, costo: 9.1}]->(valencia);
                    """,
                    """
                    MATCH (sevilla:Ciudad {nombre: "Sevilla"}), (granada:Ciudad {nombre: "Granada"})
                    MERGE (sevilla)-[:CONEXION {modo: "carretera", distancia: 250, duracion: 250*0.6, carga_descarga: 5, costo: 2.5}]->(granada);
                    """,
                    """
                    MATCH (sevilla:Ciudad {nombre: "Sevilla"}), (malaga:Ciudad {nombre: "Málaga"})
                    MERGE (sevilla)-[:CONEXION {modo: "carretera", distancia: 205, duracion: 205*0.6, carga_descarga: 5, costo: 2.05}]->(malaga);
                    """,
                    """
                    MATCH (zaragoza:Ciudad {nombre: "Zaragoza"}), (bilbao:Ciudad {nombre: "Bilbao"})
                    MERGE (zaragoza)-[:CONEXION {modo: "carretera", distancia: 309, duracion: 309*0.6, carga_descarga: 5, costo: 3.09}]->(bilbao)
                    MERGE (zaragoza)-[:CONEXION {modo: "ferrocarril", distancia: 309, duracion: 309*0.5, carga_descarga: 10, costo: 2.47}]->(bilbao);
                    """,
                    """
                    MATCH (santander:Ciudad {nombre: "Santander"}), (bilbao:Ciudad {nombre: "Bilbao"})
                    MERGE (santander)-[:CONEXION {modo: "carretera", distancia: 100, duracion: 100*0.6, carga_descarga: 5, costo: 1.0}]->(bilbao);
                    """,
                    """
                    MATCH (santander:Ciudad {nombre: "Santander"}), (valladolid:Ciudad {nombre: "Valladolid"})
                    MERGE (santander)-[:CONEXION {modo: "carretera", distancia: 245, duracion: 245*0.6, carga_descarga: 5, costo: 2.45}]->(valladolid);
                    """,
                    """
                    MATCH (valladolid:Ciudad {nombre: "Valladolid"}), (madrid:Ciudad {nombre: "Madrid"})
                    MERGE (valladolid)-[:CONEXION {modo: "carretera", distancia: 200, duracion: 200*0.6, carga_descarga: 5, costo: 2.0}]->(madrid)
                    MERGE (valladolid)-[:CONEXION {modo: "ferrocarril", distancia: 200, duracion: 200*0.5, carga_descarga: 10, costo: 1.6}]->(madrid);
                    """,
                    """
                    MATCH (valladolid:Ciudad {nombre: "Valladolid"}), (coruna:Ciudad {nombre: "A Coruña"})
                    MERGE (valladolid)-[:CONEXION {modo: "carretera", distancia: 455, duracion: 455*0.6, carga_descarga: 5, costo: 4.55}]->(coruna);
                    """,
                    """
                    MATCH (coruna:Ciudad {nombre: "A Coruña"}), (santander:Ciudad {nombre: "Santander"})
                    MERGE (coruna)-[:CONEXION {modo: "carretera", distancia: 430, duracion: 430*0.6, carga_descarga: 5, costo: 4.3}]->(santander)
                    MERGE (coruna)-[:CONEXION {modo: "aire", distancia: 360, duracion: 360*0.1, carga_descarga: 40, costo: 12.6}]->(santander);
                    """
                ]
            for query in conexiones_queries:
                session.run(query)
            print("Datos cargados correctamente.")
        
    def sendPackage(self, origin, destination, maxTime, packageId):
        with self.driver.session() as session:
            # Query para encontrar el camino más económico entre origin y destination
            query = """
            MATCH path = (start:Ciudad {nombre: $origin})-[:CONEXION*..5]-(end:Ciudad {nombre: $destination})
            WITH path, reduce(totalTime = 0, r IN relationships(path) | totalTime + r.duracion + r.carga_descarga) AS totalTime, 
                 reduce(totalPrice = 0, r IN relationships(path) | totalPrice + r.costo) AS totalPrice
            WHERE totalTime < $maxTime
            RETURN path, totalTime, totalPrice
            ORDER BY totalPrice
            LIMIT 1
            """
            result = session.run(query, origin=origin, destination=destination, maxTime=maxTime)
            for record in result:
                metodos = [r["modo"] for r in record["path"].relationships]
                ciudades = [n["nombre"] for n in record["path"].nodes]
                print(f"El paquete {packageId} se enviará desde {origin} hasta {destination} utilizando los siguientes métodos de transporte: {', '.join(metodos)}.")
                print(f"Las ciudades que se recorrerán son: {', '.join(ciudades)}.")

                # Asignar la lista de ciudades al paquete en un diccionario
                package_route = {
                    'origin': origin,
                    'destination': destination,
                    'route': ciudades,
                    'methods': metodos,
                    'id': packageId
                }

                # Buscar un vehículo disponible para este paquete
                vehicle_query = """
                MATCH (v:Vehiculo)-[:UBICADO_EN]->(c:Ciudad {nombre: $origin})
                WHERE ANY(city IN v.ciudades WHERE city IN $route)
                RETURN v
                LIMIT 1
                """
                vehicle_result = session.run(vehicle_query, origin=origin, route=ciudades)
                vehicle = vehicle_result.single()
                
                if vehicle:
                    vehicle_id = vehicle["v"]["id"]
                    update_query = """
                    MATCH (v:Vehiculo {id: $vehicle_id})
                    SET v.packages = v.packages + [$packageId]
                    """
                    session.run(update_query, vehicle_id=vehicle_id, packageId=packageId)
                    print(f"Paquete {packageId} añadido al vehículo {vehicle_id}.")
                else:
                    self.create_vehicle_with_package(package_route, session)

    def create_vehicle_with_package(self, package_route, session):
        origin = package_route['origin']
        metodos = package_route['methods']
        
        # Crear un vehículo con un solo destino inicial
        query = """
        CREATE (v:Vehiculo {id: $vehicle_id, tipo: $tipo, ubicacion: $origin, 
                              packages: [$package_id], ciudades: $route, resto_vehiculos: $resto_vehiculos})
        WITH v
        MATCH (c:Ciudad {nombre: $origin})
        MERGE (v)-[:UBICADO_EN]->(c)
        """
        vehicle_id = datetime.now().isoformat()
        session.run(query, vehicle_id=vehicle_id, tipo=metodos.pop(0), origin=origin, 
                    package_id=package_route['id'], route=package_route['route'][1:], resto_vehiculos=metodos)
        print(f"Vehículo creado para paquete {package_route['id']} con origen en {origin}.")

    def update_vehicle_location(self, vehicle_id, current_location):
        with self.driver.session() as session:
            query = """
            MATCH (v:Vehiculo {id: $vehicle_id})
            SET v.ubicacion = $current_location, v.timestamp = $timestamp
            """
            session.run(query, vehicle_id=vehicle_id, current_location=current_location, timestamp=datetime.now().isoformat())
            print(f"Ubicación del vehículo {vehicle_id} actualizada a {current_location}.")

    def move_vehicle_to_next_location(self, vehicle_id):
        with self.driver.session() as session:
            query = """
            MATCH (v:Vehiculo {id: $vehicle_id})
            UNWIND v.ciudades AS next_city
            WITH v, next_city
            LIMIT 1
            MATCH (c:Ciudad {nombre: next_city})
            MERGE (v)-[:UBICADO_EN]->(c)
            SET v.ubicacion = next_city,
                v.ciudades = tail(v.ciudades)
            """
            session.run(query, vehicle_id=vehicle_id)
            print(f"Vehículo {vehicle_id} movido a la siguiente ciudad.")
    def move_all_vehicles_to_next_location(self):
        with self.driver.session() as session:
            query = """
            MATCH (v:Vehiculo)
            UNWIND v.ciudades AS next_city
            WITH v, next_city
            LIMIT 1
            MATCH (c:Ciudad {nombre: next_city})
            MERGE (v)-[:UBICADO_EN]->(c)
            SET v.ubicacion = next_city,
                v.ciudades = tail(v.ciudades)
            """
            session.run(query)
            print("Todos los vehículos movidos a la siguiente ciudad.")
            def create_and_destroy_vehicles(self):
                with self.driver.session() as session:
                    # Query to find all vehicles and their packages
                    query = """
                    MATCH (v:Vehiculo)
                    RETURN v.id AS vehicle_id, v.packages AS packages
                    """
                    result = session.run(query)
                    
                    for record in result:
                        vehicle_id = record["vehicle_id"]
                        packages = record["packages"]
                        
                        for package_id in packages:
                            # Find the route and methods for each package
                            package_query = """
                            MATCH (p:Paquete {id: $package_id})
                            RETURN p.route AS route, p.methods AS methods, p.origin AS origin
                            """
                            package_result = session.run(package_query, package_id=package_id)
                            package_record = package_result.single()
                            
                            if package_record:
                                route = package_record["route"]
                                methods = package_record["methods"]
                                origin = package_record["origin"]
                                
                                # Create a new vehicle for each package
                                self.create_vehicle_with_package({
                                    'origin': origin,
                                    'destination': route[-1],
                                    'route': route,
                                    'methods': methods,
                                    'id': package_id
                                }, session)
                        
                        # Delete the old vehicle
                        delete_query = """
                        MATCH (v:Vehiculo {id: $vehicle_id})
                        DETACH DELETE v
                        """
                        session.run(delete_query, vehicle_id=vehicle_id)
                        print(f"Vehículo {vehicle_id} eliminado.")

if __name__ == "__main__":
    db = Neo4jDatabase(URI, USER, PASSWORD)
    try:
        db.cargar_datos()
        db.sendPackage("Madrid", "Palma de Mallorca", 400, "Television")
        paquetes = [
            {"id": "P1", "origen": "Madrid", "destino": "Barcelona", "maxTime": 400},
            {"id": "P2", "origen": "Madrid", "destino": "Sevilla", "maxTime": 400},
            {"id": "P3", "origen": "Madrid", "destino": "Valencia", "maxTime": 400},
            {"id": "P4", "origen": "Madrid", "destino": "Alicante", "maxTime": 400},
            {"id": "P5", "origen": "Palma de Mallorca", "destino": "Barcelona", "maxTime": 400},
            {"id": "P6", "origen": "Palma de Mallorca", "destino": "Valencia", "maxTime": 400},
            {"id": "P7", "origen": "Sevilla", "destino": "Barcelona", "maxTime": 400}
        ]
        for p in paquetes:
            db.sendPackage(p["origen"], p["destino"], p["maxTime"], p["id"])
        db.move_vehicle_to_next_location()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

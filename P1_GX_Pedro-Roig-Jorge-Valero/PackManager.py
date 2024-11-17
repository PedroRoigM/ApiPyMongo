import redis
import threading
import json
import time

# Función para simular el empaquetado de la compra
def package_order(order):
    thread_id = threading.get_ident()
    print(f"Hilo {thread_id}: Inicializando el empaquetado de la orden {order}")
    # Simula la lógica de empaquetado aquí
    
    print(f"Hilo {thread_id}: Orden empaquetada y lista")

# Función para simular encolar las órdenes
def enqueue_orders(cache):
    for i in range(10):
        order = {
            "id": f"Compra-{i+1}",
            "details": f"Detalles de la compra {i+1}",
        }
        # Convertir la compra a JSON y agregarla a la cola
        cache.rpush("empacados", json.dumps(order))
        print(f"Encolando {order['id']}")

# Función de trabajador que maneja el empaquetado de órdenes
def worker_process(cache, duration, startTime):
    while startTime + duration > time.time() or duration == 0:
        # Intentar sacar una compra de la cola (bloqueante) con timeout
        order_data = cache.blpop("empacados", timeout=duration)  # Esperar hasta 60 segundos
        if order_data:
            t = threading.Thread(target=worker_process, args=(cache, 60, time.time()))
            t.start()  # El hilo comienza a trabajar de forma independiente
            order = json.loads(order_data[1])  # Convertir el string a objeto
            package_order(order)  # Procesar el pedido
        else:
            print("Tiempo de espera excedido, terminando el hilo.")
        



# Inicialización y ejecución del gestor de empaquetado
def initPackManager():
    print("Esperando por órdenes...")
    cache = redis.Redis(host='localhost', port=6379, db=0)
    
    # Crear hilo principal (para encolar compras)
    enqueue_orders(cache=cache)

    # Hilo principal que gestionará el procesamiento de pedidos
    worker_process(cache=cache, duration=0, startTime=time.time())

if __name__ == "__main__":
    initPackManager()

import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        # Iniciar conexión con broker de RabbitMQ
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._queue = self._connection.channel()

        # Inicializar cola
        self._queue_name = queue_name
        self._queue.queue_declare(queue=queue_name)
        self._queue.basic_qos(prefetch_count=1)
        self._queue.confirm_delivery()
        self._is_consuming = False
    
    #Comienza a escuchar a la cola e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback, inactivity_timeout=None):

        # Agregar callback
        self._callback_function = on_message_callback

        # Consumir
        try:
            self._is_consuming = True
            for method, properties, body in self._queue.consume(queue=self._queue_name, inactivity_timeout=inactivity_timeout):

                self._internal_on_message_callback(self._queue, method, properties, body)

                if not self._is_consuming:
                    break

        except (pika.exceptions.ChannelClosed, pika.exceptions.AMQPConnectionError):
            self._is_consuming = False
            raise MessageMiddlewareDisconnectedError
        except Exception:
            self._is_consuming = False
            raise MessageMiddlewareMessageError

    # Wrapper de callback para cola
    def _internal_on_message_callback(self, ch, method, properties, body):
        #Crear funciones de ACK y NACK
        ack_function = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack_function = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)

        # Invocar función de callback
        self._callback_function(body, ack_function, nack_function)
	
	#Si se estaba consumiendo desde la cola, se detiene la escucha. Si
	#no se estaba consumiendo de la cola, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if self._is_consuming:
            try:
                self._queue.cancel()
                self._is_consuming = False
            except (pika.exceptions.ChannelClosed, pika.exceptions.AMQPConnectionError):
                raise MessageMiddlewareDisconnectedError
        
    #Envía un mensaje a la cola.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            self._queue.basic_publish(exchange="", routing_key=self._queue_name, body=message)
        except (pika.exceptions.ChannelClosed, pika.exceptions.AMQPConnectionError):
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError

    #Se desconecta de la cola.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self._queue.close()
            self._connection.close()
            self._is_consuming = False
        except Exception:
            raise MessageMiddlewareCloseError

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        # Iniciar conexión
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()

        # Inicializar configuración de exchange
        self._exchange_name = exchange_name
        self._host = host
        self._routing_keys = routing_keys
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type='direct')
        self._channel.confirm_delivery()
        self._is_consuming = False

    #Comienza a escuchar el exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback, inactivity_timeout=None):
        # Guardar función de callback
        self._callback_function = on_message_callback

        # Declarar canal como cola receptora
        result = self._channel.queue_declare(queue='', exclusive=True)
        new_channel_name = result.method.queue

        # Asociar cola a los routing_keys deseados
        for routing_key in self._routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name, queue=new_channel_name, routing_key=routing_key)

        # Consumir
        try:
            self._is_consuming = True
            for method, properties, body in self._channel.consume(queue=new_channel_name, inactivity_timeout=inactivity_timeout):

                self._internal_on_message_callback(self._channel, method, properties, body)

                if not self._is_consuming:
                    break

        except (pika.exceptions.ChannelClosed, pika.exceptions.AMQPConnectionError):
            self._is_consuming = False
            raise MessageMiddlewareDisconnectedError
        except Exception:
            self._is_consuming = False
            raise MessageMiddlewareMessageError
        
    # Wrapper de callback para exchange
    def _internal_on_message_callback(self, ch, method, properties, body):
        #Crear funciones de ACK y NACK
        ack_function = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack_function = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)

        # Invocar función de callback
        self._callback_function(body, ack_function, nack_function)

        
    #Si se estaba consumiendo desde el exchange, se detiene la escucha. Si
    #no se estaba consumiendo del exchange, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if self._is_consuming:
            try:
                self._channel.cancel()
                self._is_consuming = False
            except (pika.exceptions.ChannelClosed, pika.exceptions.AMQPConnectionError):
                raise MessageMiddlewareDisconnectedError
        
    #Envía un mensaje al tópico con el que se inicializó el exchange.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            # Enviar según los routing keys con los que se inicializó
            for routing_key in self._routing_keys:
                self._channel.basic_publish(exchange=self._exchange_name, body=message, routing_key=routing_key)
        except (pika.exceptions.ChannelClosed, pika.exceptions.AMQPConnectionError):
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError

    #Se desconecta del exchange al que estaba conectado.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self._channel.close()
            self._connection.close()
            self._is_consuming = False
        except Exception:
            raise MessageMiddlewareCloseError
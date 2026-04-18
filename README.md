# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

---

# Resolución

Nombre: Bustamante Gonzalo Manuel
DNI: 43.086.565
Padrón: 105.011

---

## Introducción

Se realiza la implementación del Trabajo Práctico de coordinación dado por la materia. Este repositorio es un *fork* del siguiente repositorio: <https://github.com/7574-sistemas-distribuidos/tp-coordinacion>.

El trabajo se realizará en Python ya que el Trabajo Práctico de MOM fue realizado en dicho lenguaje y se busca reutilizar dicho código.

## Suposiciones

A continuación se colocan las suposiciones que se toman para el desarrollo del sistema:

- No se agregan datos adicionales luego de que cada cliente envía todos los datos iniciales.
- No se caen las conexiones entre los componentes (procesos) del sistema.
- Se toman valores de prefetch de RabbitMQ pequeños (del orden de 5) sobretodo para evitar desbalanceo de carga de trabajo en las colas y para que sigan habiendo siempre mensajes disponibles, y en los exchange para que haya siempre un mensaje descargándose, sobre todo en las etapas finales del *pipeline* donde los mensajes pueden ser de gran tamaño.


## Message handler de cliente

Para poder identificar el cliente que envió los mensajes se le definió a cada clase de *MessageHandler* un identificador único. Este identificador se decidió que sería el UUID versión 4 por generar un identificador prácticamente único y porque es fácilmente escalable. Si bien se debe incurrir a un costo de transmisión considerable enviando una gran cantidad de bits esto puede ser en parte solventado utilizando métodos de procesamiento de a grandes cantidades (*batchs*), aunque eso queda por fuera del alcance de este proyecto. Este identificador, que se llamará *identificador del cliente*, se enviará en cada mensaje del message handler del cliente a los *Sum*.


## Sum

### Comunicación entre sumadores

Se identificó como problema al aumentar la cantidad de réplicas que, al enviar que se terminaron los registros de frutas, solo uno de los procesos *Sum* o *sumadores* recibiría el mensaje mientras que los demás no se enterarían de ello. Por eso se creó un exchange de mensajes de control para avisar a las demás instancias de cosas a realizar, como saber que existió un *End of records*. Cada registro de frutas y cantidades es almacenado por identificador y sumado utilizando la interfaz provista.

Una particularidad de los *sumadores* es que utilizan el módulo **threading** para ejecutar un hilo y leer los comandos de los demás *sumadores* cuando lleguen los mismos. Esto no es un problema a pesar del GIL ya que se realizan únicamente operaciones de I/O y la mayor parte del tiempo el hilo permanece bloqueado por esperar un mensaje de control de los demás *sumadores* (que aparecen únicamente al llegar al final de los datos de cada cliente), con lo que la mayor parte del tiempo transcurre en el hilo principal del sumador donde se toman datos y se procesan.

### Elección de agregador

Una vez que se determina el fin de la transmisión de los datos por parte de los clientes, que viene dada por la lectura de tantos *End of records* y la falta de recepción de datos, se envían los resultados parciales a los *Aggregator*. El *Aggregator* a utilizar se decide al aplicarle la operación *mod* al identificador UUID del cliente que viene en los mensajes. Por ejemplo, si el identificador es 10 y hay 3 *Aggregator* se calcula `10 mod 3` que es igual a `1` con lo que el *Aggregator* con ese prefijo es el utilizado. De esta forma todos los *Sum* enviarán los datos del mismo cliente al mismo *Aggregator*. Para cada uno de los *agregadores* se usa un proceso particular en el *sumador* para que el envío de datos sea paralelizable.

Ahora bien, para poder coordinar el envío de los datos de un determinado cliente a un *agregador* entre dos clientes se decide utilizando la comunicación en *exchange* entre los *sumadores* mencionada antes definiendo un orden de envío entre todos los clientes. Como todos los *sumadores* reciben el mismo mensaje una vez que RabbitMQ decide enviarlo a los suscriptores, se puede enviar un mensaje con el cliente que llega al fin de los datos y se envía a todos los *sumadores*, que almacenan dichos clientes por orden de llegada, y como siempre se envían en el mismo orden a todos los *sumadores* se puede obtener un orden entre todos utilizando el orden de recepción de los mensajes.

#### Ejemplo

Por ejemplo, dados dos clientes A y B envían datos a dos *sumadores* S1 y S2:

  1) A envía un EOF que es recibido por S1.
  2) S1 envía el EOF de A a S2 y guarda en una cola que el EOF de A llegó. Si antes estaba vacía, resulta `[A]`.
  3) S2 recibe el EOF de A y lo guarda en una cola, que si estaba vacía es también `[A]`.
  4) B envía un EOF que es recibido por S2.
  5) S2 envía el EOF de B a S1 y guarda en una cola que el EOF de B llegó. La cola resulta siendo `[A, B]`.
  6) S1 recibe el EOF de B y lo guarda en una cola que termina siendo `[A, B]`.

De esta forma se forma un orden "global" entre los clientes a medida que llegan los EOF.

Supóngase que ahora hay 4 clientes y que la cola de orden global resulta `[A, B, C, D]`, hay dos *sumadores* y 2 *agregadores*. Suponiendo que todos los *sumadores* tienen datos de todos los clientes, entonces se deben elegir no solo los *agregadores* a donde enviar los datos de cada cliente, porque cada *agregador* maneja los datos de un solo cliente a la vez, si no que tambien hace falta saber el orden de los clientes de los que se envían los datos a los *agregadores*.

Aprovechando que los *routing keys* de los exchanges de salida son de la forma "{NOMBRE}_{PREFIJO}" y que el prefijo va de 0 a *N-1* (si hay *N* *agregadores*) se hizo lo siguiente: si el orden global entre *sumadores* es `[A, B, C, D]`, siguiendo con el nuevo ejemplo, se calcula un hash que se acota con la función *mod*, como se explicó antes, y se obtiene en este caso un *0* o un *1*. Supóngase que el cálculo resultó en 0 para *A* y *D* y 1 para *B* y *C*, entonces se arman dos procesos en cada *sumador*, para los *agregadores* con prefijos 0 y 1, y se les pasa a la lista `[A, D]` al proceso del agregador con prefijo 0 y `[B, C]` al proceso del agregador con prefijo 1 (notar que el orden entre los elementos se mantiene del orden "global"). Entonces en cada sumador al *agregador* de prefijo 0 se le envían los datos del cliente *A* y luego *D*, mientras que al *agregador* de prefijo 1 se le envían los datos del cliente *B* y luego del cliente *C*.

## Aggregator

Los *Aggregator* o *agregadores* son los encargados de reunir los resultados parciales de los *sumadores* y unirlos entre sí para llegar al resultado total que el cliente necesita. Los agregadores, por lo que se explicó antes, reciben siempre datos del mismo cliente y los unen para poder obtener el resultado de cantidades final. El resultado final se sabe que se obtuvo cuando todos los sumadores enviaron

Puede ocurrir que los *sumadores* decidan enviar al mismo *agregador* que ya estaban enviando y que no se hayan recibido los *EOF* de todos los *sumadores* aún, pero para estos casos el *agregador* guarda los mensajes que envían los *sumadores* de otros clientes a la espera de que llegue el *EOF* *sumador* hasta un límite, en cuyo caso el *agregador* reconoce que se perdió la conexión con un *sumador*.
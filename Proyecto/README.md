Hola a continuación te comparto los pasos para poder ejecutar de forma exitosa. 

*Nota:* Hay que asegurarnos de tener instalado Docker Desktop.
*Nota2:* Hay que tener abierto docker desktop y verificar que tenga el estado Engine Running

* Paso 1: Creamos una carpeta con el nombre que tu gustes, en este caso yo elegi Proyecto y colocar los archivos Dockerfile y requirements.txt que estan en esta carpeta
  
* Paso 2: A continuación abrimos la carpeta en VScode, abrimos una terminal y ejecutamos la siguiente sentencia docker build -t clima_cdmx .
  esta sentencia que acabaos de ejecutar nos creara la imagen, para vereficar que este en orden ejecutemos la siguiente linea docker image ls y deberia aparecer 
  la imagen creada previamente.

* Paso 3: Ejecutamos la siguiente linea curl -O docker-compose.yaml 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yaml' y a continuación
  esto nos creara un archivo .yaml al cual borraremos su contenido y le pegaremos el contenido que tiene el archivo .yaml que esta en esta carpeta
  
* Paso 4: después necesitamos crear tres carpetas, las cuales crearemos ejecutando lo siguiente mkdir -p logs,plugins,dags, una vez hecho esto ejecutaremos la
  sentencia docker-compose up, en caso de que no te abra directamente, escribe localhost:8080/ en tu navegador y ya debarias ver la pagina de airflow que levantamos
  la contraseña y el usuario son airflow, a continuacion realizamos un control + c para interrumpir la ejecucion de la pagina y pasamos al siguiente paso
  
* Paso 5: En la carpeta _dags_ colocaremos el archivo 001A.py el cual contiene nuestro dag y la funcion que ejecuta. volvemos a realizar un docker-compose up
  y ya estara creado nuestro dag.
  
* Paso 6: Para ejecutar el Dag hay que darle en el icono de play y trigger dag, esto comenzara la ejecución. 

*Nota3:* en la funcion creada, hay un parametro que debemos cambiar conforme a la ejecucion que se requiera, estos parametros que se cambian estan en la funcion 
_call_api_ debemos de cambiarlo por las paginas que deseemos cargar puede ser desde cualquier cantidad de hojas. Aunque previamente la tabla que creamos ya esta poblada
con las primeras 106 paginas. ejemplo deseamos cargar las hojas 108 a 110, por lo que deberiamos de ingresar (108,110).





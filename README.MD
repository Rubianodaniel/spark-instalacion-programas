## CONFIGURACION DE SPARK 


-Descargar apache spark
-descargas jdk y jke
-si esta en windows descargar el archivo winutlis.exe desde el repositorio de github
    el archivo winutils ponerlo dentro de la carpeta de spark - crear una carpeta winutils y adentro una carpeta bin y dentro meter el archivo

configuracion de variables de entorno o del sistema 
    SPARK_HOME = Ruta donde quedo instalado spark
    JAVA_HOME = ruta donde quedo instalado java 
    HADOOP_HOME = ruta donde quedo el archivo winutils  

    pruebe abrir una terminal si estan bien las variables de entorno 
    internet escribir spark-shell esto abrira una shell de spark en scala

para correr spark con python desde un entorno virtual debe tener instalado el entorno 
    py -m venv venv
    activar el entorno .\venv\scripts\activate
    crear una variable tempora si esta en windows es $env:PYSPARK_PYTHON = "ruta del interprete de python del entorno virtual"
                                                                            ejemplo "C:\users\user\caperta1\venv\scripts\python.exe"
    ahora puede ejecutar desde la terminal spark-submit rutadelaapp.py

    para inicializar un cluster debe escribir esto en la terminarl 
        #MASTER    spark-class org.apache.spark.deploy.master.Master
        #workers   spark-class org.apache.spark.deploy.worker.Workers <la ruta donde se esta ejecutando el master> -m 8Gb -c 8

        

import os
import logging
from pyspark.sql import SparkSession
import time

BASEDIR = os.getcwd()
folderpath = os.path.join(BASEDIR, "ml-latest-small")

# Configurar nivel de registro de Python a ERROR
logging.getLogger("py4j").setLevel(logging.ERROR)


star  = time.time()


spark = SparkSession.builder.appName("RatingsHistogram").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Cargando los datos de ratings en un DataFrame
#df = spark.read.csv(r"C:\Users\drubianm\Downloads\ml-latest-small\ratings.csv", header=True, inferSchema=True)
rdd = spark.sparkContext.textFile(f'{folderpath}/ratings.csv')
header = rdd.first()


rdd2 = rdd.filter(lambda title : title != header)
rdd3 = rdd2.map(lambda x : (int(x.split(",")[1]),1))
rdd4 = rdd3.reduceByKey(lambda x,y: x+y)

rdd5 = rdd4.map(lambda xy : (xy[1] , xy[0]))
rdd6 = rdd5.sortByKey()

results = rdd6.collect()

for result in results:
    print(result)

fin = time.time()
print(fin-star)


import os
import logging
from pyspark.sql import SparkSession
import time

BASEDIR = os.getcwd()
folderpath = os.path.join(BASEDIR, "ml-latest-small")

# Configurar nivel de registro de Python a ERROR
logging.getLogger("py4j").setLevel(logging.ERROR)

spark = SparkSession.builder.appName("RatingsHistogram").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def load_movie_names(filepath):
    movie_names = {}
    with open(filepath, mode="r", encoding="utf-8") as f:
        next(f)
        for line in f:
            fields = line.strip().split(",")
            movie_names[int(fields[0])] = fields[1]
    return movie_names


if __name__ == '__main__':

    star = time.time()
    # Cargando los datos de ratings en un DataFrame
    #df = spark.read.csv(r"C:\Users\drubianm\Downloads\ml-latest-small\ratings.csv", header=True, inferSchema=True)
    ''' cargando las variables brodcast'''
    filepath = f'{folderpath}/movies.csv'
    movie_names = spark.sparkContext.broadcast(load_movie_names(filepath=filepath))


    '''cargado los datos de ratings'''
    rdd = spark.sparkContext.textFile(f'{folderpath}/ratings.csv')

    header = rdd.first()
    rdd2 = rdd.filter(lambda title : title != header)
    rdd3 = rdd2.map(lambda x : (int(x.split(",")[1]),1))
    rdd4 = rdd3.reduceByKey(lambda x,y: x+y)

    rdd5 = rdd4.map(lambda xy : (xy[1] , xy[0]))
    rdd6 = rdd5.sortByKey()

    rdd7 = rdd6.map(lambda x : (x[0],movie_names.value[x[1]]))

    results = rdd7.collect()

   
    for result in results:
        print(result)

    fin = time.time()
    print(fin-star)
import findspark
findspark.init()
import time
from pyspark import SparkConf, SparkContext
import collections

def parseline(line):
    fields = line.split(",")
    move_id = fields[1]
    rating = fields[2]

    return (rating, move_id)

    


if __name__=='__main__':
    star = time.time()
    conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
    sc = SparkContext(conf=conf)

    ### cargando los datos de ratings en un rdd
    lines = sc.textFile(r"C:\Users\drubianm\Downloads\ml-latest-small\ratings.csv")

    fields = lines.map(parseline)
    total = fields.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (int(x[0])+int(y[0]),int(y[0])+int(y[1])))

    print(total.collect())
    # ratings = lines.map(lambda x:x.split(",")[2])
    # result = ratings.countByValue()


    # sorted_results = collections.OrderedDict(sorted(result.items()))
    # for key, value in sorted_results.items():
    #     print(f'{key}, {value}')


    

    fin = time.time()
    print(fin-star)

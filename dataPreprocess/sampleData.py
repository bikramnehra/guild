from pyspark import SparkContext
import sys

def main(args):
    userFileName = args[1]
    sc = SparkContext(appName="hbaseload")
    rdd = sc.textFile(userFileName).repartition(1000).cache()
    print rdd.count()
    rdd = rdd.takeSample(False,100000)
    rdd = sc.parallelize(rdd)
    rdd.saveAsTextFile(args[2])
    return

if __name__ == '__main__':
    main(sys.argv)

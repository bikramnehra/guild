from pyspark import SparkContext
import sys
from pyspark.sql import SQLContext
from xml.dom.minidom import parseString

def rddParseXml(xmlStr):
    dom = parseString(xmlStr)
    commonSet = set()
    NotCommonSet = set()
    for attr in dom.documentElement.attributes.items():
        commonSet.add(attr[0])
    return (commonSet,NotCommonSet)


def reduceSets((commonSet1,NotCommonSet1),(commonSet2,NotCommonSet2)):
    interSet = (commonSet1 & commonSet2)
    unionSet = (commonSet1 - interSet) | (commonSet2 - interSet)
    return (interSet, unionSet | NotCommonSet1 | NotCommonSet2)




def main(args):
    userFileName = args[1]
    #userFileName = 'sampleUser'

    sc = SparkContext(appName="hbaseload")
    rdd = sc.textFile(userFileName,1000)
    #rdd = sc.textFile(userFileName).takeSample(False,100)
    #rdd = sc.parallelize(rdd)
    #rdd.coalesce(1).saveAsTextFile('sampleUser')
    #return
    mapRdd = rdd.map(lambda l:l.encode('utf-8').lstrip()).\
            filter(lambda l :l.find('<row Id=') != -1).\
            map(rddParseXml)
    print mapRdd.reduce(reduceSets)
    #mapRdd.saveAsTextFile('output')

if __name__ == '__main__':
    main(sys.argv)

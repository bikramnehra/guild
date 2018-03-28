from pyspark import SparkContext
import sys
from pyspark.sql import SQLContext
from xml.dom.minidom import parseString
import json



def rddParseXml(xmlStr):
    dom = parseString(xmlStr)
    return dom.documentElement


def reduceSets((commonSet1,NotCommonSet1),(commonSet2,NotCommonSet2)):
    interSet = (commonSet1 & commonSet2)
    unionSet = (commonSet1 - interSet) | (commonSet2 - interSet)
    return (interSet, unionSet | NotCommonSet1 | NotCommonSet2)


def getConvertedValue(value,conv):
    if conv == "I":
        return int(value)
    return value


def mapUserStructures(xmlObj,userAttrList):
    di ={}
    for item in userAttrList:
        if xmlObj.hasAttribute(item[0]):
            di[item[0]] = getConvertedValue(xmlObj.getAttribute(item[0]),item[1])
        else:
            di[item[0]] = getConvertedValue(item[2],item[1])
    di['Col_Key'] = di['Id']
    di['Col_Family'] = 'Profile_F'
    di['Col_Name'] = 'Profile'
    return di

def preprocessXML(fileName):
    rdd = sc.textFile(fileName)
    #rdd = rdd.repartition(1000)
    #rdd = sc.textFile(fileName)
    return rdd.map(lambda l:l.encode('utf-8').lstrip()).\
            filter(lambda l :len(l) > 9 ).\
            filter(lambda l :l[0:8] == '<row Id=').\
            map(rddParseXml)




def processUser_XML(userFileName,userAttrList,outPutDir):
    mapRdd = preprocessXML(userFileName).\
            map(lambda l:mapUserStructures(l,userAttrList))
    mapRdd.cache()
    if writeFile == True:
        mapRdd.map(lambda l: json.dumps(l)).\
                saveAsTextFile(outPutDir + '/Stack_overflow_User.json' )
    return mapRdd

def createDic(xmlObj):
    di ={}
    for item in xmlObj.attributes.items():
        di[item[0]] = item[1]
    return di

def mapReputationScores(d):
    key = d['PostId']
    result = {}
    result['UpVot'] = 0
    result['DownVot'] = 0
    result['Accepted'] = 0
    if(d['VoteTypeId'] == "2"):
        result['UpVot'] = 1
    elif(d['VoteTypeId'] == "3"):
        result['DownVot'] = 1
    elif(d['VoteTypeId'] == "1"):
        result['DownVot'] = 1
    return (int(key),result)



def removeNonEssentialVotes(d):
   val = d['VoteTypeId'] == "2" or d['VoteTypeId'] == "3" or d['VoteTypeId'] == "1"
   return val


def calculateVoteReputation(d1,d2):
    result = {}
    result['UpVot'] = d1['UpVot'] + d2['UpVot']
    result['DownVot'] = d1['DownVot'] + d2['DownVot']
    result['Accepted'] = d1['Accepted'] + d2['Accepted']
    return result



def processVotes_XML(votesFileName):
    votesRdd = preprocessXML(votesFileName).\
            map(createDic).\
            filter(removeNonEssentialVotes).\
            map(mapReputationScores).\
            reduceByKey(calculateVoteReputation)
    votesRdd.cache()
    return votesRdd


def calculatePostReputation((postid,(pD,vD))):
    score = 0

    if(pD["PostTypeId"] == "1"):
        score = score + 5*vD["UpVot"]
        score = score - 2*vD["DownVot"]
    elif(pD["PostTypeId"] == "2"):
        score = score + 10*vD["UpVot"]
        score = score - 2*vD["DownVot"]
        score = score + 15*vD["Accepted"]

    return (score,int(pD['OwnerUserId']),pD['Tags'])


def mapTagAndUser((score,userId,Tags)):
    result = []
    for word in Tags.split('>'):
        if(len(word) > 1 and word[0] == '<'):
            result.append(((word[1:],userId),score))
    return result



def removeNotTags((id,d)):
    if "Tags" in d and "OwnerUserId" in d:
        return True
    else:
        return False


def retainQuesAns((id,d)):
    if d["PostTypeId"] == "1" or d["PostTypeId"] == "2":
        return True
    return False



def retainAns((id,d)):
    if d["PostTypeId"] == "2" and "ParentId" in d:
        return True
    return False


def retainQues((id,d)):
    if d["PostTypeId"] == "1" and "Tags" in d:
        return True
    return False


def mapQuesTags((id,d)):
    return ((id,d["Tags"]))


def mapAnsTags((id,(d,tags))):
    d["Tags"] = tags
    return (int(d["Id"]),d)


def mapParentId((id,d)):
    return (int(d["ParentId"]),d)



def getAggTagUserScore(score1,score2):
    return score1 + score2


def mapTags(((tag,user),score)):
    arr = []
    arr.append((score,user))
    return (tag,(1,arr))

def mapUsers(((tag,user),score)):
    arr = []
    arr.append((score,tag))
    return (user,arr)


def reduceTopUsers((c1,arr1),(c2,arr2)):
    res =  arr1 + arr2
    res = sorted(res,reverse=True)
    return (c1+c2,res)


def mapToPercentile((key,(c,arr))):
    n = c
    ranks = list(reversed(xrange(0,n)))
    ranks = map(lambda l:l+1,ranks)
    for i in xrange(0,n):
        newScore = (ranks[i] * 100.0)/n
        arr[i] = (newScore,arr[i][1])
    return  (key,(c,arr))

def filterScore((key,(c,arr,Tscore))):
    return Tscore > 0

def normalizeScore((key,(c,arr,Tscore))):
    arr = map(lambda (s,id):(s*10000/Tscore,id),arr)
    return (key,(c,arr,Tscore))


def retainTopN((key,(c,arr))):
    if(len(arr) > 100):
        arr = arr[0:100]
    return (key,(c,arr))


def reverseMapUsers((key,(c,arr))):
    ret = []
    for i in arr:
        ret.append((i[1],[(i[0],key)]))
    return ret


def reduceTagsByUsers(arr1,arr2):
    res = arr1 + arr2
    return res

def mapTopUsersJson((tag,(count,arr))):
    di = {}
    di['Col_Key'] = tag
    di['Col_Family'] = 'Score_F'
    di['Col_Name'] = 'Score'
    di['count'] = count
    di['Users'] = arr
    return di


def mapUsersJson((user,(arr))):
    di = {}
    di['Col_Key'] = user
    di['Col_Family'] = 'Tags_F'
    di['Col_Name'] = 'Tags'
    di['Tags'] = arr
    return di


def processPosts_XML(postsXMlFileName,votesRdd,outputDir):
    postsRdd = preprocessXML(postsXMlFileName).\
            map(createDic).\
            map(lambda l:(int(l["Id"]),l)).\
            filter(retainQuesAns).\
            cache()
#cache bcoz used multiple times
    postsRddQ = postsRdd.filter(retainQues).cache()
#cache bcoz used in a join and union


    postsRddQTags = postsRddQ.map(mapQuesTags).cache()
#cache bcoz used in a join, although not required but a bug observed before where join without cache is slow

    postsRddA  = postsRdd.\
            filter(retainAns).\
            map(mapParentId).\
            join(postsRddQTags).\
            map(mapAnsTags)

    postsRdd = postsRddA.union(postsRddQ).\
            filter(removeNotTags).\
            join(votesRdd).\
            map(calculatePostReputation).\
            flatMap(mapTagAndUser).\
            reduceByKey(getAggTagUserScore).\
            cache()

    topUserInTag = postsRdd.map(mapTags).\
            reduceByKey(reduceTopUsers).\
            map(mapToPercentile).\
            cache()

    topNUserInTag = topUserInTag.map(retainTopN).\
            map(mapTopUsersJson).\
            map(lambda l:json.dumps(l)).cache()

    if writeFile == True:
        topNUserInTag.saveAsTextFile(outputDir + '/TagUserScore')

    usersTag = topUserInTag.flatMap(reverseMapUsers).\
            reduceByKey(reduceTagsByUsers).\
            map(mapUsersJson).\
            map(lambda l:json.dumps(l)).cache()
    if writeFile == True:
        usersTag.saveAsTextFile(outputDir + '/UsersTags')

    return postsRddA



def getSchemaRdds(schemaPath):
    userRdd = sc.textFile(schemaPath + '/' + "UserSchema").\
            map(lambda l: l.split(":"))
    userAttrList = userRdd.collect()
    return userAttrList


def dummyHBaseTest():
    rdd = sc.parallelize(range(1,10)).map(lambda l:(l,"familily1","col",1))
    #conf = #{"hbase.zookeeper.quorum": host,
    conf = {"hbase.mapred.outputtable": 'jivjotTable',
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}

    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    rdd.saveAsNewAPIHadoopDataset(conf=conf,\
            keyConverter=keyConv,\
            valueConverter=valueConv)

    #rdd.toHBaseTable("jivjotTable").\
    #        toColumns("column1", "column2").\
    #        inColumnFamily("mycf").save()

def main(args):
    outPutDir = args[1]
    metaDir = args[2]
    userXMlFileName = args[3]
    votesXMlFileName = args[4]
    postsXMlFileName = args[5]
    userAttrList  = getSchemaRdds(metaDir)
    UserRdd = processUser_XML(userXMlFileName,userAttrList,outPutDir)
    votesRdd = processVotes_XML(votesXMlFileName)
    joinPosts = processPosts_XML(postsXMlFileName,votesRdd,outPutDir)
    print joinPosts.take(100)

if __name__ == '__main__':
    sc = SparkContext(appName="hbaseload")
    writeFile = True
    main(sys.argv)

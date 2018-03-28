userPath=/user/ggbaker/stack-exchange/stackoverflow.com/Users.xml.gz
#set userPath=AllVotes
#set userPath=/user/ggbaker/stack-exchange/stackoverflow.com/Posts.xml.gz
#set userPath=AllPosts
#set outPath=sampleVotes
outPath=AllUsers
echo $outPath
echo $userPath
hadoop fs -rmr $outPath
spark-submit --master yarn-cluster --num-executors=29 repartition.py $userPath $outPath > sampleData.dump

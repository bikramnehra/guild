#spark-submit --master yarn-client hbaseLoad.py /user/ggbaker/stack-exchange/stackoverflow.com
#hadoop fs -rmr output
#set userPath=/user/ggbaker/stack-exchange/stackoverflow.com/Users.xml.gz
set userPath=AllUsers
set votePath=AllVotes
set postPath=AllPosts
echo $userPath
echo $votePath
echo $postPath
#set votePath=/user/ggbaker/stack-exchange/stackoverflow.com/Votes.xml.gz
#set userPath=sampleUser
spark-submit  --master yarn-client --num-executors=29 hbaseLoad.py output2 StackOverflowSchema $userPath $votePath $postPath > hbaseLoad.dump

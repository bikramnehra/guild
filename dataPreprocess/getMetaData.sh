#spark-submit --master yarn-client hbaseLoad.py /user/ggbaker/stack-exchange/stackoverflow.com
#set userPath=/user/ggbaker/stack-exchange/stackoverflow.com/Users.xml.gz
set userPath=/user/ggbaker/stack-exchange/stackoverflow.com/Votes.xml.gz
spark-submit  --master yarn-client getMetaData.py $userPath  > getMetaData.dump

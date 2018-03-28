#spark-submit --master yarn-client hbaseLoad.py /user/ggbaker/stack-exchange/stackoverflow.com
setenv SPARK_CLASSPATH `hadoop classpath`:`hbase classpath`
spark-submit  --master yarn-client testHbase.py 127.0.0.1 jivjotTest 1 Fam1 Col1 1 > hbaseLoad.dump

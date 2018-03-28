Welcome to preprocessing part of the project !!!

Note: All this is one time process to store data into hbase
We have processed and stored the data in tables StackOver_User and StackOver_Tag
If you only wish to run UI please visit UI RUNNING.md


Main file for converting stackoverflow data set into json format is
hbaseLoad.py

it takes 5 arguments

1-> output directory // This needs to exist

2-> Schema of the Users.xml; We needed default values for missing the attributes so we created a schema file with default values so that each attribute has some values.
This was hard coded after analyzing the data. File is located in
dataPreprocess/StackOverflowSchema/UserSchema

3-> Users.xml path

4-> Votes.xml path

5-> Posts.xml path

We used hbaseLoad.sh script to run the job. it contains the default input directory and output path

Output: It writes 3 folders in our output directory

: This contains User profile information, which we wish to show to Users
: This contains All Tags with top 100 score Users in an array
: This contains All Userid with their corresponding tags and scores

All the above files are in json format. We pass these files to Java json writer described below to write the data to Hbase

Each row in above output files are in  json format, where they contain the
following mandatory fields.

Col_Key:String format which is used for a key in hbase table
Col_Family: Column Family for  hbase
Col_Name: Column name for hbase

Due to given above generic format JsonToHbase described below, can convert each one of them to hbase format

___________________________________________________
Java json writer

We have uploaded JsonToHbase.java
which we use to read json format and write to hbase database

parameters:
1-> input filename
2-> Database Table Name

As describe above row key, column Name, column Family is already present in the json data
___________________________________________________
Database schema

DB schema is described dbScript
___________________________________________________
Utilities

repartition.py repartition.sh

As described in project report, we uncompressed and repartitioned the data
in order to speed up the process.

arguments
1-> input filename
2-> output directory

__________________________________________________

sampleData.py sampleData.sh

Its very time consuming to test on such a large dataset so we created a sampling script for testing purposes

arguments
1-> input filename
2-> output directory



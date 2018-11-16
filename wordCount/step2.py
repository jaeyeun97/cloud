import re
from pyspark import SparkContext
from pyspark.sql import SQLContext

text = "Hey, you - what are \"you\" doing here!? test1-test2_test3"
sc = SparkContext("local", "sibal")
sqlc = SQLContext(sc)

#Words
hadoop_conf=sc._jsc.hadoopConfiguration()
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", "AKIAJ6G7DAUEOXWO74QA")
hadoop_conf.set("fs.s3a.awsSecretAccessKey", "BaGy0PVJlD0rc9qk0/H814sExdvmEGDRnvRqFSED")

filtered = sc.textFile("s3a://group-dataset/sample-a.txt").flatMap(lambda x:filter(None, re.split("[, .;:?!\"()\[\]{}\-_]+", x))).filter(lambda x:x.isalpha())

allWords = filtered.map(lambda x:("total", 1)) \
                .reduceByKey(lambda p:p[0]+p[1]) \
                .sortByKey().collect()

sqlc.createDataFrame(allWords, ["AllTotal", "count"]).show()

<<<<<<< HEAD
distinctWords = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda p:1) \
                .sortByKey().collect()
=======
distinctWordsTemp = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda a,b:1) \
                .map(lambda (a,b):("distinctTotal", b)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey()

distinctWordsDict = distinctWordsTemp.collectAsMap()
distinctWords = distinctWordsTemp.collect()
>>>>>>> 8d1713917a5d5cc4c2daa427edd46d03916390fa

sqlc.createDataFrame(distinctWords, ["DistinctTotal", "count"]).show()

wordCount = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda p:p[0]+p[1]) \
                .map(lambda p: (p[1],p[0])) \
                .sortByKey().collect()
sqlc.createDataFrame(wordCount, ["count", "word"]).show()

<<<<<<< HEAD
#Letters
=======
dCount = distinctWordsDict["distinctTotal"]
popular = Math.ceil(dCount * 0.05)
rare = Math.floor(dCount * 0.95)
common_l = Math.floor(dCount * 0.475)
common_u = Math.ceil(dCount * 0.525)
>>>>>>> 8d1713917a5d5cc4c2daa427edd46d03916390fa

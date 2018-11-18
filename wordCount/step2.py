# -- coding: utf-8 --
import re
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext("local", "wordCount")
sqlc = SQLContext(sc)

delimiters = u'[\n\t ,\.;:?!"\(\)\[\]{}\-_]+'
alphabets = u'abcdefghijklmnopqrstuvwxyz'

hadoop_conf=sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", 'AKIAJ6G7DAUEOXWO74QA')
hadoop_conf.set("fs.s3n.awsSecretAccessKey", 'BaGy0PVJlD0rc9qk0/H814sExdvmEGDRnvRqFSED') 

filtered = sc.textFile('s3n://group-dataset/sample-a.txt') \
            .flatMap(lambda x: re.split(delimiters, x)) \
            .map(unicode.lower) \
            .filter(lambda w: len(w) > 0) \
            .filter(lambda w: reduce(lambda x, y: x and y, (c in alphabets for c in w)))

allWordsTemp = filtered.map(lambda x:("total", 1)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey()

allWordsDict = allWordsTemp.collectAsMap()
allWords = allWordsTemp.collect()


sqlc.createDataFrame(allWords, ["AllTotal", "count"]).show()

distinctWordsTemp = filtered.map(lambda x:(x,1)) \
                .reduceByKey(lambda a,b:1) \
                .map(lambda (a,b):("distinctTotal", b)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey()

distinctWordsDict = distinctWordsTemp.collectAsMap()
distinctWords = distinctWordsTemp.collect()

sqlc.createDataFrame(distinctWords, ["DistinctTotal", "count"]).show()

wordCount = filtered.map(lambda x:(x,1)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey() \
                .map(lambda (a,b):(b,a)) \
                .sortByKey() \
                .zipWithIndex()

wordCountDF = wordCount.map(lambda r: Row(rank=int(r[1])+1, word=r[0][1], frequency=r[0][0])).toDF().show()

aCount = allWordsDict["total"]
dCount = distinctWordsDict["distinctTotal"]
popular = math.ceil(dCount * 0.05)
rare = math.floor(dCount * 0.95)
common_l = math.floor(dCount * 0.475)
common_u = math.ceil(dCount * 0.525)

print(aCount)
print(dCount)
print(popular)
print(common_l)
print(common_u)
print(rare)

#Letters


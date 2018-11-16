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

distinctWords = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda p:1) \
                .sortByKey().collect()

sqlc.createDataFrame(distinctWords, ["DistinctTotal", "count"]).show()

wordCount = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda p:p[0]+p[1]) \
                .map(lambda p: (p[1],p[0])) \
                .sortByKey().collect()
sqlc.createDataFrame(wordCount, ["count", "word"]).show()

#Letters

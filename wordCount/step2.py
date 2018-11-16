import re
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext


file = "sample-d"
sc = SparkContext("local", "sibal")
sqlc = SQLContext(sc)

#Words
filtered = sc.textFile(file).flatMap(lambda x:filter(None, re.split("[, .;:?!\"()\[\]{}\-_]+", x))).filter(lambda x:x.isalpha()).filter(lambda x: True if u"U+00A3" not in x else False)

allWords = filtered.map(lambda x:("total", 1)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey().collect()


sqlc.createDataFrame(allWords, ["AllTotal", "count"]).show()

distinctWordsTemp = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda a,b:1) \
                .map(lambda (a,b):("distinctTotal", b)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey()

distinctWordsDict = distinctWordsTemp.collectAsMap()
distinctWords = distinctWordsTemp.collect()

sqlc.createDataFrame(distinctWords, ["DistinctTotal", "count"]).show()

wordCount = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda a,b:a+b) \
                .map(lambda (a,b):(b,a)) \
                .sortByKey().collect()
sqlc.createDataFrame(wordCount, ["count", "word"]).show()

dCount = distinctWordsDict["distinctTotal"]
popular = math.ceil(dCount * 0.05)
rare = math.floor(dCount * 0.95)
common_l = math.floor(dCount * 0.475)
common_u = math.ceil(dCount * 0.525)

#Letters
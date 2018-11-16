import re
from pyspark import SparkContext
from pyspark.sql import SQLContext

text = "Hey, you - what are \"you\" doing here!? test1-test2_test3"
file = "test"
sc = SparkContext("local", "sibal")
sqlc = SQLContext(sc)

#Words
filtered = sc.textFile(file).flatMap(lambda x:filter(None, re.split("[, .;:?!\"()\[\]{}\-_]+", x)))
                .filter(lambda x:x.isalpha())

allWords = filtered.map(lambda x:("total", 1)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey().collect()

sqlc.createDataFrame(allWords, ["AllTotal", "count"]).show()

distinctWords = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda a,b:1) \
                .sortByKey().collect()

sqlc.createDataFrame(distinctWords, ["DistinctTotal", "count"]).show()

wordCount = filtered.map(lambda x:(x.lower(),1)) \
                .reduceByKey(lambda a,b:a+b) \
                .map(lambda (a,b):(b,a)) \
                .sortByKey().collect()
sqlc.createDataFrame(wordCount, ["count", "word"]).show()

#Letters
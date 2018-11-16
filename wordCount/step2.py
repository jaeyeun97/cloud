import re
from pyspark import SparkContext
from pyspark.sql import SQLContext

text = "Hey, you - what are \"you\" doing here!? test1-test2_test3"
file = "test"
sc = SparkContext("local", "TestWordCount")

#Words
out = sc.textFile(file).flatMap(lambda x:re.split("[, .;:?!\"()\[\]{}\-_]+", x)) \
                .map(lambda x:(x,1)) \
                .reduceByKey(lambda a,b:a+b) \
                .map(lambda (a,b):(b,a)) \
                .sortByKey().collect()
SQLContext(sc).createDataFrame(out, ["count", "word"]).show()

#Letters
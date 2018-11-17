# -*- encoding: utf-8 -*-
from pyspark import SparkContext
import re

spark = SparkContext.getOrCreate()

#f = spark.text.read('data/sample-a.txt').rdd

delimiters = u'[\n\t ,\.;:?!"\(\)\[\]{}\-_]+'

def strable(w):
    try:
        s = str(w)
        return True
    except:
        return False


filtered = spark.textFile('data/sample-f.txt') \
            .flatMap(lambda x: re.split(delimiters, x)) \
            .map(unicode.lower) \
            .filter(unicode.isalpha) \
            .filter(strable) \
            .map(lambda w: (w,1))
            # .count()
            # .reduceByKey(lambda x, y: x+y, 2) \
            # .map(lambda p: p[0]) \

#result = spark.textFile('data/sample-f.txt') \
#            .flatMap(lambda x: re.split(delimiters, x)) \
#            .map(str.lower) \
#            .filter(str.isalpha) \
#            .map(lambda w: (w,1)).count()


print(result) 

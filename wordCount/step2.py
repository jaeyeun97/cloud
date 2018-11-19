# -- coding: utf-8 --
import re
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import *

cnx=   {'host': 'group2dbinstance.cxezedslevku.eu-west-2.rds.amazonaws.com',
        'username': 'group2',
        'password': 'group2sibal',
        'db': 'sw777_CloudComputingCoursework'}

file = 'sample-f.txt'
sc = SparkContext("local", "wordCount")
sqlc = SQLContext(sc)

delimiters = u'[\n\t ,\.;:?!"\(\)\[\]{}\-_]+'
alphabets = u'abcdefghijklmnopqrstuvwxyz'

'''
hadoop_conf=sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", 'AKIAJ6G7DAUEOXWO74QA')
hadoop_conf.set("fs.s3n.awsSecretAccessKey", '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') 
'''

#tokenize
filtered = sc.textFile(file) \
            .flatMap(lambda x: re.split(delimiters, x)) \
            .map(unicode.lower) \
            .filter(lambda w: len(w) > 0) \
            .filter(lambda w: reduce(lambda x, y: x and y, (c in alphabets for c in w)))

wordCount = filtered.map(lambda x:(x,1)) \
                .reduceByKey(lambda a,b:a+b) \
                .sortByKey() \
                .map(lambda (a,b):(b,a)) \
                .sortByKey(ascending=False) \
                .zipWithIndex()

wordCountDF = wordCount.map(lambda r: Row(rank=int(r[1])+1, word=r[0][1], frequency=r[0][0])).toDF()
#wordCountDF.show()

aCount = filtered.count()
dCount = wordCount.count()
popular = int(math.ceil(dCount * 0.05))
rare = int(math.floor(dCount * 0.95))
common_l = int(math.floor(dCount * 0.475))
common_u = int(math.ceil(dCount * 0.525))

print('total number of words: ' + str(aCount))
print('total number of distinct words: ' + str(dCount))
print('popular_threshold_word: ' + str(popular))
print('common_threshold_l_word: ' + str(common_l))
print('common_threshold_u_word: ' + str(common_u))
print('rare_threshold_word: ' + str(rare))

def getPopularStr(col):
    return "popular"
def getCommonStr(col):
    return "common"
def getRareStr(col):
    return "rare"

udfCategoryPop = udf(getPopularStr, StringType())
udfCategoryCom = udf(getCommonStr, StringType())
udfCategoryRare = udf(getRareStr, StringType())


DFpopular = wordCountDF.filter(wordCountDF.rank <= popular).withColumn('category', udfCategoryPop('word'))
DFcommon = wordCountDF.filter(wordCountDF.rank <= common_u).filter(wordCountDF.rank >= common_l).withColumn('category', udfCategoryCom('word'))
DFrare = wordCountDF.filter(wordCountDF.rank >= rare).withColumn('category', udfCategoryRare('word'))
DFout = DFpopular.union(DFcommon).union(DFrare)

DFout.select('rank', 'word', 'category', 'frequency').show()

#Write to RDS mySQL DB
DFout.write.format('jdbc').options(
    url = "jdbc:mysql://{0}:{1}/{2}".format(cnx['host'], '3306', cnx['db']),
    driver = "com.mysql.jdbc.Driver",
    dbtable = "words_spark",
    user = cnx['username'],
    password = cnx['password']).mode("overwrite").save()

#Letters
filtered_letters = sc.textFile(file) \
        .flatMap(lambda x: list(x)) \
        .map(unicode.lower) \
        .filter(lambda w: len(w) > 0) \
        .filter(lambda w: reduce(lambda x, y: x and y, (c in alphabets for c in w)))

wordCount_letters = filtered_letters.map(lambda x:(x,1)) \
        .reduceByKey(lambda a,b:a+b) \
        .sortByKey() \
        .map(lambda (a,b):(b,a)) \
        .sortByKey(ascending=False) \
        .zipWithIndex()

wordCountDF_letters = wordCount_letters.map(lambda r: Row(rank=int(r[1])+1, word=r[0][1], frequency=r[0][0])).toDF()

dCount_letters = wordCount_letters.count()
popular_letters = int(math.ceil(dCount_letters * 0.05))
rare_letters = int(math.floor(dCount_letters * 0.95))
common_l_letters = int(math.floor(dCount_letters * 0.475))
common_u_letters = int(math.ceil(dCount_letters * 0.525))

print('total number of distinct letters: ' + str(dCount_letters))
print('popular_threshold_letter: ' + str(popular_letters))
print('common_threshold_l_letter: ' + str(common_l_letters))
print('common_threshold_u_letter: ' + str(common_u_letters))
print('rare_threshold_letter: ' + str(rare_letters))

DFpopularL = wordCountDF_letters.filter(wordCountDF_letters.rank <= popular_letters).withColumn('category', udfCategoryPop('word'))
DFcommonL = wordCountDF_letters.filter(wordCountDF_letters.rank <= common_u_letters).filter(wordCountDF_letters.rank >= common_l_letters).withColumn('category', udfCategoryCom('word'))
DFrareL = wordCountDF_letters.filter(wordCountDF_letters.rank >= rare_letters).withColumn('category', udfCategoryRare('word'))
DFoutL = DFpopularL.union(DFcommonL).union(DFrareL)

DFoutL.select('rank', 'word', 'category', 'frequency').show()

#Write to RDS mySQL DB
DFoutL.write.format('jdbc').options(
    url = "jdbc:mysql://{0}:{1}/{2}".format(cnx['host'], '3306', cnx['db']),
    driver = "com.mysql.jdbc.Driver",
    dbtable = "letters_spark",
    user = cnx['username'],
    password = cnx['password']).mode("overwrite").save()






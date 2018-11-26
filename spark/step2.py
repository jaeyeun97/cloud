import re
import math
import sys
import os
import time
from functools import reduce
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

start_time = time.perf_counter()

cnx = {'host': 'group2dbinstance.cxezedslevku.eu-west-2.rds.amazonaws.com',
       'username': 'group2',
       'password': 'group2sibal',
       'db': 'sw777_CloudComputingCoursework'}

spark = SparkSession.builder.appName("wordCount").getOrCreate()
sc = spark.sparkContext
sqlc = SQLContext(sc)

delimiters = '[\n\t ,\.;:?!"\(\)\[\]{}\-_]+'
alphabets = 'abcdefghijklmnopqrstuvwxyz'

if len(sys.argv) < 2:
    url = 's3a://group-dataset/sample-a.txt'
else:
    url = sys.argv[1]

filtered = sc.textFile(url) \
            .flatMap(lambda x: re.split(delimiters, x)) \
            .map(str.lower) \
            .filter(lambda w: len(w) > 0) \
            .filter(lambda w: reduce(lambda x, y: x and y, (c in alphabets for c in w)))

wordCount = filtered.map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .sortByKey() \
                .map(lambda p: (p[1], p[0])) \
                .sortByKey(ascending=False) \
                .zipWithIndex()

wordCountDF = wordCount.map(lambda r: Row(rank=int(r[1])+1, word=r[0][1], frequency=r[0][0])).toDF()

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

# Write to RDS mySQL DB
DFout.write.format('jdbc').options(
    url="jdbc:mysql://{0}:{1}/{2}".format(cnx['host'], '3306', cnx['db']),
    driver="com.mysql.jdbc.Driver",
    dbtable="words_spark",
    user=cnx['username'],
    password=cnx['password']).mode("overwrite").save()

# Letters
filtered_letters = sc.textFile(url) \
        .flatMap(lambda x: list(x)) \
        .map(str.lower) \
        .filter(lambda w: len(w) > 0) \
        .filter(lambda w: reduce(lambda x, y: x and y, (c in alphabets for c in w)))

wordCount_letters = filtered_letters.map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortByKey() \
        .map(lambda p: (p[1], p[0])) \
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

# Write to RDS mySQL DB
DFoutL.write.format('jdbc').options(
    url="jdbc:mysql://{0}:{1}/{2}".format(cnx['host'], '3306', cnx['db']),
    driver="com.mysql.jdbc.Driver",
    dbtable="letters_spark",
    user=cnx['username'],
    password=cnx['password']).mode("overwrite").save()

#execution time
elapsed_time = time.perf_counter() - start_time
execNum = int(sc.getConf().get(u'spark.executor.instances')) + 1
filename = os.path.basename(sys.argv[1])
match = re.match(r'data-(.*)MB.txt', filename)
if match:
    size = int(match.group(1))
    row = Row(application='spark', nodes=execNum, data=size, execution_time=round(elapsed_time))
    df_exp = spark.createDataFrame([row])
    df_exp.write.format('jdbc').options(
        url="jdbc:mysql://{0}:{1}/{2}".format(cnx['host'], '3306', cnx['db']),
        driver="com.mysql.jdbc.Driver",
        dbtable="step4_performance_results",
        user=cnx['username'],
        password=cnx['password']).mode("append").save()

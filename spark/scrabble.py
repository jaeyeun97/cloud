# -- coding: utf-8 -
import re
#from pyspark import SparkContext
#from pyspark.sql import SQLContext


filename = "data/sample-f.txt"
file = open(filename, "r", encoding='UTF8')

letters = list()
alphabets = "abcdefghijklmnopqrstuvwxyz"
line = file.readline()
while(line):
        for l in line:
                if( l not in alphabets):
                        letters.append(l)
        line=file.readline()


#sc = SparkContext("local", "TestWordCount")

#Words
#out = sc.textFile(file).flatMap(lambda x:re.split("[, .;:?!\"()\[\]{}\-_]+", x)).filter(lambda x:x.isalpha()).map(lambda x:(x.lower,1)).reduceByKey(lambda a,b:a+b).map(lambda (a,b):(b,a)).sortByKey().collect()
#SQLContext(sc).createDataFrame(out, ["count", "word"]).show()





def alphabetical(x):
        alphabets = "abcdefghijklmnopqrstuvwxyz"
        if(not x.isalpha):
                return False
        for c in x.lower():
                if(c not in alphabets):
                        return False
        return True

print(alphabetical('æ'))
	
#print(filter(None, re.split("[, .;:?!\"()\[\]{}\-_]+", text)))

#x = "asdf@%&*/\ ^'#><~=+asdf"

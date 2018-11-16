import re
#from pyspark import SparkContext
#from pyspark.sql import SQLContext

text = "Hey, you - what are \"you\" doing here!? test1-test2_test3(test4)[test5]{test6}"
file = "test"
#sc = SparkContext("local", "TestWordCount")

#Words
#out = sc.textFile(file).flatMap(lambda x:re.split("[, .;:?!\"()\[\]{}\-_]+", x)).filter(lambda x:x.isalpha()).map(lambda x:(x.lower,1)).reduceByKey(lambda a,b:a+b).map(lambda (a,b):(b,a)).sortByKey().collect()
#SQLContext(sc).createDataFrame(out, ["count", "word"]).show()



def alphabetical(x):
	if(x.isalpha()):
		return True
	else:
		return False
	
print(re.split("[, .;:?!\"()\[\]{}\-_]+", text))

x = "asdf£asdf";
if(x.isalpha):
	print("y")
else:
	print("n")
#Letters
import re
#from pyspark import SparkContext

text = "Hey, you - what are \"you\" doing here!? test1-test2_test3"
file = "sample-a"
#sc = SparkContext("local", "TestWordCount")

#Words
#out = sc.textFile(file).flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).map(lambda (a,b):(b,a)).sortByKey().collect()
#SQLContext(sc).createDataFrame(out, ["count", "word"]).show()

#, .;:?!\"()[]{}_
def splitter(x):
	return re.split("[ ,.;:?!\"()\[\]{}\-_]+", x)

test = splitter(text)

print(test)
#Letters
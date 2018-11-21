import re
import boto3
from boto3.session import Session
from functools import reduce


AWS_ACCESS_KEY_ID = 'AKIAJ6G7DAUEOXWO74QA'
AWS_SECRET_ACCESS_KEY = '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'
bucket_name = 'group-dataset'

def mapper(id, partitionNum, input, bucket):	#returns string[] outputNames
	#create temp files according to partitionNum
	files = list()
	for i in range(0, partitionNum):
		f = open("map_{0}_{1}.txt".format(id, i), 'w+')
		files.append(f)
	
	#Connect to s3 and get input
	session = Session(aws_access_key_id=AWS_ACCESS_KEY_ID, 
			aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
			region_name = 'eu-west-2')
	s3 = session.resource('s3')
	bucket = s3.Bucket(bucket_name)
	scanner = s3.Object(bucket_name, input).get()['Body']._raw_stream
	line = scanner.readline()
	
	#for tokenization
	delimiters = u'[\n\t ,\.;:?!"\(\)\[\]{}\-_]+'
	alphabets = u'abcdefghijklmnopqrstuvwxyz'
	
	#read input line by line
	while( line != ""):
		#tokenize
		tokens = filter(lambda w: reduce(lambda x,y: x and y, (c in alphabets for c in w)),  
								filter(lambda x:len(x)>0, 
								map(lambda x:x.lower(), 
								re.split(delimiters, line))))
								
		#write tokens as (token, 1) to corresponding file
		for token in tokens:
			partition = hash(token) % partitionNum
			files[partition].write("{0}\t1\n".format(token))
			
		#read next line
		line = scanner.readline()
	
	#close files and upload to s3
	outputNames = list()
	for i in range(0, partitionNum):
		files[i].close()
		fname = "map_{0}_{1}.txt".format(id, i)
		bucket.upload_file(fname, fname)
		outputNames.append(fname)

	return outputNames


mapper(123, 3, 'sample-a.txt', bucket_name)
print("done")

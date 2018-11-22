import re
import boto3
from boto3.session import Session
from functools import reduce


AWS_ACCESS_KEY_ID = 'AKIAJ6G7DAUEOXWO74QA'
AWS_SECRET_ACCESS_KEY = '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'
bucket_name = 'group2-custom'

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
	
	print("id {0} finished map".format(id))
	return outputNames

def reducer(id, partition, bucket):
	#get input files
	session = Session(aws_access_key_id=AWS_ACCESS_KEY_ID, 
			aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
			region_name = 'eu-west-2')
	s3 = session.resource('s3')
	bucket = s3.Bucket(bucket_name)
	allFiles = map(lambda x:x.key, bucket.objects.all())
	needFiles = filter(lambda x : re.split('[_.]', x)[0] == 'map' and int(re.split('[_.]', x)[2]) == partition, allFiles)
		
	#put contents into a dictionary
	kv = {}
	temp = open('temp.txt', 'w+')
	
	#merge files to sortedF.txt
	for f in needFiles:
		temp.write(s3.Object(bucket_name, f).get()['Body'].read())

	#sort local file
	temp.seek(0)
	temp_sorted = sorted(temp)
	sortedF = open('sorted.txt', 'w+')
	sortedF.writelines(temp_sorted)
	temp.close()
	sortedF.seek(0)
	
	#reduce
	word = ""
	count = 0
	output = open('output.txt', 'w')
	for line in sortedF:
		kv = line.split('\t')
		if word == kv[0]:
			count += 1
		else:
			if(word != ""):
				output.write('{0}\t{1}\n'.format(word, count))
			word = kv[0]
			count = 1
	output.write('{0}\t{1}\n'.format(word, count)) #last line
	output.close()
	
	#upload file to  s3
	fname = 'reduce_{0}_{1}.txt'.format(id, partition)
	bucket.upload_file('output.txt', fname)
	
	print("id {0} finished reduce".format(id))
	return ""

mapper(123, 3, 'sample-a.txt', bucket_name)
reducer(123, 0, bucket_name)

print("done")

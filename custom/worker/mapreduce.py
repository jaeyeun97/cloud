import re
import boto3
import os
import socket
from boto3.session import Session
from functools import reduce

#import from environment
AWS_ACCESS_KEY_ID = 'AKIAJ6G7DAUEOXWO74QA'
AWS_SECRET_ACCESS_KEY = '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'
id = 123
bucket_name = 'group2-custom'
host_service = 'asdf'
port_service = '1234'

def mapper(id, input, partitionNum, bucket):	#returns string[] outputNames
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

def reducer(id, partition, bucket): #returns string output file name
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
	return fname

'''
worker: worker x init
master: map <chunk_name>  <num_partition>
worker: worker x doing map <chunk_name> <num_partition>
worker: worker x done map <chunk_name> <num_partition>
master: reduce <partition_num>
worker: worker x doing reduce <partition_num>
worker: worker x done reduce <partition_num>
master: kill worker x
'''
if __name__ == '__main__':
	s = socket.socket()
	s.connect(host_service, int(port_service))
	
	while True:
		#say I'm ready
		s.send("worker {0} init".format(id))
		#wait for job
		job = s.recv(4096).decode('utf-8')
		jobToken = job.split(' ')
		if jobToken[0] == 'map':
			s.send("worker {0} doing map {1} {2}".format(id, jobToken[1], jobToken[2]))
			mapper(id,  jobToken[1], jobToken[2], bucket_name)
			s.send("worker {0} done map {1} {2}".format(id, jobToken[1], jobToken[2]))
		elif jobToken[0] == 'reduce':
			s.send("worker {0} doing reduce {1}".format(id, jobToken[1])
			reducer(id, jobToken[1], bucket_name)
			s.send("worker {0} done reduce {1}".format(id, jobToken[1])
		elif jobToken[0] == 'kill':
			break
		else:
			err = "Error: some communication error took place"
			print(err)
			#maybe send error to master?

print("done, I can kill myself!")

import re
import os
import socket
import time
import boto3
import traceback
from boto3.session import Session
from functools import reduce
from smart_open import smart_open

log = open('log.txt', 'w+')

# import from environment
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
region = os.environ['AWS_DEFAULT_REGION']
bucket_name = os.environ['BUCKET_NAME']
id = os.environ['WORKER_NUM']
host_service = os.environ['GROUP2_CUSTOM_MASTER_SERVICE_HOST']
port_service = os.environ['GROUP2_CUSTOM_MASTER_SERVICE_PORT']

# Connect to s3 and get input
session = Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  region_name='eu-west-2')
s3 = session.resource('s3')
bucket = s3.Bucket(bucket_name)

# for tokenization
delimiters = u'[\n\t ,\.;:?!"\(\)\[\]{}\-_]+'
alphabets = u'abcdefghijklmnopqrstuvwxyz'


def word_mapper(id, input, partitionNum):  # returns string[] outputNames
    log.write("Started WordMap on chunk {} for {} partitions\n".format(input, partitionNum))

    chunk = int(input.split(':')[1])

    # create temp files according to partitionNum
    files = list()
    for i in range(0, partitionNum):
        f = open("word_map_{0}_{1}.txt".format(chunk, i), 'w+')
        files.append(f)

    scanner = s3.Object(bucket_name, input).get()['Body'].iter_lines()

    # read input line by line
    for line in scanner:
        # tokenize
        tokens = filter(lambda w: reduce(lambda x, y: x and y, (c in alphabets for c in w)),
                        filter(lambda x: len(x) > 0,
                               map(lambda x: x.lower(),
                                   re.split(delimiters, line.decode('utf-8')))))

        # write tokens as (token, 1) to corresponding file
        for token in tokens:
            partition = hash(token) % partitionNum
            files[partition].write("{0}\t1\n".format(token))

    # close files and upload to s3
    outputNames = list()
    for i in range(partitionNum):
        files[i].close()
        fname = "word_map_{0}_{1}.txt".format(chunk, i)
        bucket.upload_file(fname, fname)
        outputNames.append(fname)
        #remove local file
        os.remove(fname)

    log.write("finished word_map\n")
    return outputNames


def word_reducer(id, partition):  # returns string output file name
    log.write("Started WordReduce on partition {}\n".format(partition))
    # get input files

    allFiles = (x.key for x in bucket.objects.all())

    def splitter(x):
        splits = re.split('[_.]', x)
        return splits[0] == 'word' and splits[1] == 'map' and int(splits[3]) == partition

    needFiles = filter(splitter, allFiles)

    # Dictionary for reduce
    outDict = dict()  # word : count

    # iterate through all files in a partition
    for file in needFiles:
        log.write("Opened file: {}\n".format(file))
        scanner = s3.Object(bucket_name, file).get()['Body'].iter_lines()
        for line in scanner:
            kv = line.decode('utf-8').rstrip().split('\t')
            log.write('{}\t{}\n'.format(kv[0], kv[1]))
            if kv[0] not in outDict:
                outDict[kv[0]] = 1
            else:
                outDict[kv[0]] = outDict[kv[0]] + 1

    key = 'word_reduce_{0}_{1}.txt'.format(id, partition)
    with smart_open('s3://{}/{}'.format(bucket_name, key), 'w', s3_session=session) as f:
        for k, v in outDict.items():
            f.write("{}\t{}\n".format(k, v))

    '''
    # put contents into a file
    temp = open('temp.txt', 'w+')

    # merge files to sortedF.txt
    for f in needFiles:
        temp.write(s3.Object(bucket_name, f).get()['Body'].read().decode('utf-8'))

    # sort local file
    temp.seek(0)
    temp_sorted = sorted(temp)
    sortedF = open('sorted.txt', 'w+')
    sortedF.writelines(temp_sorted)
    temp.close()
    sortedF.seek(0)

    # reduce
    word = ""
    count = 0
    output = open('output.txt', 'w')
    for line in sortedF:
        kv = line.split('\t')
        if word == kv[0]:
            count += 1
        else:
            if word != "":
                output.write('{0}\t{1}\n'.format(word, count))
            word = kv[0]
            count = 1
    if word != "":
        output.write('{0}\t{1}\n'.format(word, count))  # last line
    output.close()
    sortedF.close()
    '''

    # upload file to  s3
    # fname = 'word_reduce_{0}_{1}.txt'.format(id, partition)
    # bucket.upload_file('output.txt', fname)
    log.write("finished word_reduce\n")
    return key


def letter_mapper(id, input, partitionNum):  # returns string[] outputNames
    log.write("Started LetterMap on chunk {} for {} partitions\n".format(input, partitionNum))

    chunk = int(input.split(':')[1])
    # create temp files according to partitionNum
    files = list()
    for i in range(0, partitionNum):
        f = open("letter_map_{0}_{1}.txt".format(chunk, i), 'w+')
        files.append(f)

    scanner = s3.Object(bucket_name, input).get()['Body'].iter_lines()

    # read input line by line
    for line in scanner:
        # tokenize
        tokens = filter(lambda w: reduce(lambda x, y: x and y, (c in alphabets for c in w)),
                        filter(lambda x: len(x) > 0,
                               list(line.decode('utf-8').lower())))

        # write tokens as (token, 1) to corresponding file
        for token in tokens:
            partition = hash(token) % partitionNum
            files[partition].write("{0}\t1\n".format(token))

    # close files and upload to s3
    outputNames = list()
    for i in range(0, partitionNum):
        files[i].close()
        fname = "letter_map_{0}_{1}.txt".format(chunk, i)
        bucket.upload_file(fname, fname)
        outputNames.append(fname)
        #remove local file
        os.remove(fname)

    log.write("finished letter_map\n")
    return outputNames


def letter_reducer(id, partition):  # returns string output file name
    log.write("Started LetterReduce on partition {}\n".format(partition))
    # get input files

    allFiles = map(lambda x: x.key, bucket.objects.all())

    def splitter(x):
        splits = re.split('[_.]', x)
        return splits[0] == 'letter' and splits[1] == 'map' and int(splits[3]) == partition
    needFiles = filter(splitter, allFiles)

    # Dictionary for reduce
    outDict = {}  # word : count

    # iterate through all files in a partition
    for file in needFiles:
        log.write("Opened file: {}\n".format(file))
        scanner = s3.Object(bucket_name, file).get()['Body'].iter_lines()
        for line in scanner:
            kv = line.decode('utf-8').rstrip().split('\t')
            log.write('{}\t{}\n'.format(kv[0], kv[1]))
            if kv[0] not in outDict:
                outDict[kv[0]] = 1
            else:
                outDict[kv[0]] = outDict[kv[0]] + 1

    key = 'letter_reduce_{0}_{1}.txt'.format(id, partition)
    with smart_open('s3://{}/{}'.format(bucket_name, key), 'w', s3_session=session) as f:
        for k, v in outDict.items():
            f.write("{}\t{}\n".format(k, v))

    '''
    # put contents into a file
    temp = open('temp.txt', 'w+')

    # merge files to sortedF.txt
    for f in needFiles:
        temp.write(s3.Object(bucket_name, f).get()['Body'].read().decode('utf-8'))

    # sort local file
    temp.seek(0)
    temp_sorted = sorted(temp)
    sortedF = open('sorted.txt', 'w+')
    sortedF.writelines(temp_sorted)
    temp.close()
    sortedF.seek(0)

    # reduce
    word = ""
    count = 0
    output = open('output.txt', 'w')
    for line in sortedF:
        kv = line.split('\t')
        if word == kv[0]:
            count += 1
        else:
            if word != "":
                output.write('{0}\t{1}\n'.format(word, count))
            word = kv[0]
            count = 1
    if word != "":
        output.write('{0}\t{1}\n'.format(word, count))  # last line
    output.close()
    sortedF.close()


    # upload file to  s3
    fname = 'letter_reduce_{0}_{1}.txt'.format(id, partition)
    bucket.upload_file('output.txt', fname)
    '''

    log.write("finished letter_reduce\n")
    return key


"""
worker: worker x init
master: map <chunk_name>  <num_partition>
worker: worker x doing map <chunk_name> <num_partition>
worker: worker x done map <chunk_name> <num_partition>
master: reduce <partition_num>
worker: worker x doing reduce <partition_num>
worker: worker x done reduce <partition_num>
master: kill worker x
"""
if __name__ == '__main__':
    toSend = "worker {0} ready".format(id)
    while True:
        s = socket.socket()
        try:
            s.connect(((host_service, int(port_service))))
            # say I'm ready
            log.write("sending : {}\n".format(toSend))
            s.send(toSend.encode('utf-8'))
            # wait for job
            job = s.recv(4096).decode('utf-8')
            # log.write("Received: {}".format(job))
            if len(job) == 0:
                time.sleep(1)
                toSend = "worker {0} ready".format(id)
                continue
            jobToken = job.split(' ')
            if jobToken[0] == 'mapWord':
                word_mapper(id,  jobToken[1], int(jobToken[2]))
                toSend = "worker {0} done mapWord {1} {2}".format(id, jobToken[1], jobToken[2])
            elif jobToken[0] == 'reduceWord':
                word_reducer(id, int(jobToken[1]))
                toSend = "worker {0} done reduceWord {1}".format(id, jobToken[1])
            elif jobToken[0] == 'mapLetter':
                letter_mapper(id,  jobToken[1], int(jobToken[2]))
                toSend = "worker {0} done mapLetter {1} {2}".format(id, jobToken[1], jobToken[2])
            elif jobToken[0] == 'reduceLetter':
                letter_reducer(id, int(jobToken[1]))
                toSend = "worker {0} done reduceLetter {1}".format(id, jobToken[1])
            elif jobToken[0] == 'kill':
                break
            else:
                err = "Error: first word of message was not map/reduce/kill."
                log.write("{}\n".format(job))
                log.write("{}\n".format(err))
                # maybe send error to master?
                break
            log.flush()
        except Exception:
            traceback.print_exc()
            traceback.print_exc(file=log)
            log.flush()
        finally:
            s.close()

    with smart_open('log.txt', 'rb') as remote_log:
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        s3.put_object(Body=remote_log.read(), Bucket=bucket_name, Key='log_worker_{}.txt'.format(id))
    log.write("done, I can kill myself!\n")
    log.close()

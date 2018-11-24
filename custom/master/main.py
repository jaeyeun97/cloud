import yaml
import boto3
import re
import math
import os
import socket
from boto3.session import Session
from kubernetes import client, config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from functools import reduce

engine = create_engine('mysql+pymysql://group2:group2sibal@group2dbinstance.cxezedslevku.eu-west-2.rds.amazonaws.com/sw777_CloudComputingCoursework')
SQLSession = sessionmaker(bind=engine)
Base = declarative_base()


class Word(Base):
    __tablename__ = 'words_custom'
    rank = Column(Integer, primary_key=True)
    word = Column(String(40))
    category = Column(String(40))
    frequency = Column(Integer)

    def __init__(self, rank, word, category, frequency):
        self.rank = rank
        self.word = word,
        self.category = category
        self.frequency = frequency


class Letter(Base):
    __tablename__ = 'letters_custom'
    rank = Column(Integer, primary_key=True)
    letter = Column(String(5))
    category = Column(String(40))
    frequency = Column(Integer)

    def __init__(self, rank, letter, category, frequency):
        self.rank = rank
        self.letter = letter,
        self.category = category
        self.frequency = frequency


Base.metadata.create_all(engine)

key = os.environ['AWS_ACCESS_KEY_ID']
secret = os.environ['AWS_SECRET_ACCESS_KEY']
region = os.environ['AWS_DEFAULT_REGION']
bucket_name = os.environ['BUCKET_NAME']
file_name = os.environ['FILE_NAME']
chunk_size = int(os.environ['CHUNK_SIZE'])
worker_count = int(os.environ['WORKER_COUNT'])
worker_image = os.environ['WORKER_IMAGE']
partition_num = 2 * worker_count
newline = '\n'.encode()

config.load_incluster_config()
api = client.CoreV1Api()

# chunk/partition: 'unassigned', 'doing', 'done'
mapWordStat = dict()
reduceWordStat = dict((i, 'unassigned') for i in range(partition_num))
mapLetterStat = dict()
reduceLetterStat = dict((i, 'unassigned') for i in range(partition_num))
# woker number : {'mapWord', 'reduceWord', 'mapLetter', 'reduceLetter', 'idle', 'dead'}
workerStat = dict()


# get file, chunk it up, upload to s3
def chunk():
    s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret)
    body = s3.get_object(Bucket=bucket_name, Key=file_name)['Body']

    # the character that we'll split the data with (bytes, not string)
    partial_chunk = b''
    chunk_count = 0
    while True:
        new_read = body.read(chunk_size*1024)
        if len(new_read) == 0 and len(partial_chunk) == 0:
            break
        chunk = partial_chunk + new_read
        last_newline = chunk.rfind(newline)
        result = chunk[0:last_newline+1]
        print("Chunk Count {}".format(chunk_count))
        print("Result {}".format(result.decode('utf-8')))
        chunk_name = "{}:{}".format(file_name, chunk_count)
        s3.put_object(Body=result, Bucket=bucket_name, Key=chunk_name)
        mapWordStat[chunk_name] = 'unassigned'
        mapLetterStat[chunk_name] = 'unassigned'
        if len(new_read) == 0:
            break
        else:
            partial_chunk = chunk[last_newline+1:]
            chunk_count += 1
    return chunk_count


def spawnWorkers():
    with open('worker.yaml', 'r') as f:
        conf_str = f.read()
    for i in range(worker_count):
        conf = yaml.load(conf_str.format(**{
            'aws_access_key_id': key,
            'aws_secret_access_key': secret,
            'bucket_name': bucket_name,
            'file_name': file_name,
            'worker_image': worker_image,
            'worker_num': i+1
        }).format())
        resp = api.create_namespaced_pod(body=conf, namespace="default")


def getWorkers():
    resp = api.list_namespaced_pod(namespace='default', label_selector='group2-custom-worker')
    return resp.items


# infinitely communicate until job over
def communicate(s):
    while True:
        conn, addr = s.accept()
        try:
            r = conn.recv(4096).decode('utf-8').split(' ')
            if r[0] != 'worker':
                continue
            worker_num = int(r[1])
            status = r[2]
            args = r[3:]
            if status == 'init':
                print("Received worker {} INIT, set workerStat to idle".format(worker_num))
                workerStat[worker_num] = 'idle'
                # workerStat will need to be locked as well for worker entering/leaving feature
            elif status == 'done':
                print("Received worker {} DOING {}".format(worker_num, func_name))
                func_name = args[0]
                workerStat[worker_num] = 'idle'
                if func_name == 'mapWord':
                    chunk_count = int(args[1])
                    mapWordStat[chunk_count] = 'done'
                elif func_name == 'reduceWord':
                    partition_count = int(args[1])
                    reduceWordStat[partition_count] = 'done'
                elif func_name == 'mapLetter':
                    chunk_count = int(args[1])
                    mapLetterStat[chunk_count] = 'done'
                elif func_name == 'reduceLetter':
                    partition_count = int(args[1])
                    reduceLetterStat[partition_count] = 'done'
            if workerStat[worker_num] == 'idle':
                # assign job or kill
                if 'unassigned' in mapWordStat.values():
                    for k, v in mapWordStat.items():
                        if v == 'unassigned':
                            # give map job
                            conn.send("mapWord {} {}".format(k, partition_num).encode('utf-8'))
                            mapWordStat[k] = 'doing'
                            print("assigned mapWord for chunk {} to {}".format(k, worker_num))
                            break
                elif 'doing' not in mapWordStat.values() and 'unassigned' in reduceWordStat.values():
                    for k, v in reduceWordStat.items():
                        if v == 'unassigned':
                            # give reduce job
                            conn.send("reduceWord {}".format(k).encode('utf-8'))
                            reduceWordStat[k] = 'doing'
                            print("assigned reduceWord for partition {} to {}".format(k, worker_num))
                            break
                elif 'unassigned' in mapLetterStat.values():
                    for k, v in mapLetterStat.items():
                        if v == 'unassigned':
                            # give map job
                            conn.send("mapLetter {} {}".format(k, partition_num).encode('utf-8'))
                            mapLetterStat[k] = 'doing'
                            print("assigned mapLetter for chunk {} to {}".format(k, worker_num))
                            break
                elif 'doing' not in mapLetterStat.values() and 'unassigned' in reduceLetterStat.values():
                    for k, v in reduceLetterStat.items():
                        if v == 'unassigned':
                            # give reduce job
                            conn.send("reduceLetter {}".format(k).encode('utf-8'))
                            reduceLetterStat[k] = 'doing'
                            print("assigned reduceLetter for partition {} to {}".format(k, worker_num))
                            break
                else:
                    if reduce(lambda x, y: x and y, (v == 'done' for v in reduceWordStat.values())) \
                            and reduce(lambda x, y: x and y, (v == 'done' for v in reduceLetterStat.values())):
                        conn.send("kill worker {}".format(worker_num).encode('utf-8'))
                        workerStat[worker_num] == 'dead'
                        print("just killed worker {}".format(worker_num))
                        break
        except Exception:
            print('Something gone wrong')
        finally:
            conn.close()
            if reduce(lambda x, y: x and y, (v == 'dead' for v in workerStat.values())):
                break
            print(workerStat)


def initSocket():
    s = socket.socket()
    host = socket.gethostname()
    port = 8000
    s.bind((host, port))
    s.listen(20)
    return s


# combine results
def combineAndSort(t):
    if t != 'word' and t != 'letter':
        return None
    # get input files
    session = Session(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    allFiles = map(lambda x: x.key, bucket.objects.all())
    needFiles = filter(lambda x: re.split('[_.]', x)[0] == t and re.split('[_.]', x)[0] == 'reduce', allFiles)

    # put contents into a dictionary
    odict = {}

    # merge files to sortedF.txt
    for f in needFiles:
        rawLine = s3.Object(bucket_name, f).get()['Body'].readline().rstrip().split('\t')
        odict[rawLine[0]] = int(rawLine[1])

    return list(map(lambda p: (p[0]+1, p[1][0], p[1][1]),
                enumerate(sorted(sorted(odict.items(), key=lambda p: p[0]), key=lambda p: p[1]))))


def uploadSQL(t, l, session):
    if t == 'word':
        session.query(Word).delete()
    else:
        session.query(Letter).delete()

    dCount = len(l)
    popular = int(math.ceil(dCount * 0.05))
    rare = int(math.floor(dCount * 0.95))
    common_l = int(math.floor(dCount * 0.475))
    common_u = int(math.ceil(dCount * 0.525))

    for rank, element, frequency in l:
        if rank <= popular:
            category = 'popular'
        elif common_l <= rank <= common_u:
            category = 'common'
        elif rare <= rank:
            category = 'rare'
        if category:
            if t == 'word':
                session.add(Word(rank, element, category, frequency))
            else:
                session.add(Letter(rank, element, category, frequency))


def main():
    session = SQLSession()
    chunk_size = chunk()
    print(chunk_size)
    spawnWorkers()
    s = initSocket()
    communicate(s)
    for t in ['word', 'letter']:
        rows = combineAndSort(t)
        uploadSQL(t, rows, session)
    session.commit()


if __name__ == "__main__":
    main()

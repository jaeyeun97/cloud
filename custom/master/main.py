import yaml, boto3, smart_open
import re, math, os, socket, threading
from boto3.session import Session
from kubernetes import client, config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, sessionmaker, Column, Integer, String

engine = create_engine('sqlite:///:memory:', echo=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Word(Base):
    __tablename__='words_custom'
    rank = Column(Integer)
    word = Column(String)
    category = Column(String)
    frequency = Column(Integer)
    
    def __init__(self, rank, word, category, frequency):
        self.rank = rank
        self.word = word,
        self.category = category
        self.frequency = frequency
    
class Letter(Base):
    __tablename__='letters_custom'
    rank = Column(Integer)
    letter = Column(String)
    category = Column(String)
    frequency = Column(Integer)
    
    def __init__(self, rank, letter, category, frequency):
        self.rank = rank
        self.letter = letter,
        self.category = category
        self.frequency = frequency
    
Base.metadata.create_all()

key = os.environ['AWS_ACCESS_KEY_ID']
secret = os.environ['AWS_SECRET_ACCESS_KEY']
region = os.environ['AWS_DEFAULT_REGION']
bucket_name = os.environ['BUCKET_NAME']
file_name = os.environ['FILE_NAME']
chunk_size = int(os.environ['CHUNK_SIZE'])
worker_count = int(os.environ['WORKER_COUNT'])
partition_num = 2 * worker_count
newline = '\n'.encode()   

config.load_incluster_config()
api = client.CoreV1Api()

# chunk/partition: 'unassigned', 'doing', 'done'
mapWordStat = dict()
reduceWordStat = dict()
mapLetterStat = dict()
reduceLetterStat = dict()
# woker number : {'mapWord', 'reduceWord', 'mapLetter', 'reduceLetter', 'idle'}
workerStat = dict()

# get file, chunk it up, upload to s3
def chunk():
    s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret)
    body = s3.get_object(Bucket=bucket_name, Key=file_name)['Body'] 
    kbread = body.read(1024)

    # the character that we'll split the data with (bytes, not string)
    partial_chunk = b''
    chunk_count = 0 
    while True:
        new_read = body.read(chunk_size)
        chunk = partial_chunk + new_read
        last_newline = chunk.rfind(newline) 
        result = chunk[0:last_newline+1]
        chunk_name = "{}:{}".format(file_name, chunk_count)
        s3.put_object(Body=result, Bucket=bucket_name, Key=chunk_name)
        mapWordStat[chunk_count] = 'unassigned'
        mapLetterStat[chunk_count] = 'unassigned'
        if len(partial_chunk) == 0:
            break
        else:
            partial_chunk = chunk[last_newline+1:]
            chunk_count += 1
    return chunk_count

def spawnWorkers():
    with open('worker.yaml', 'r') as f:
        conf_str = f.read().format(**{
            'aws_access_key_id': key,
            'aws_secret_access_key': secret,
            'bucket_name': bucket_name,
            'file_name': file_name
        })
    for i in range(worker_count):
        conf = yaml.load(conf_str.format(worker_num=i+1))
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
                workerStat[worker_num] = 'idle'
                #workerStat will need to be locked as well for worker entering/leaving feature
            elif status == 'doing':
                func_name = args[0]
                workerStat[worker_num] = func_name
                if func_name == 'mapWord':
                    chunk_count = int(args[1])
                    mapWordStat[chunk_count] = 'doing'
                elif func_name == 'reduceWord':
                    partition_count = int(args[1])
                    reduceWordStat[partition_count] = 'doing'
                elif func_name == 'mapLetter':
                    chunk_count = int(args[1])
                    mapLetterStat[chunk_count] = 'doing'
                elif func_name == 'reduceLetter':
                    partition_count = int(args[1])
                    reduceLetterStat[partition_count] = 'doing'
            elif status == 'done':
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
                            #give map job
                            conn.send("mapWord {} {}".format(k, partition_num))
                            print("assigned mapWord for chunk {} to {}".format(k, worker_num))
                            break
                elif 'unassigned' in reduceWordStat.values():
                    for k, v in reduceWordStat.items():
                        if v == 'unassigned':
                            #give reduce job
                            conn.send("reduceWord {}".format(k))
                            print("assigned reduceWord for partition {} to {}".format(k, worker_num))
                            break
                elif 'unassigned' in mapLetterStat.values():
                    for k, v in mapLetterStat.items():
                        if v == 'unassigned':
                            #give map job
                            conn.send("mapLetter {} {}".format(k, partition_num))
                            print("assigned mapLetter for chunk {} to {}".format(k, worker_num))
                            break
                elif 'unassigned' in reduceLetterStat.values():
                    for k, v in reduceLetterStat.items():
                        if v == 'unassigned':
                            #give reduce job
                            conn.send("reduceLetter {}".format(k))
                            print("assigned reduceLetter for partition {} to {}".format(k, worker_num))
                            break
                else:
                    conn.send("kill worker {}".format(worker_num))
                    print("just killed worker {}".format(worker_num))
                    break

        except:
            print('Something gone wrong')


def initSocket():
    s = socket.socket()
    host = socket.gethostname()
    port = 8000 
    s.bind((host, port))
    s.listen(10)
    return s


# combine results
def combineAndSort(t):
    if t != 'word' and t != 'letter':
        return None
    #get input files
    session = Session(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    allFiles = map(lambda x:x.key, bucket.objects.all())
    needFiles = filter(lambda x : re.split('[_.]', x)[0] == t and re.split('[_.]', x)[0] == 'reduce', allFiles)

	#put contents into a dictionary
    odict = {}
	
	#merge files to sortedF.txt
    for f in needFiles:
        rawLine = s3.Object(bucket_name, f).get()['Body'].readline().rstrip().split('\t')
        odict[rawLine[0]] = int(rawLine[1])
	    
    return list(map(lambda p: (p[0]+1, p[1][0], p[1][1]),
                enumerate(sorted(sorted(odict.items(), key=lambda p: p[0]), key=lambda p: p[1]))))	


def uploadSQL(t, l):
    session = Session()
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
            
    session.commit()

def main(): 
    chunk_size = chunk()
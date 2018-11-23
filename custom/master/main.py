import yaml, boto3, smart_open, os, socket
from kubernetes import client, config, watch

key = os.environ['AWS_ACCESS_KEY_ID']
secret = os.environ['AWS_SECRET_ACCESS_KEY']
region = os.environ['AWS_DEFAULT_REGION']
bucket_name = os.environ['BUCKET_NAME']
file_name = os.environ['FILE_NAME']
chunk_size = int(os.environ['CHUNK_SIZE'])
worker_count = int(os.environ['WORKER_COUNT'])
newline = '\n'.encode()   

config.load_incluster_config()
api = client.CoreV1Api()

mapStat = dict()
reduceStat = dict()

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

def commWorker(conn):
    while True:
        try:
            r = conn.recv(4096).decode('utf-8').split(' ') 
            if r[0] != 'worker':
                continue
            worker_num = int(r[1])
            status = r[2]
            args = r[3:]
            if status == 'init':
                if mapNotDone:
                    
                pass
            elif status == 'doing':
                func_name = args[0]
                # update worker status
                if func_name == 'map':
                    pass
                elif func_name == 'reduce':
                    pass
            elif status == 'done':
                func_name = args[0] 
                if func_name == 'map':
                    pass
                elif func_name == 'reduce':
                    pass
            

def communicate():
    s = socket.socket()
    host = socket.gethostname()
    port = 8000 
    s.bind((host, port))
    s.listen(5)
    while True:
       conn, addr = s.accept()
       commWorker(conn)

# combine results

# upload to database

def main():
    pass

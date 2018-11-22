import yaml, boto3, smart_open, os
from kubernetes import client, config, watch

key = os.environ['AWS_ACCESS_KEY_ID']
secret = os.environ['AWS_SECRET_ACCESS_KEY']
region = os.environ['AWS_DEFAULT_REGION']
bucket_name = os.environ['BUCKET_NAME']
file_name = os.environ['FILE_NAME']
chunk_size = int(os.environ['CHUNK_SIZE'])
newline = '\n'.encode()   

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

# start spawning workers

# infinitely communicate until job over

# combine results

# upload to database

def main():
    config.load_incluster_config()
    api =

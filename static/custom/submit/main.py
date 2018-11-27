import argparse
import yaml
import csv
import boto3
import os
from kubernetes import config, client
from smart_open import smart_open

bucketName = 'group2-custom'


def uploadToS3(key, secret, file_url):
    session = boto3.Session(aws_access_key_id=key, aws_secret_access_key=secret)
    s3 = session.client('s3')
    existing = [item['Name'] for item in s3.list_buckets()['Buckets']]

    if bucketName not in existing:
        resp = s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
        })
        if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("New Bucket Created")
    else:
        res = session.resource('s3')
        bucket = res.Bucket(bucketName)
        bucket.objects.all().delete()
    filename = os.path.basename(file_url)
    with smart_open(file_url, 'rb') as local:
        with smart_open('s3://{}/{}'.format(bucketName, filename), 'wb',
                        s3_session=session) as r:
            r.write(local.read())

    return bucketName, filename


def main(key, secret, file_url, chunk_size, master_image, worker_image, worker_count, input_size):
    # read in configuration
    config.load_kube_config()

    bucket_name, file_name = uploadToS3(key, secret, file_url)
    api = client.CoreV1Api()

    # populate .yaml file
    with open('master.svc.yaml', 'r') as f:
        conf = yaml.load(f.read())
        resp = api.create_namespaced_service(body=conf, namespace="default")

    with open('master.yaml', 'r') as f:
        conf = yaml.load(f.read().format(**{
            'aws_access_key_id': key,
            'aws_secret_access_key': secret,
            'bucket_name': bucket_name,
            'file_name': file_name,
            'chunk_size': chunk_size,
            'master_image': master_image,
            'worker_image': worker_image,
            'worker_count': worker_count,
            'input_size': input_size
        }))
        resp = api.create_namespaced_pod(body=conf, namespace="default")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Group2 Custom Word Count Application. You should have a kubernetes configuration set up--check by execute `kubectl`')
    parser.add_argument('file_url', help='file url to count')
    parser.add_argument('--csv', dest='csv', default='credentials.csv', help='CSV file with credentials')
    parser.add_argument('--input-size', dest='input_size', default='200', type=int, help='Input Size in MB')
    parser.add_argument('--chunk-size', dest='chunk_size', default='64', type=int, help='Chunk size in KiB')
    parser.add_argument('--master-image', dest='master_image', default='jaeyeun97/wordcount-master:latest', help='Master Docker Image')
    parser.add_argument('--worker-image', dest='worker_image', default='jaeyeun97/wordcount-worker:latest', help='Worker Docker Image')
    parser.add_argument('--worker-count', dest='worker_count', default='5', type=int, help='Number of workers to spawn')
    args = parser.parse_args()

    username = None
    password = None
    key = None
    secret = None

    with open(args.csv, 'r') as f:
        reader = csv.reader(f)
        info = [line for line in reader][1]
        if len(info) < 4:
            raise Exception('do not have enough information')
    username = info[0]
    password = info[1]
    key = info[2]
    secret = info[3]

    if key is None or secret is None:
        raise Exception('need key and secret')
    main(key, secret, args.file_url, args.chunk_size, args.master_image, args.worker_image, args.worker_count, args.input_size)

import boto3
import csv
import subprocess
import os
import argparse
import re
import yaml
import pydoc
from sqlalchemy import create_engine
from kubernetes import client, config

prompt_text = """
0: Exit
1: Define a Kubernetes Cluster
    11: Review the cluster Definition
2: Launch the cluster on AWS
    21: Validate the cluster
    22: Deploy the Kubernetes web-dashboard
    23: Access the Kubernetes web-dashboard
3: View the cluster
    31: Get the admin password
    32: Get the admin service account token
4: Delete the cluster
5: Run Spark WordLetterCount App URL
    51: View Spark App
    52: Show Output
6: Run Custom-built WordLetterCount App URL chunk-size
    61: View Custom-built App
    62: Show Output
7: Run Both WordLetterCounts with Static Scheduler
    71: View Static Scheduler

Please enter your choice: [#] """

readable_name = {
    'cluster_name': 'Cluster Name',
    'zones': 'Zones',
    'node_count': 'Node_Count',
    'node_size': 'Node Size',
    'master_size': 'Master Size',
    'master_count': 'Master Count'
}

create_template = """
kops create cluster \
        --cloud aws \
        --name {cluster_name} \
        --node-count {node_count} \
        --zones {zones} \
        --node-size {node_size} \
        --node-volume-size 8 \
        --master-size {master_size} \
        --master-count {master_count} \
        --yes
"""

config.load_kube_config()
kapi = client.CoreV1Api()
kapi_rbac = client.RbacAuthorizationV1alpha1Api()
delete_options = client.V1DeleteOptions()
worker_count = 9

update_template = "kops update cluster --name {cluster_name} --yes"
validate_template = "kops validate cluster --name {cluster_name}"

kops_export_template = 'kops export kubecfg {cluster_name}'
view_cluster_command = 'kubectl cluster-info'.split()
get_nodes_command = 'kubectl get nodes'.split()

deploy_dashboard_template = 'kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/master/src/deploy/recommended/kubernetes-dashboard.yaml'
access_dashboard_template = "kubectl proxy"
get_password_template = 'kops get secrets kube --type secret -oplaintext --name {cluster_name}'
get_token_template = 'kops get secrets admin --type secret -oplaintext --name {cluster_name}'

# Delete Cluster
delete_template = 'kops delete cluster {cluster_name} --yes'

delete_all_command = 'kubectl delete pods --all'.split()
delete_svc_template = 'kubectl delete svc {service}'

custom_run_template = """python custom/submit/main.py \
                         --key {key} \
                         --secret {secret} \
                         --worker-count {worker_count} \
                         --input-size {input_size} \
                         --worker-scheduler {scheduler} \
                         --chunk-size {chunk_size} \
                         {file_url}"""

spark_run_template = """spark-submit \
--master k8s://http://localhost:8001 \
--deploy-mode cluster \
--name wordCount \
--conf spark.app.name=wordCount \
--conf spark.executor.instances={worker_count} \
--conf spark.kubernetes.driver.label.inputSize={input_size} \
--conf spark.kubernetes.driver.label.run=group2-spark-master \
--conf spark.kubernetes.executor.label.run=group2-spark-worker \
--conf spark.kubernetes.container.image={spark_image} \
--conf spark.kubernetes.container.image.pullPolicy=Always \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.pyspark.pythonVersion=3 \
--jars http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar,http://central.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar,http://central.maven.org/maven2/mysql/mysql-connector-java/8.0.13/mysql-connector-java-8.0.13.jar \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--conf "spark.hadoop.fs.s3a.access.key={key}" \
--conf "spark.hadoop.fs.s3a.secret.key={secret}" \
file:///opt/spark/work-dir/step2.py {file_url}
"""

engine = create_engine('mysql+pymysql://group2:group2sibal@group2dbinstance.cxezedslevku.eu-west-2.rds.amazonaws.com/sw777_CloudComputingCoursework')


letterSQL = 'select rank, letter, category, frequency from letters_{}'
wordSQL = 'select rank, word, category, frequency from words_{}'


def getDBResults(appName):
    conn = engine.connect()
    ts = conn.execute('show tables')
    ts = [t[0] for t in ts]
    content = ""
    if 'letters_{}'.format(appName) in ts:
        letters = conn.execute(letterSQL.format(appName))
        content += 'Letters\n--------\nrank\tletter\tcategory\tfrequency\n' \
                   + "\n".join(("{}\t{}\t{}\t{}".format(l[0], l[1], l[2], l[3]) for l in letters)) \
                   + "\n\n"
    else:
        print('Letters not in the table, did you run the app?')
    if 'words_{}'.format(appName) in ts:
        words = conn.execute(wordSQL.format(appName))
        content += 'Words\n-----\nrank\tword\tcategory\tfrequency\n' \
                   + "\n".join(("{}\t{}\t{}\t\t{}".format(l[0], l[1], l[2], l[3]) for l in words)) \
                   + "\n\n"
    else:
        print('Words not in the table, did you run the app?')
    if len(content) > 0:
        pydoc.pager(content)
    conn.close()


class Shibal(object):
    def __init__(self, username, key, secret):
        assert username is not None
        assert key is not None
        assert secret is not None

        self.username = username
        self.key = key
        self.secret = secret
        self.clusterName = "{}.k8s.local".format(username)
        self.p = None

        self.environ = os.environ.copy()
        self.environ.update({
            'AWS_ACCESS_KEY_ID': key,
            'AWS_SECRET_ACCESS_KEY': secret,
            'PYTHONUNBUFFERED': '0'
        })

        self.default_options = {
            'cluster_name': self.clusterName
        }

        self.creation_options = {
            'zones': "eu-west-2a,eu-west-2b,eu-west-2c",
            'node_count': 10,
            'node_size': "t2.small",
            'master_size': "c4.large",
            'master_count': 1
        }

        self.setupS3()
        self.main()

    @staticmethod
    def createServiceAccount(yamlFile):
        with open(os.path.join(os.path.dirname(__file__), yamlFile), 'r') as f:
            try:
                kapi.create_namespaced_service_account('default', yaml.load(f))
            except client.rest.ApiException:
                print('ServiceAccount Exists')

    @staticmethod
    def createClusterRoleBinding(yamlFile):
        with open(os.path.join(os.path.dirname(__file__), yamlFile), 'r') as f:
            try:
                kapi_rbac.create_cluster_role_binding(yaml.load(f))
            except client.rest.ApiException:
                print('ClusterRoleBinding Exists')

    def setupS3(self):
        storeName = "{}-kops-state-store".format(self.username)
        s3 = boto3.client('s3', aws_access_key_id=self.key, aws_secret_access_key=self.secret)
        existing = [item['Name'] for item in s3.list_buckets()['Buckets']]

        if storeName not in existing:
            resp = s3.create_bucket(Bucket=storeName, CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
                })
            if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("New Bucket Created")

        # Versioning
        s3.put_bucket_versioning(Bucket=storeName, VersioningConfiguration={
            'Status': 'Enabled'
            })
        # Encryption
        s3.put_bucket_encryption(
                Bucket=storeName,
                ServerSideEncryptionConfiguration={
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256"
                                }
                            }
                        ]
                    }
                )
        self.environ['KOPS_STATE_STORE'] = "s3://{}".format(storeName)

    def runSpark(self, file_url, spark_image):
        self.createServiceAccount('spark/driver.sva.yaml')
        self.createClusterRoleBinding('spark/driver.crb.yaml')
        # Spark Run
        file_name = os.path.basename(file_url)
        match = re.match(r'data-(.*)MB.txt', file_name)
        if match:
            file_size = int(match.group(1))
            command = spark_run_template.format(**{
                    'key': self.key,
                    'secret': self.secret,
                    'worker_count': worker_count,
                    'file_url': file_url,
                    'input_size': file_size,
                    'spark_image': spark_image
                }).split()
            if self.p is None:
                proxy_command = access_dashboard_template.format(**self.default_options).split()
                self.p = subprocess.Popen(proxy_command, env=self.environ)
            try:
                self.p.communicate(timeout=1)
            except subprocess.TimeoutExpired:
                pass
            except Exception:
                self.p.terminate()
                self.p = None
                print('Proxy cannot be Established')
                return
            try:
                subprocess.call(command, env=self.environ)
            except KeyboardInterrupt:
                print("Moving on")
                return
        else:
            print("File wrong Format")

    def runCustom(self, file_url, scheduler_name, chunk_size):
        subprocess.call('kubectl delete service group2-custom-master'.split(), env=self.environ)
        # try:
        #     kapi.delete_namespaced_service(name='group2-custom-master', namespace='default', body=delete_options)
        # except client.rest.ApiException:
        #     print('Service does not exist, but all good to go')
        self.createServiceAccount('custom/master.sva.yaml')
        self.createClusterRoleBinding('custom/master.crb.yaml')
        file_name = os.path.basename(file_url)
        match = re.match(r'data-(.*)MB.txt', file_name)
        if match:
            file_size = int(match.group(1))
            command = custom_run_template.format(**{
                        'key': self.key,
                        'secret': self.secret,
                        'worker_count': worker_count,
                        'file_url': file_url,
                        'input_size': file_size,
                        'scheduler': scheduler_name,
                        'chunk_size': chunk_size
                    }).split()
            subprocess.call(command, env=self.environ)
        else:
            print("File wrong Format")

    def runStatic(self, spark_file, custom_file, chunk_size):
        self.createServiceAccount('static/static.sva.yaml')
        self.createClusterRoleBinding('static/static.crb.yaml')
        # try:
        #     kapi.delete_namespaced_pod(name='staticscheduler', namespace='kube-system', body=delete_options)
        # except client.rest.ApiException:
        #     print('Scheduler does not exist, but all good to go')
        subprocess.call('kubectl delete pod staticscheduler --namespace=kube-system'.split(), env=self.environ)
        with open(os.path.join(os.path.dirname(__file__), 'static/staticScheduler.yaml'), 'r') as f:
            kapi.create_namespaced_pod(body=yaml.load(f), namespace='kube-system')
        self.runCustom(custom_file, 'staticscheduler', chunk_size)
        self.runSpark(spark_file, 'jaeyeun97/wordcount:static')

    def main(self):
        while True:
            prompt = input(prompt_text)
            try:
                prompt = int(prompt)
            except Exception:
                print("Input an Integer")
                continue
            if prompt == 0:
                if self.p:
                    self.p.terminate()
                break
            elif prompt == 1:
                print()
                for k, v in self.default_options.items():
                    print("{}: {}".format(readable_name[k], v))
                for k, v in self.creation_options.items():
                    print("{}: {}".format(readable_name[k], v))
            elif prompt == 11:
                for k, v in self.default_options.items():
                    val = input("{} [{}]: ".format(readable_name[k], v))
                    if len(val) > 0:
                        self.default_options[k] = val
                for k, v in self.creation_options.items():
                    val = input("{} [{}]: ".format(readable_name[k], v))
                    if len(val) > 0:
                        self.creation_options[k] = val
            elif prompt == 2:
                # Create
                create_command = create_template.format(**self.default_options, **self.creation_options).split()
                subprocess.call(create_command, env=self.environ)
                update_command = update_template.format(**self.default_options).split()
                subprocess.call(update_command, env=self.environ)
            elif prompt == 21:
                # Validate
                kops_export_command = kops_export_template.format(**self.default_options).split()
                subprocess.call(kops_export_command, env=self.environ)
                validate_command = validate_template.format(**self.default_options).split()
                subprocess.call(validate_command, env=self.environ)
            elif prompt == 22:
                # Deploy Dashboard
                deploy_dashboard_command = deploy_dashboard_template.format(**self.default_options).split()
                subprocess.call(deploy_dashboard_command, env=self.environ)
            elif prompt == 23:
                # Access Dashboard
                access_dashboard_command = access_dashboard_template.format(**self.default_options).split()
                if self.p is None:
                    self.p = subprocess.Popen(access_dashboard_command, env=self.environ)
                try:
                    self.p.communicate(timeout=1)
                except Exception:
                    pass
                finally:
                    print("Open: http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/")
            elif prompt == 3:
                subprocess.call(view_cluster_command, env=self.environ)
                subprocess.call(get_nodes_command, env=self.environ)
            elif prompt == 31:
                get_password_command = get_password_template.format(**self.default_options).split()
                subprocess.call(get_password_command, env=self.environ)
            elif prompt == 32:
                get_token_command = get_token_template.format(**self.default_options).split()
                subprocess.call(get_token_command, env=self.environ)
            elif prompt == 4:
                if self.p:
                    self.p.terminate()
                delete_command = delete_template.format(**self.default_options).split()
                subprocess.call(delete_command, env=self.environ)
            elif prompt == 5:
                url = input('URL (Must be a s3:// file): ')
                if len(url) < 0 or not url.startswith('s3://'):
                    print('Invalid URL')
                    continue
                subprocess.call(delete_all_command, env=self.environ)
                self.runSpark(url, 'jaeyeun97/wordcount:latest')
            elif prompt == 51:
                pass
            elif prompt == 52:
                getDBResults('spark')
            elif prompt == 6:
                # Custom Run
                url = input('URL (Local, http(s), public s3 file (see smart_open)): ')
                if len(url) < 0:
                    print('Invalid URL')
                    continue
                chunk_size = input('Chunk Size (KiB): ')
                if not chunk_size.isdigit():
                    print('Chunk Size must be an Integer')
                else:
                    chunk_size = int(chunk_size)
                    subprocess.call(delete_all_command, env=self.environ)
                    self.runCustom(url, 'default-scheduler', chunk_size)
            elif prompt == 61:
                pass
            elif prompt == 62:
                getDBResults('custom')
            elif prompt == 7:
                spark_url = input('Spark URL (Must be a s3:// file): ')
                if len(spark_url) < 0 or not spark_url.startswith('s3://'):
                    print('Invalid URL')
                    continue
                custom_url = input('Custom URL (Local, http(s), public s3 file (see smart_open)): ')
                if len(custom_url) < 0:
                    print('Invalid URL')
                    continue
                chunk_size = input('Chunk Size (KiB): ')
                if not chunk_size.isdigit():
                    print('Chunk Size must be an Integer')
                    continue
                chunk_size = int(chunk_size)
                subprocess.call(delete_all_command, env=self.environ)
                self.runStatic(spark_url, custom_url, chunk_size)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloud Computing Assessment CLI - Group 2')
    parser.add_argument('--csv', dest='csvfile', default='credentials.csv',
                        help='CSV file with credentials')
    args = parser.parse_args()

    username = None
    password = None
    key = None
    secret = None

    with open(args.csvfile, 'r') as f:
        reader = csv.reader(f)
        info = [line for line in reader][1]
        username = info[0]
        password = info[1]
        key = info[2]
        secret = info[3]

    Shibal(username, key, secret)

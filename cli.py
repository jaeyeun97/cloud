import boto3
import csv
import subprocess
import os
import argparse

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

update_template = "kops update cluster --name {cluster_name} --yes"
validate_template = "kops validate cluster --name {cluster_name}"

view_cluster_command = 'kubectl cluster-info'.split()
get_nodes_command = 'kubectl get nodes'.split()

deploy_dashboard_template = 'kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/master/src/deploy/recommended/kubernetes-dashboard.yaml'
access_dashboard_template = "kubectl proxy"
get_password_template = 'kops get secrets kube --type secret -oplaintext --name {cluster_name}'
get_token_template = 'kops get secrets admin --type secret -oplaintext --name {cluster_name}'

# Delete Cluster
delete_template = 'kops delete cluster {cluster_name} --yes'


class Shibal(object):
    def __init__(self, username, key, secret):
        assert username is not None
        assert key is not None
        assert secret is not None

        self.username = username
        self.key = key
        self.secret = secret
        self.clusterName = "{}.k8s.local".format(username)

        self.environ = os.environ.copy()
        self.environ.update({
            'AWS_ACCESS_KEY_ID': key,
            'AWS_SECRET_ACCESS_KEY': secret
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

    def main(self):
        p = None
        while True:
            prompt = input(prompt_text)
            prompt = int(prompt)
            if prompt == 0:
                if p:
                    p.terminate()
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
                validate_command = validate_template.format(**self.default_options).split()
                subprocess.call(validate_command, env=self.environ)
            elif prompt == 22:
                # Deploy Dashboard
                deploy_dashboard_command = deploy_dashboard_template.format(**self.default_options).split()
                subprocess.call(deploy_dashboard_command, env=self.environ)
            elif prompt == 23:
                # Access Dashboard
                access_dashboard_command = access_dashboard_template.format(**self.default_options).split()
                p = subprocess.Popen(access_dashboard_command, env=self.environ)
                try:
                    p.communicate(timeout=1)
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
                if p:
                    p.terminate()
                delete_command = delete_template.format(**self.default_options).split()
                subprocess.call(delete_command, env=self.environ)


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

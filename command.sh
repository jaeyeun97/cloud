#set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# AWS helpers
aws iam get-user

aws s3api create-bucket --bucket group2-kops-state-store --region eu-west-2 --create-bucket-configuration LocationConstraint=eu-west-2

aws s3api put-bucket-versioning --bucket group2-kops-state-store --versioning-configuration Status=Enabled 

aws s3api put-bucket-encryption --bucket group2-kops-state-store --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'


# Create Cluster
kops create cluster \
--cloud=aws \
--name=group2.k8s.local \
--state="s3://group2-kops-state-store" \
--node-count=10 \
--zones="eu-west-2a,eu-west-2b,eu-west-2c" \
--node-size=t2.small \
--node-volume-size=8 \
--master-size=c4.large \
--master-count=1 \
--yes

## Build Cluster
kops update cluster --name "group2.k8s.local" --yes

# Validate (uses kubectl context)
kops validate cluster
# (catch on unexpected error during validation, need to wait)

# View Cluster
kubectl cluster-info
kubectl get nodes

# deploy dashboard
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/master/src/deploy/recommended/kubernetes-dashboard.yaml

# access dashboard
kubectl proxy
# print "Open: http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/"

# Admin password
kops get secrets kube --type secret -oplaintext

# Admin Token
kops get secrets admin --type secret -oplaintext

# Delete Cluster
kops delete cluster "group2.k8s.local" --yes

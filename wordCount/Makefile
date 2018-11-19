PROJ_NAME=wordcount
REPO=jaeyeun97
SPARK=../../spark

export PYSPARK_PYTHON=python2

default: all

all: build run

build: build_image tag push

build_image:
	docker build -t ${PROJ_NAME}:latest .

tag:
	docker tag ${PROJ_NAME} ${REPO}/${PROJ_NAME}

push:
	docker push ${REPO}/${PROJ_NAME}

runlocal:
	${SPARK}/bin/spark-submit \
		--master local[4] \
		--conf "spark.driver.extraClassPath=jars/aws-java-sdk-bundle-1.11.271.jar:jars/hadoop-aws-3.1.1.jar:jars/mysql-connector-java-8.0.13.jar" \
		--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
		--conf "spark.hadoop.fs.s3a.access.key=AKIAJ6G7DAUEOXWO74QA" \
		--conf "spark.hadoop.fs.s3a.secret.key=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" \
		./step2.py "s3a://cam-cloud-computing-data-source/data-500MB.txt"

runcluster:
	${SPARK}/bin/spark-submit \
	--master k8s://http://localhost:8001 \
    --deploy-mode cluster \
    --name wordCount \
	--conf spark.app.name=wordCount \
    --conf spark.executor.instances=10 \
	--conf spark.kubernetes.container.image=jaeyeun97/wordcount:latest \
	--conf spark.kubernetes.container.image.pullPolicy=Always \
	--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  \
	--jars http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar,http://central.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar,http://central.maven.org/maven2/mysql/mysql-connector-java/8.0.13/mysql-connector-java-8.0.13.jar \
	--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
	--conf "spark.hadoop.fs.s3a.access.key=AKIAJ6G7DAUEOXWO74QA" \
	--conf "spark.hadoop.fs.s3a.secret.key=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" \
	file:///opt/spark/work-dir/step2.py "s3a://cam-cloud-computing-data-source/data-500MB.txt"

# kubectl create serviceaccount spark
# kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default

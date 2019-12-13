PROJ_NAME=wordcount
REPO=jaeyeun97
SPARK=../../spark

export PYSPARK_PYTHON=python3

default: all

all: build run

build:
	docker build -t ${PROJ_NAME}:latest .
	docker tag ${PROJ_NAME}:latest ${REPO}/${PROJ_NAME}:latest
	docker push ${REPO}/${PROJ_NAME}:latest

buildstatic:
	docker build -f Dockerfile.static -t ${PROJ_NAME}:static .
	docker tag ${PROJ_NAME}:static ${REPO}/${PROJ_NAME}:static
	docker push ${REPO}/${PROJ_NAME}:static


runlocal:
	${SPARK}/bin/spark-submit \
		--master local[4] \
		---jars jars/aws-java-sdk-bundle-1.11.271.jar,jars/hadoop-aws-3.1.1.jar,jars/mysql-connector-java-8.0.13.jar \
		--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
		--conf "spark.hadoop.fs.s3a.access.key=AKIAJ6G7DAUEOXWO74QA" \
		--conf "spark.hadoop.fs.s3a.secret.key=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" \
		./step2.py "s3a://cam-cloud-computing-data-source/data-200MB.txt"

init:
	kubectl create -f driver.sva.yaml
	kubectl create -f driver.crb.yaml

run:
	${SPARK}/bin/spark-submit \
	--master k8s://http://localhost:8001 \
  	--deploy-mode cluster \
  	--name wordCount \
	--conf spark.app.name=wordCount \
  	--conf spark.executor.instances=1 \
	--conf spark.kubernetes.container.image=jaeyeun97/wordcount:latest \
	--conf spark.kubernetes.container.image.pullPolicy=Always \
	--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  \
	--conf spark.kubernetes.pyspark.pythonVersion=3 \
	--jars http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar,http://central.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar,http://central.maven.org/maven2/mysql/mysql-connector-java/8.0.13/mysql-connector-java-8.0.13.jar \
	--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
	--conf "spark.hadoop.fs.s3a.access.key=AKIAJ6G7DAUEOXWO74QA" \
	--conf "spark.hadoop.fs.s3a.secret.key=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" \
	file:///opt/spark/work-dir/step2.py "s3://cam-cloud-computing-data-source/data-200MB.txt"

runstatic:
	${SPARK}/bin/spark-submit \
	--master k8s://http://localhost:8001 \
	--deploy-mode cluster \
	--name wordCount \
	--conf spark.app.name=wordCount \
	--conf spark.executor.instances=9 \
	--conf spark.kubernetes.driver.label.inputSize=200 \
	--conf spark.kubernetes.driver.label.run=group2-spark-master \
	--conf spark.kubernetes.executor.label.run=group2-spark-worker \
	--conf spark.kubernetes.container.image=jaeyeun97/wordcount:static \
	--conf spark.kubernetes.container.image.pullPolicy=Always \
	--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
	--conf spark.kubernetes.pyspark.pythonVersion=3 \
	--jars http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar,http://central.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar,http://central.maven.org/maven2/mysql/mysql-connector-java/8.0.13/mysql-connector-java-8.0.13.jar \
	--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
	--conf "spark.hadoop.fs.s3a.access.key=AKIAJ6G7DAUEOXWO74QA" \
	--conf "spark.hadoop.fs.s3a.secret.key=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" \
	file:///opt/spark/work-dir/step2.py "s3://cam-cloud-computing-data-source/data-200MB.txt"



# kubectl create serviceaccount spark
# kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default

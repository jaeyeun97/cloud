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
		--master local[2] \
		./step2.py

runtest:
	${SPARK}/bin/spark-submit \
		--master local[2] \
		./test.py

runcluster:
	${SPARK}/bin/spark-submit \
    --master k8s://http://localhost:8001 \
    --deploy-mode cluster \
    --name wordcount \
    --conf spark.executor.instances=10 \
    --conf spark.kubernetes.container.image=jaeyeun97/wordcount \
    file:///opt/spark/work-dir/step2.py

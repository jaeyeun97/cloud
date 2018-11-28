all:

spark_%:
	$(MAKE) -C spark $*

custom_%:
	$(MAKE) -C custom $*

build: static_build spark_build custom_build

static_build: image tag push

image:
	docker build -t staticscheduler .

tag:
	docker tag staticscheduler jaeyeun97/staticscheduler

push:
	docker push jaeyeun97/staticscheduler

delete: custom_delete

deleteall:
	kubectl delete pods --all

static_run:
	kubectl create -f staticScheduler.yaml

run: spark_run custom_run

PROJ_NAME=wordcount
REPO=jaeyeun97

all:

spark_%:
	$(MAKE) -C spark $*

custom_%:
	$(MAKE) -C custom $*

build: static_build spark_build custom_build

delete: custom_delete

deleteall:
	kubectl delete pods --all

static_build: image tag push

image:
	docker build ?

tag:
	docker tag ?

push:
	docker push ?

run: spark_run custom_run

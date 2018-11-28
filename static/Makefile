all:

spark_%:
	$(MAKE) -C spark $*

custom_%:
	$(MAKE) -C custom $*

build: static_build spark_build custom_build

static_build: image tag push

image:
	docker build -t staticScheduler .

tag:
	docker tag staticSchedule jaeyeun97/staticScheduler

push:
	docker push jaeyeun97/staticScheduler

delete: custom_delete

deleteall:
	kubectl delete pods --all

run: spark_run custom_run

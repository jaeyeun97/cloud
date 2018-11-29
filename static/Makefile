all: custom_delete scheduler_delete scheduler_build

spark_%:
	$(MAKE) -C spark $*

custom_%:
	$(MAKE) -C custom $*

scheduler_%:
	$(MAKE) -C scheduler $*

build: scheduler_build spark_build custom_build

delete: scheduler_delete custom_delete

deleteall:
	kubectl delete pods --all

run: scheduler_delete custom_delete scheduler_run custom_run

scheduler_build:
	docker build -t staticscheduler ./scheduler
	docker tag staticscheduler jaeyeun97/staticscheduler
	docker push jaeyeun97/staticscheduler

scheduler_delete:
	kubectl delete pod staticscheduler --namespace=kube-system || true

scheduler_run:
	kubectl create -f ./scheduler/staticScheduler.yaml

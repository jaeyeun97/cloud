all: delete build

static_build: image tag push

image:
	docker build -t staticscheduler .

tag:
	docker tag staticscheduler jaeyeun97/staticscheduler

push:
	docker push jaeyeun97/staticscheduler

delete:
	kubectl delete deployment staticscheduler --namespace=kube-system

run:
	kubectl create -f staticScheduler.yaml

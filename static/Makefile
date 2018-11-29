all: delete build

build:
	docker build -t staticscheduler .
	docker tag staticscheduler jaeyeun97/staticscheduler
	docker push jaeyeun97/staticscheduler

delete:
	kubectl delete pod staticscheduler --namespace=kube-system || true

run:
	kubectl create -f ./staticScheduler.yaml

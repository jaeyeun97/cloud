all: delete build

build:
	docker build -t dynamicscheduler .
	docker tag dynamicscheduler jaeyeun97/dynamicscheduler
	docker push jaeyeun97/dynamicscheduler

delete:
	kubectl delete pod dynamicscheduler --namespace=kube-system || true

run:
	kubectl create -f ./dynamicscheduler.yaml


TARGET = master worker
IMG_PREFIX = wordcount-
POD_PREFIX = group2-custom-
USER_HANDLE = jaeyeun97

WORKERS = $(addprefix worker-, 1 2 3 4 5)
PODS = $(addprefix $(POD_PREFIX), master $(WORKERS))

all: delete build

build: build_master build_worker

delete: delete_pods delete_svc

delete_svc:
	kubectl delete svc $(POD_PREFIX)master || true

delete_pods: $(addprefix delete_pod_, $(PODS))

delete_pod_%:
	kubectl delete pod $* || true

build_%:
	docker build -t $(IMG_PREFIX)$* ./$*
	docker tag $(IMG_PREFIX)$* $(USER_HANDLE)/$(IMG_PREFIX)$*
	docker push $(USER_HANDLE)/$(IMG_PREFIX)$*

run:
	cd ./submit && python main.py --csv ../../credentials.csv --worker-count 9 --chunk-size 32768 https://s3.eu-west-2.amazonaws.com/cam-cloud-computing-data-source/data-500MB.txt

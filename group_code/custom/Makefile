
TARGET = master worker
IMG_PREFIX = wordcount-
POD_PREFIX = group2-custom-
USER_HANDLE = jaeyeun97

WORKERS = $(addprefix worker-, 1 2 3 4 5 6 7 8 9)
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
	cd ./submit && python main.py --csv ../../credentials.csv --worker-count 5 --chunk-size 32768 https://www.cl.cam.ac.uk/teaching/1819/CloudComp/files/samples/sample-f.txt

runstatic:
	cd ./submit && python main.py --csv ../../credentials.csv --worker-count 9 --input-size 200  --worker-scheduler staticscheduler --chunk-size 32768  https://s3.eu-west-2.amazonaws.com/cam-cloud-computing-data-source/data-200MB.txt

all:

spark_%:
	$(MAKE) -C spark $*

custom_%:
	$(MAKE) -C custom $*

static_%:
	$(MAKE) -C static $*

build: spark_build custom_build static_build

delete: custom_delete

deleteall:
	kubectl delete pods --all

run: spark_run custom_run

all: custom_delete static_delete static_build

spark_%:
	$(MAKE) -C spark $*

custom_%:
	$(MAKE) -C custom $*

static_%:
	$(MAKE) -C custom $*

build: static_build spark_build custom_build

delete: custom_delete

deleteall:
	kubectl delete pods --all

run: static_delete custom_delete static_run custom_run

.PHONY: all build_alert_api build_storage_backends build_anomaly_store alert-api anomaly_store release

PWD := $(shell pwd)
MAVEN := ./mvnw

clean:
	${MAVEN} clean

build: clean
	${MAVEN} install package

all: clean build_alert_api build_storage_backends build_anomaly_store alert-api anomaly_store

report-coverage:
	${MAVEN} scoverage:report-only

build_alert_api:
	${MAVEN} package -DfinalName=haystack-alert-api -pl alert-api -am

alert-api:
	$(MAKE) -C alert-api all

build_storage_backends:
	$(MAKE) -C storage-backends all

build_anomaly_store:
	${MAVEN} package -DfinalName=haystack-anomaly-store -pl anomaly-store -am

anomaly_store:
	$(MAKE) -C anomaly-store all

# build all and release
release: clean build_alert_api build_anomaly_store
	cd alert-api && $(MAKE) release
	cd anomaly-store && $(MAKE) release
	./.travis/deploy.sh




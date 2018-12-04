.PHONY: all build_subscription_manager subscription-manager release

PWD := $(shell pwd)
MAVEN := ./mvnw

clean:
	${MAVEN} clean

build: clean
	${MAVEN} install package

all: clean build_subscription_manager subscription-manager

report-coverage:
	${MAVEN} scoverage:report-only

build_transformer:
	${MAVEN} package -DfinalName=haystack-subscription-manager -pl subscription-manager -am

subscription-manager:
	$(MAKE) -C subscription-manager all

# build all and release
release: clean build_subscription_manager
	cd subscription-manager && $(MAKE) release
	./.travis/deploy.sh




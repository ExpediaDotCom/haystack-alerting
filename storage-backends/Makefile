.PHONY: all build_api build_elasticsearch

PWD := $(shell pwd)
MAVEN := ../mvnw

build: ${MAVEN} install package

all: build_api build_elasticsearch

build_api:
	${MAVEN} package -pl api -am

build_elasticsearch:
	${MAVEN} package -DfinalName=elasticsearch-store -pl elasticsearch -am




.PHONY: integration_test release all

MAVEN := ../mvnw

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-alert-api

docker_build: copy_backend_jars
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

copy_backend_jars:
	mkdir -p storage-backends
	cp ../storage-backends/elasticsearch/target/elasticsearch-store.jar storage-backends/elasticsearch-store.jar

integration_test: docker_build
	# prepare environment to run integration tests against
	docker-compose -f build/docker/docker-compose.yml up -d
	sleep 30
	${MAVEN} verify
	docker-compose -f build/docker/docker-compose.yml stop

# build jar, docker image and run integration tests
all: integration_test

# build all and release
release: docker_build
	../deployment/scripts/publish-to-docker-hub.sh

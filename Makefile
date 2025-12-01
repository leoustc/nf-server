.PHONY: all clean up logs build-local build-gateway build-proxy-base build-proxy-runtime 

all: clean build-local up logs

fresh: clean build-local up logs

up:
	docker compose up -d --force-recreate

build-local: build-gateway build-proxy-base build-proxy-runtime

build-gateway:
	cd src/gateway && docker build -t rest-gateway:latest -f Dockerfile .

build-proxy-base:
	cd src/proxy && docker build --target base -t rest-proxy-base:latest -f Dockerfile .

build-proxy-runtime:
	cd src/proxy && docker build --target runtime -t rest-proxy:latest -f Dockerfile .

logs:
	docker compose logs -f

logs-proxy:
	docker compose logs -f proxy-0 proxy-1

clean:
	docker compose down --volumes --remove-orphans
.PHONY: build deps vendor docker-build docker-push clean init

VERSION ?= latest
IMAGE ?= shaowenchen/ops-proxy
DOCKER_TAG = $(IMAGE):$(VERSION)

deps:
	go mod download
	go mod tidy

vendor:
	go mod vendor
	go mod verify

# Initialize vendor directory (first time setup)
init: deps vendor
	@echo "Vendor directory initialized. You can now commit vendor/ to the repository."

# Build using vendor if available, otherwise use go mod
build:
	@if [ -d "vendor" ]; then \
		echo "Building with vendor..."; \
		CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo -o ops-proxy .; \
	else \
		echo "Building without vendor (downloading dependencies)..."; \
		go mod download; \
		CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ops-proxy .; \
	fi

# Build without vendor (always download dependencies)
build-without-vendor: deps
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ops-proxy .

docker-build:
	docker build -t $(DOCKER_TAG) .

docker-push: docker-build
	docker push $(DOCKER_TAG)

clean:
	rm -f ops-proxy

test:
	go test -v ./...


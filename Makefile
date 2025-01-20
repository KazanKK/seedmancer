# Build variables
BINARY_NAME=reseeder
VERSION=$(shell git describe --tags --always --dirty)
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME}"

# Supported platforms
PLATFORMS=linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

.PHONY: all build clean test lint security build-all

all: clean build test lint security

build:
	go build ${LDFLAGS} -o bin/${BINARY_NAME}

build-all:
	$(foreach platform,$(PLATFORMS),\
		GOOS=$(word 1,$(subst /, ,$(platform))) \
		GOARCH=$(word 2,$(subst /, ,$(platform))) \
		go build ${LDFLAGS} -o bin/${BINARY_NAME}-$(word 1,$(subst /, ,$(platform)))-$(word 2,$(subst /, ,$(platform)))$(if $(findstring windows,$(platform)),.exe,) ;\
	)

clean:
	rm -rf bin/

test:
	go test -v -race ./...

lint:
	golangci-lint run

security:
	gosec ./... 
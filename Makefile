VERSION := 1.0.0
BINARY_NAME := reseeder
PLATFORMS := linux darwin windows

.PHONY: all clean build-all docs

all: clean build-all

clean:
	rm -rf dist/

build-all:
	mkdir -p dist
	$(foreach PLATFORM,$(PLATFORMS),\
		GOOS=$(PLATFORM) GOARCH=amd64 go build -ldflags="-X 'main.Version=$(VERSION)'" -o dist/$(BINARY_NAME)_$(VERSION)_$(PLATFORM)_amd64 ;\
		if [ "$(PLATFORM)" = "windows" ]; then \
			mv dist/$(BINARY_NAME)_$(VERSION)_$(PLATFORM)_amd64 dist/$(BINARY_NAME)_$(VERSION)_$(PLATFORM)_amd64.exe; \
		fi; \
	)
	cd dist && \
	$(foreach PLATFORM,$(PLATFORMS),\
		tar -czf $(BINARY_NAME)_$(VERSION)_$(PLATFORM)_amd64.tar.gz $(BINARY_NAME)_$(VERSION)_$(PLATFORM)_amd64* ;\
	) 
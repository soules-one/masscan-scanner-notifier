GO_BIN=/usr/local/go/bin/go
BINARY_NAME=scanner

run:
	sudo $(GO_BIN) run ./cmd/main.go

build:
	$(GO_BIN) build -o $(BINARY_NAME) ./cmd/main.go
	@echo "Build complete: ./$(BINARY_NAME)"

build-linux:
	GOOS=linux GOARCH=amd64 $(GO_BIN) build -o $(BINARY_NAME)-linux-amd64 ./cmd/main.go
	@echo "Linux build complete: ./$(BINARY_NAME)-linux-amd64"

build-windows:
	GOOS=windows GOARCH=amd64 $(GO_BIN) build -o $(BINARY_NAME)-windows-amd64.exe ./cmd/main.go
	@echo "Windows build complete: ./$(BINARY_NAME)-windows-amd64.exe"

build-mac-amd64:
	GOOS=darwin GOARCH=amd64 $(GO_BIN) build -o $(BINARY_NAME)-darwin-amd64 ./cmd/main.go
	@echo "Mac (Intel) build complete: ./$(BINARY_NAME)-darwin-amd64"

build-mac-arm64:
	GOOS=darwin GOARCH=arm64 $(GO_BIN) build -o $(BINARY_NAME)-darwin-arm64 ./cmd/main.go
	@echo "Mac (Apple Silicon) build complete: ./$(BINARY_NAME)-darwin-arm64"

build-all: build-linux build-windows build-mac-amd64 build-mac-arm64

clean:
	rm -f $(BINARY_NAME) $(BINARY_NAME)-*
	rm -f ./scanner.db

.PHONY: run build build-linux build-windows build-mac-amd64 build-mac-arm64 build-all clean

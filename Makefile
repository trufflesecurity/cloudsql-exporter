.PHONY: build

lint:
	golangci-lint run ./...

dep:
	go mod tidy

build:
	go build -o build/cloudsql-exporter main.go

test:
	@go clean -testcache
	@go test -v -cover -coverprofile coverage.txt.tmp -race ./...
	@echo
	@cat coverage.txt.tmp | grep -v .pb.go > coverage.txt
	rm coverage.txt.tmp
	@go tool cover -func coverage.txt

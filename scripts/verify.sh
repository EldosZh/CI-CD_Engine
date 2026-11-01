#!/bin/sh
set -eu
go mod tidy
gofmt -w cmd internal
go vet ./...
go test ./...
go test -race ./...
mkdir -p bin
go build -o bin/cicd ./cmd/cicd
./bin/cicd --help
./bin/cicd version
./bin/cicd validate examples/parallel.yaml
./bin/cicd benchmark scheduler --jobs 10000 --workers 32

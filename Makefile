PROTO_DIR := proto
PROTO_SRC := $(wildcard $(PROTO_DIR)/*.proto)

.PHONY: generate-proto build test vet

generate-proto:
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. --go_opt=module=kyc-platform \
		--go-grpc_out=. --go-grpc_opt=module=kyc-platform \
		$(PROTO_SRC)

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

.PHONY: test-race test-integration

test-race:
	go test -race ./...

test-integration:
	TEST_POSTGRES_URL='postgres://kyc:kyc@localhost:5433/verification?sslmode=disable' go test -race -count=1 ./...

BIN       := bin/audit-sink
IMAGE     ?= vm-audit-sink:dev
NAMESPACE ?= vm-audit

.PHONY: all tidy fmt vet build verify image deploy schema clean

all: build

## tidy: Synchronise go.mod and go.sum with source imports.
tidy:
	go mod tidy

## fmt: Format all Go source files in-place.
fmt:
	gofmt -w ./...

## vet: Run go vet across all packages.
vet:
	go vet ./...

## build: Compile the audit-sink binary into bin/.
build:
	@mkdir -p bin
	go build -trimpath -o $(BIN) ./cmd/audit-sink

## verify: Check that source is gofmt-clean and passes go vet (no modifications).
verify: vet
	@dirty=$$(gofmt -l ./...); \
	if [ -n "$$dirty" ]; then \
		printf "gofmt: the following files need formatting:\n%s\n" "$$dirty"; \
		exit 1; \
	fi
	@echo "All checks passed"

## image: Build the container image (requires Docker or Podman on PATH).
image:
	docker build -t $(IMAGE) -f Dockerfile .

## deploy: Apply all manifests (Strimzi and OCP Logging operators must be installed first).
deploy:
	kubectl apply -k deployment/

## schema: Apply schema.sql to the running PostgreSQL pod in $(NAMESPACE).
schema:
	@PGPOD=$$(kubectl get pod -n $(NAMESPACE) -l app=postgres \
		-o jsonpath='{.items[0].metadata.name}'); \
	echo "Applying schema to pod $$PGPOD in namespace $(NAMESPACE)..."; \
	kubectl exec -i -n $(NAMESPACE) "$$PGPOD" -- \
		psql -U audituser -d vmaudit -v ON_ERROR_STOP=1 < schema.sql

## clean: Remove compiled binaries.
clean:
	rm -rf bin/

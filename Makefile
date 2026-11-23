.PHONY: generate lint test integration up down

generate:
	buf generate

lint:
	buf lint
	buf format --diff --exit-code
	golangci-lint run

test:
	go test -race -count=1 ./...

# Needs Docker: starts throwaway Redpanda containers via Testcontainers.
integration:
	go test -tags=integration -race -count=1 ./...

up:
	docker compose up -d --build

down:
	docker compose down

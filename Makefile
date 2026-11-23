.PHONY: generate lint test up down

generate:
	buf generate

lint:
	buf lint
	buf format --diff --exit-code
	golangci-lint run

test:
	go test -race -count=1 ./...

up:
	docker compose up -d

down:
	docker compose down

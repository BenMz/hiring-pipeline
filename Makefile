.PHONY: generate lint breaking test integration up down rebuild

generate:
	buf generate

lint:
	buf lint
	buf format --diff --exit-code
	golangci-lint run

# Fails if the protos break wire or source compatibility with main.
breaking:
	buf breaking --against '.git#branch=main'

test:
	go test -race -count=1 ./...

# Needs Docker: starts throwaway Redpanda containers via Testcontainers.
integration:
	go test -tags=integration -race -count=1 ./...

up:
	docker compose up -d --build

down:
	docker compose down

# Drop the read models and replay the whole topic. The live projector holds a
# lock on the read models, so it is stopped for the duration.
rebuild:
	docker compose stop pipeline-projector
	docker compose run --rm pipeline-projector rebuild
	docker compose start pipeline-projector

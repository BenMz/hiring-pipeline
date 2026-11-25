.PHONY: generate lint breaking test integration up down rebuild demo loadgen notifier-install

generate:
	buf generate

lint: notifier-install
	buf lint
	buf format --diff --exit-code
	golangci-lint run
	cd services/notifier && npm run lint

# Fails if the protos break wire or source compatibility with main.
breaking:
	buf breaking --against '.git#branch=main'

test: notifier-install
	go test -race -count=1 ./...
	cd services/notifier && npm test

# Needs Docker: starts throwaway Redpanda containers via Testcontainers.
integration: notifier-install
	go test -tags=integration -race -count=1 ./...
	cd services/notifier && npm run test:integration

notifier-install:
	cd services/notifier && [ -d node_modules ] || npm ci

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

# Walk through the whole flow against the running stack (needs grpcurl).
demo:
	./scripts/demo.sh

# Write ~100k events through the command API, for measuring a rebuild.
# Stop the notifier first, or it will email every load-test candidate
# and compete for PostgreSQL. See docs/measurements/rebuild-100k.md.
loadgen:
	go run ./tools/loadgen -events 100000

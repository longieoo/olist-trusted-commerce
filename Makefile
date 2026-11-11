.PHONY: build doctor up down seed-fixture batch replay stream-start stream-stop test smoke benchmark-smoke ui integration
DC = docker compose -p olist-final -f compose.yaml -f infra/compose.clean.yaml
build:
	$(DC) build job
doctor:
	$(DC) run --rm job doctor
up:
	$(DC) up -d --wait postgres minio
down:
	$(DC) --profile ui --profile stream --profile orchestration down
seed-fixture:
	$(DC) run --rm job fixture
	$(DC) run --rm job seed
batch:
	$(DC) run --rm job batch
replay:
	$(DC) --profile stream up -d --wait kafka
	$(DC) run --rm job replay
stream-start:
	$(DC) --profile stream up -d --wait kafka
	$(DC) --profile stream up -d stream
stream-stop:
	$(DC) stop stream
test:
	$(DC) run --rm job test
smoke:
	$(DC) run --rm job smoke
benchmark-smoke:
	$(DC) run --rm job benchmark-smoke
ui:
	$(DC) --profile ui up -d dashboard
integration:
	$(DC) run --rm job test --integration

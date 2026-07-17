COMPOSE = docker compose

.PHONY: up down restart logs ps test psql topics clean

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

test:
	cd ingestion && uv run pytest
	cd api && uv run pytest
	$(COMPOSE) run --rm --no-deps --entrypoint python3 spark-ohlcv -m pytest tests/ -q

psql:
	$(COMPOSE) exec timescaledb psql -U $${POSTGRES_USER:-stock} -d $${POSTGRES_DB:-stocks}

topics:
	$(COMPOSE) exec redpanda rpk topic list

# Wipe everything including data volumes
clean:
	$(COMPOSE) down -v

# Stock Streaming Platform — Project Charter

Real-time US stock streaming platform (portfolio project). Monorepo, one folder = one
service = one container. Full blueprint: `docs/project-structure-plan.md`.

## Architecture (data flow)

```
[Finnhub WS | Simulator] → ingestion → Redpanda (trades.raw)
    → processing/spark → TimescaleDB (ohlcv_1m + continuous aggregates)
                       → MinIO (bronze/silver Parquet)
    → api (FastAPI: REST /candles + WS /ws/{symbol}) → frontend (Next.js dashboard)
```

## Services

| Folder | Runtime | Notes |
|---|---|---|
| `ingestion/` | Python 3.12, uv | `TRADE_SOURCE=simulator\|finnhub` — simulator is default (no API key needed) |
| `processing/spark/` | PySpark, local[*] | 1-min event-time windows, 30s watermark, upsert via foreachBatch |
| `storage/timescale/init/` | SQL | Runs via /docker-entrypoint-initdb.d — only on FIRST boot of the volume; `make clean` to re-init |
| `storage/minio/` | sh | Bucket bootstrap (bronze/silver/gold) |
| `api/` | FastAPI, uv | REST reads TimescaleDB; WS consumes trades.raw via aiokafka |
| `frontend/` | Next.js + TS | lightweight-charts candlesticks |

## Commands

- `make up` / `make down` / `make clean` (clean wipes volumes → re-runs SQL init)
- `make logs`, `make ps`, `make topics`, `make psql`
- Tests per service: `cd <service> && uv run pytest`; Spark tests run in-container:
  `docker compose run --rm --no-deps --entrypoint python3 spark-ohlcv -m pytest tests/ -q`
- No `make` on this machine? Use `docker compose up -d --build` etc. directly.

## Conventions

- All config via env vars (12-factor). `.env` never committed; keep `.env.example` current.
- Every Python service: uv + pyproject.toml, ruff, pytest, own Dockerfile + tests/.
- Kafka messages are JSON, keyed by symbol. Schema = Pydantic `Trade` in `ingestion/src/schemas.py`.
- Shell scripts and SQL must stay LF (enforced via .gitattributes) — they run inside Linux containers.
- Commit at the end of each working milestone.

## Ports (host)

Redpanda Kafka 19092 (internal: redpanda:9092) · Console 8080 · TimescaleDB 5432 ·
MinIO 9000/9001 · API 8000 · Frontend 3000

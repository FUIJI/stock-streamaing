# Project Structure Plan — Stock Streaming Platform

โครงสร้าง monorepo สำหรับแพลตฟอร์ม streaming ข้อมูลหุ้นสหรัฐฯ
(Redpanda + Spark Structured Streaming + TimescaleDB + MinIO + Airflow + FastAPI + Next.js)

---

## หลักการออกแบบโครงสร้าง

1. **แยกตาม service ไม่ใช่ตาม layer ของโค้ด** — แต่ละโฟลเดอร์บนสุดมี Dockerfile ของตัวเอง, build image แยกกันใน CI, และ map กับ container ใน docker-compose แบบ 1:1 เปิด repo แล้วเห็นภาพ architecture ทันที (ดีต่อ portfolio)
2. **`processing/` มี Spark และ lite aggregator คู่กัน** — เป็นจุดขายเรื่อง environment-aware tooling: Spark สำหรับ local/dev, Python aggregator สำหรับ production บน VM หน่วยความจำจำกัด สลับด้วย `docker-compose.prod.yml`
3. **Schema เป็น SQL ล้วนใน `storage/timescale/init/`** — mount ผ่าน `/docker-entrypoint-initdb.d` ให้ reproducible โดยไม่ต้องมี migration tool ในเฟสแรก (เพิ่ม Alembic ภายหลังได้)
4. **`CLAUDE.md` อยู่ root** — ใช้ Prompt #0 (project charter) ให้ Claude Code อ่านทุก session

---

## โครงสร้าง Monorepo

```
stock-streaming-platform/
├── CLAUDE.md                        # Project charter (Prompt #0) สำหรับ Claude Code
├── README.md                        # Portfolio README + architecture diagram + demo GIF
├── docker-compose.yml               # Local dev: ยกทั้ง stack ในคำสั่งเดียว
├── docker-compose.prod.yml          # Production override (lite aggregator แทน Spark)
├── .env.example                     # Template ของ env vars (ห้าม commit .env จริง)
├── Makefile                         # คำสั่งลัด: make up, make test, make seed
│
├── .github/
│   └── workflows/
│       ├── ci.yml                   # lint + test + build images → GHCR
│       └── deploy.yml               # SSH deploy ไป VM เมื่อ merge main
│
├── ingestion/                       # Phase 1: Finnhub WebSocket → Redpanda
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── src/
│   │   ├── main.py                  # WebSocket client + producer loop
│   │   ├── config.py                # อ่าน env vars (symbols, topic names)
│   │   └── schemas.py               # Pydantic models ของ trade message
│   └── tests/
│
├── processing/
│   ├── spark/                       # Phase 2: Spark Structured Streaming (local/dev)
│   │   ├── Dockerfile
│   │   ├── jobs/
│   │   │   ├── ohlcv_aggregation.py # Event-time window + watermark 30s
│   │   │   └── bronze_to_silver.py  # เขียน Parquet ลง MinIO
│   │   └── tests/
│   └── lite_aggregator/             # Production: Python aggregator แทน Spark
│       ├── Dockerfile
│       ├── src/
│       │   ├── main.py              # Kafka consumer + in-memory OHLCV windows
│       │   └── writer.py            # Flush ลง TimescaleDB
│       └── tests/
│
├── storage/
│   ├── timescale/
│   │   └── init/                    # SQL init: hypertables, continuous aggregates
│   │       ├── 001_schema.sql
│   │       └── 002_hypertables.sql
│   └── minio/
│       └── init.sh                  # สร้าง buckets: bronze, silver, gold
│
├── orchestration/                   # Airflow: historical backfill
│   ├── Dockerfile
│   ├── dags/
│   │   └── yfinance_backfill.py
│   └── plugins/
│
├── api/                             # FastAPI backend
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── src/
│   │   ├── main.py
│   │   ├── routers/
│   │   │   ├── candles.py           # REST: historical OHLCV
│   │   │   └── stream.py            # WebSocket: real-time push
│   │   ├── db.py                    # asyncpg pool
│   │   └── models.py
│   └── tests/
│
├── frontend/                        # Next.js + TypeScript + lightweight-charts
│   ├── package.json
│   ├── src/
│   │   ├── app/
│   │   │   └── page.tsx             # Dashboard หลัก
│   │   ├── components/
│   │   │   ├── CandlestickChart.tsx
│   │   │   └── SymbolSelector.tsx
│   │   ├── hooks/
│   │   │   └── useWebSocket.ts      # เชื่อม WS ของ FastAPI
│   │   └── lib/
│   │       └── api.ts               # REST client
│   └── ...
│
├── deploy/                          # ฝั่ง VM (Oracle Always Free / Plan B)
│   ├── Caddyfile                    # Reverse proxy + auto HTTPS
│   └── scripts/
│       └── deploy.sh
│
└── docs/
    ├── stock-streaming-project-plan.md
    └── architecture.png             # Diagram สำหรับ README
```

---

## บทบาทของแต่ละ Service

| โฟลเดอร์ | หน้าที่ | เฟส |
|---|---|---|
| `ingestion/` | รับ trade จาก Finnhub WebSocket แล้ว produce เข้า Redpanda | 1 |
| `processing/spark/` | Aggregate OHLCV ด้วย event-time windowing + watermark 30s, เขียน Parquet ลง MinIO (medallion) | 2 |
| `processing/lite_aggregator/` | ทางเลือก production บน VM เล็ก — consumer + in-memory windows → TimescaleDB | Reinforce |
| `storage/timescale/` | Serving layer: hypertables + continuous aggregates | 2–3 |
| `storage/minio/` | Data lake: buckets bronze / silver / gold | 2 |
| `orchestration/` | Airflow DAG backfill ข้อมูลย้อนหลังผ่าน yfinance | Reinforce |
| `api/` | FastAPI — REST (historical) + WebSocket (real-time) | 3 |
| `frontend/` | Next.js dashboard กราฟแท่งเทียน real-time | 6–7 |
| `deploy/` | Caddy + สคริปต์ deploy ไปยัง VM | 7 |
| `.github/workflows/` | CI (lint/test/build → GHCR) + CD (SSH deploy) | Reinforce |

---

## ลำดับการสร้าง (Fast Path: 0 → 1 → 2 → 3 → 6 → 7)

1. **Foundation** — `docker-compose.yml`, `.env.example`, `Makefile`, `storage/` (Redpanda + TimescaleDB + MinIO ต้องขึ้นก่อน)
2. **Ingestion** — `ingestion/` ต่อ Finnhub → Redpanda ให้เห็น message ไหล
3. **Processing** — `processing/spark/` aggregate OHLCV → TimescaleDB + MinIO
4. **API** — `api/` เปิด REST + WebSocket
5. **Frontend** — `frontend/` dashboard กราฟ real-time → **ได้ demo ที่มองเห็นได้**
6. **Deploy** — `deploy/` ขึ้น Oracle VM (หรือ Plan B: Render + Neon + Vercel)

**เฟส Reinforce (หลังมี demo แล้ว):**
- `.github/workflows/` — CI/CD
- `processing/lite_aggregator/` — production aggregator
- `orchestration/` — Airflow backfill

---

## กติกาที่ควรยึดตลอดโปรเจกต์

- ทุก service มี `Dockerfile` + `tests/` ของตัวเอง — CI build และ test แยกกันได้
- Config ทั้งหมดผ่าน environment variables (12-factor) — ไม่ hardcode symbols, topics, credentials
- `.env` ไม่เข้า git เด็ดขาด, มีแค่ `.env.example`
- Secrets ฝั่ง CI เก็บใน GitHub Actions Secrets
- README ต้องมี architecture diagram, demo GIF, และคำอธิบาย trade-off Spark vs lite aggregator (จุดขายเรื่อง environment-aware tooling)

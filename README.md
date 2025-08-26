# Tech Safari – Robo Advisor (public) + Robo Judge client (internal)

Public API (FastAPI):
- `POST /rebalance` – compute orders
- `POST /ack` – acknowledgement
- `GET /health`

**No Robo Judge endpoints are exposed.** Instead, use the internal `RoboJudgeClient` class:
- Stub mode by default (in-memory data)
- Set `ROBO_JUDGE_URL` and `ROBO_JUDGE_API_KEY` to call the real service when available
- Optional env: `ROBO_JUDGE_MODE=stub|remote` (defaults to `stub`)

## Run locally
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## Env for remote Robo Judge (when you get the endpoint)
```bash
export ROBO_JUDGE_MODE=remote
export ROBO_JUDGE_URL=https://<real-host>
export ROBO_JUDGE_API_KEY=<key>
```

## Minimal example (stub mode)
```bash
curl -X POST http://localhost:8000/rebalance -H "Content-Type: application/json" -d '{
  "asOf": "2025-08-26",
  "filters": {"accountIds": ["C001","C004"], "minCashPct": 0.02, "maxSecurityWeight": 0.10, "maxSectorWeight": 0.25},
  "sentimentWeight": 0.20
}'
```

The service will fetch data via `RoboJudgeClient` (stub) and price using an S-curve impact.
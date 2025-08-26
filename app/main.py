from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, timezone
import numpy as np, io, zipfile, hashlib, csv, json

from .judge_client import RoboJudgeClient, find_price, PriceBar
from .judge_client import STUB_CLIENTS, STUB_HOLDINGS, STUB_INDEX, STUB_PRICES, STUB_SENTIMENT
from .judge_client import Client, Holding, IndexConstituent, SentimentRecord, PriceBar as PriceModel

app = FastAPI(
    title="Tech Safari – Robo Advisor – STUB",
    version="0.3.0",
    description="Public endpoints: /ingest/upload, /rebalance, /ack, /health. Robo Judge stays internal.",
)

# Idempotency stores
INGESTED_CHECKSUMS: Dict[str, str] = {}
REBALANCE_RESULTS: Dict[str, dict] = {}
ACKED_REQUESTS: Dict[str, bool] = {}

class RebalanceFilter(BaseModel):
    accountIds: Optional[List[str]] = None
    minCashPct: float = 0.02
    maxSecurityWeight: float = 0.10
    maxSectorWeight: float = 0.25

class RebalanceRequest(BaseModel):
    asOf: str
    filters: RebalanceFilter
    sentimentWeight: float = 0.20
    riskTargetVol: Optional[float] = None

class Order(BaseModel):
    accountId: str
    ticker: str
    side: str   # BUY/SELL
    qty: int
    execPrice: float
    ts: str

class RebalanceResponse(BaseModel):
    requestId: str
    orders: List[Order]

def s_curve_price(prev_close: float, order_qty: int, adv: float,
                  k: float = 4.0, half_adv: float = 0.1) -> float:
    x = (abs(order_qty) / (half_adv * max(adv, 1.0)))
    impact = 1.0 / (1.0 + np.exp(-k*(x-1.0)))
    signed = impact if order_qty > 0 else -impact
    return round(prev_close * (1 + 0.002 * signed), 4)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def current_minute_bucket():
    dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return dt.isoformat()

# ---------- Ingest (multipart zip) ----------
@app.post("/ingest/upload", tags=["Ingest"])
async def ingest_upload(file: UploadFile = File(...),
                        asOf: Optional[str] = None,
                        sourceId: Optional[str] = None,
                        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key")):
    blob = await file.read()
    checksum = "sha256:" + hashlib.sha256(blob).hexdigest()
    key = idempotency_key or checksum

    if key in INGESTED_CHECKSUMS:
        return {"datasetVersion": INGESTED_CHECKSUMS[key], "checksum": checksum,
                "receivedFiles": [], "asOf": asOf, "idempotent": True}

    zf = zipfile.ZipFile(io.BytesIO(blob))
    names = set(zf.namelist())

    required_any = [{"clients.csv", "holdings.csv", "index.csv", "prices.csv", "sentiment.jsonl"},
                    {"clients.csv", "holdings.csv", "index.csv", "prices.parquet", "sentiment.jsonl"}]
    if not any(req.issubset(names) for req in required_any):
        raise HTTPException(status_code=400, detail=f"Zip must include clients.csv, holdings.csv, index.csv, prices.csv (or prices.parquet), sentiment.jsonl. Found: {sorted(names)}")

    def _read_csv(name):
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.DictReader(text)
            return list(reader)

    if "clients.csv" in names:
        rows = _read_csv("clients.csv")
        STUB_CLIENTS.clear()
        for r in rows:
            STUB_CLIENTS.append(Client(
                clientId=r.get("client_id") or r.get("clientId"),
                segment=r.get("segment","retail"),
                riskProfile=r.get("risk_profile") or r.get("riskProfile","balanced")
            ))

    if "holdings.csv" in names:
        rows = _read_csv("holdings.csv")
        STUB_HOLDINGS.clear()
        for r in rows:
            STUB_HOLDINGS.append(Holding(
                accountId=r.get("client_id") or r.get("account_id") or r.get("accountId"),
                ticker=r.get("ticker"),
                qty=int(float(r.get("qty") or r.get("quantity") or "0"))
            ))

    if "index.csv" in names:
        rows = _read_csv("index.csv")
        STUB_INDEX.clear()
        for r in rows:
            STUB_INDEX.append(IndexConstituent(
                ticker=r.get("ticker"),
                weight=float(r.get("weight") or r.get("target_weight") or "0"),
                sector=r.get("sector","Unknown")
            ))

    received_prices = False
    if "prices.csv" in names:
        rows = _read_csv("prices.csv")
        STUB_PRICES.clear()
        for r in rows:
            STUB_PRICES.append(PriceModel(
                date=r.get("date"),
                ticker=r.get("ticker"),
                close=float(r.get("close")),
                adv=float(r.get("adv") or 0) if r.get("adv") not in (None, "") else None
            ))
        received_prices = True

    if "sentiment.jsonl" in names:
        STUB_SENTIMENT.clear()
        with zf.open("sentiment.jsonl") as f:
            for line in io.TextIOWrapper(f, encoding="utf-8"):
                if not line.strip(): continue
                d = json.loads(line)
                STUB_SENTIMENT.append(SentimentRecord(
                    date=d.get("date"),
                    ticker=d.get("ticker"),
                    label=d.get("label"),
                    score=float(d.get("score")),
                    source=d.get("source")
                ))

    dataset_version = f"v{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    INGESTED_CHECKSUMS[key] = dataset_version
    return {
        "datasetVersion": dataset_version,
        "checksum": checksum,
        "receivedFiles": sorted(list(names)),
        "asOf": asOf,
        "parsed": {"prices_csv": received_prices}
    }

# ---------- Rebalance (robust, minute-bucket price) ----------
@app.post("/rebalance", response_model=RebalanceResponse, tags=["RoboAdvisor"])
def rebalance(req: RebalanceRequest,
              request: Request,
              idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key")):
    if idempotency_key and idempotency_key in REBALANCE_RESULTS:
        return REBALANCE_RESULTS[idempotency_key]

    judge = RoboJudgeClient.from_env()

    accounts = req.filters.accountIds
    if not accounts:
        clients = judge.list_clients()["items"]
        accounts = [c["clientId"] for c in clients]

    prices_asof = judge.get_prices(date=req.asOf)
    pb = find_price(prices_asof, "AAPL", req.asOf)

    if pb is None:
        prices_any = judge.get_prices()
        pb = find_price(prices_any, "AAPL")

    if pb is None:
        raise HTTPException(
            status_code=503,
            detail=f"No price found for AAPL (asOf={req.asOf}). Upload prices via /ingest/upload or configure Robo Judge."
        )

    senti = judge.get_sentiment(tickers=["AAPL"])
    senti_aapl = next((s for s in senti if s["ticker"] == "AAPL"), None)
    tilt = ((senti_aapl["score"] - 0.5) * 2) if senti_aapl else 0.0

    bucket = current_minute_bucket()
    exec_price_bucket: Dict[str, float] = {}

    orders: List[Order] = []
    base_qty = 10
    qty_common = max(1, int(round(base_qty * (1 + req.sentimentWeight * tilt))))

    for acc in accounts:
        key = f"AAPL@{bucket}"
        if key not in exec_price_bucket:
            exec_price_bucket[key] = s_curve_price(pb.close, qty_common, pb.adv or 1_000_000)
        orders.append(Order(accountId=acc, ticker="AAPL", side="BUY",
                            qty=qty_common, execPrice=exec_price_bucket[key], ts=now_iso()))
    request_id = f"rb-{int(datetime.now(timezone.utc).timestamp())}"
    resp = RebalanceResponse(requestId=request_id, orders=orders)
    if idempotency_key:
        REBALANCE_RESULTS[idempotency_key] = resp.model_dump()
    return resp

# ---------- ACK (idempotent) ----------
@app.post("/ack", tags=["RoboAdvisor"])
def ack(resp: RebalanceResponse):
    duplicate = ACKED_REQUESTS.get(resp.requestId, False)
    ACKED_REQUESTS[resp.requestId] = True
    return {"status": "ok", "duplicate": duplicate, "received": {"requestId": resp.requestId, "orders": len(resp.orders)}}

@app.get("/health")
def health():
    return {"status": "ok"}
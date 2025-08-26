from __future__ import annotations
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import os

class Client(BaseModel):
    clientId: str
    segment: str
    riskProfile: str
    preferences: Dict[str, Any] = {}

class Holding(BaseModel):
    accountId: str
    ticker: str
    qty: int

class IndexConstituent(BaseModel):
    ticker: str
    weight: float
    sector: str

class PriceBar(BaseModel):
    date: str
    ticker: str
    close: float
    adv: Optional[float] = None

class SentimentRecord(BaseModel):
    date: str
    ticker: str
    label: str  # pos/neg/neu
    score: float
    source: Optional[str] = None

# ---- mutable in-memory stub (updated by /ingest/upload) ----
STUB_CLIENTS: List[Client] = [
    Client(clientId="C001", segment="retail", riskProfile="balanced"),
    Client(clientId="C002", segment="retail", riskProfile="conservative"),
    Client(clientId="C003", segment="hni", riskProfile="growth"),
    Client(clientId="C004", segment="retail", riskProfile="balanced"),
]
STUB_HOLDINGS: List[Holding] = [
    Holding(accountId="C001", ticker="AAPL", qty=120),
    Holding(accountId="C001", ticker="MSFT", qty=80),
    Holding(accountId="C002", ticker="V", qty=50),
    Holding(accountId="C003", ticker="NVDA", qty=30),
    Holding(accountId="C004", ticker="TSLA", qty=20),
    Holding(accountId="C004", ticker="AAPL", qty=15),
]
STUB_INDEX: List[IndexConstituent] = [
    IndexConstituent(ticker="AAPL", weight=0.035, sector="Information Technology"),
    IndexConstituent(ticker="MSFT", weight=0.040, sector="Information Technology"),
    IndexConstituent(ticker="NVDA", weight=0.030, sector="Information Technology"),
    IndexConstituent(ticker="AMZN", weight=0.028, sector="Consumer Discretionary"),
    IndexConstituent(ticker="TSLA", weight=0.020, sector="Consumer Discretionary"),
    IndexConstituent(ticker="V", weight=0.018, sector="Financials"),
]
STUB_PRICES: List[PriceBar] = [
    PriceBar(date="2025-08-25", ticker="AAPL", close=227.13, adv=82000000),
    PriceBar(date="2025-08-25", ticker="MSFT", close=430.55, adv=25000000),
    PriceBar(date="2025-08-25", ticker="NVDA", close=116.22, adv=60000000),
    PriceBar(date="2025-08-25", ticker="AMZN", close=171.40, adv=50000000),
    PriceBar(date="2025-08-25", ticker="TSLA", close=238.65, adv=150000000),
    PriceBar(date="2025-08-25", ticker="V", close=278.90, adv=7000000),
]
STUB_SENTIMENT: List[SentimentRecord] = [
    SentimentRecord(date="2025-08-25", ticker="AAPL", label="pos", score=0.78, source="https://news.example/a"),
    SentimentRecord(date="2025-08-25", ticker="TSLA", label="neg", score=0.66, source="https://news.example/b"),
    SentimentRecord(date="2025-08-25", ticker="MSFT", label="neu", score=0.52, source="https://news.example/c"),
]

class RoboJudgeClient:
    """Internal client. Stub mode unless ROBO_JUDGE_URL is set and ROBO_JUDGE_MODE=remote."""
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None, timeout: float = 10.0):
        self.base_url = base_url
        self.api_key = api_key
        self.timeout = timeout
        self.use_stub = (os.getenv("ROBO_JUDGE_MODE", "stub").lower() == "stub") or not base_url

    @classmethod
    def from_env(cls) -> "RoboJudgeClient":
        return cls(
            base_url=os.getenv("ROBO_JUDGE_URL"),
            api_key=os.getenv("ROBO_JUDGE_API_KEY"),
            timeout=float(os.getenv("ROBO_JUDGE_TIMEOUT", "10.0")),
        )

    def list_clients(self, limit: int = 100, cursor: Optional[str] = None) -> Dict[str, Any]:
        if self.use_stub:
            start = int(cursor) if (cursor or "0").isdigit() else 0
            items = STUB_CLIENTS[start:start+limit]
            next_cursor = str(start+limit) if (start+limit) < len(STUB_CLIENTS) else None
            return {"items": [c.model_dump() for c in items], "nextCursor": next_cursor}
        else:
            import httpx
            with httpx.Client(base_url=self.base_url, headers={"X-API-Key": self.api_key}, timeout=self.timeout) as client:
                r = client.get("/judge/clients", params={"limit": limit, "cursor": cursor})
                r.raise_for_status()
                return r.json()

    def list_holdings(self, account_ids: Optional[list[str]] = None) -> List[dict]:
        if self.use_stub:
            ids = set(account_ids) if account_ids else None
            return [h.model_dump() for h in STUB_HOLDINGS if (ids is None or h.accountId in ids)]
        else:
            import httpx
            params = {"accountIds": ",".join(account_ids)} if account_ids else None
            with httpx.Client(base_url=self.base_url, headers={"X-API-Key": self.api_key}, timeout=self.timeout) as client:
                r = client.get("/judge/holdings", params=params)
                r.raise_for_status()
                return r.json()

    def get_index(self) -> List[dict]:
        if self.use_stub:
            return [c.model_dump() for c in STUB_INDEX]
        else:
            import httpx
            with httpx.Client(base_url=self.base_url, headers={"X-API-Key": self.api_key}, timeout=self.timeout) as client:
                r = client.get("/judge/index")
                r.raise_for_status()
                return r.json()

    def get_prices(self, date: Optional[str] = None, ticker: Optional[str] = None) -> List[dict]:
        if self.use_stub:
            return [p.model_dump() for p in STUB_PRICES if (not date or p.date == date) and (not ticker or p.ticker == ticker)]
        else:
            import httpx
            params = {"date": date, "ticker": ticker}
            with httpx.Client(base_url=self.base_url, headers={"X-API-Key": self.api_key}, timeout=self.timeout) as client:
                r = client.get("/judge/prices", params={k: v for k, v in params.items() if v})
                r.raise_for_status()
                return r.json()

    def get_sentiment(self, from_date: Optional[str] = None, to_date: Optional[str] = None, tickers: Optional[list[str]] = None) -> List[dict]:
        if self.use_stub:
            tick_set = set([t.upper() for t in tickers]) if tickers else None
            res = [s for s in STUB_SENTIMENT if (not tick_set or s.ticker in tick_set)]
            return [s.model_dump() for s in res]
        else:
            import httpx
            params = {"from": from_date, "to": to_date, "tickers": ",".join(tickers) if tickers else None}
            with httpx.Client(base_url=self.base_url, headers={"X-API-Key": self.api_key}, timeout=self.timeout) as client:
                r = client.get("/judge/sentiment", params={k: v for k, v in params.items() if v})
                r.raise_for_status()
                return r.json()

def find_price(prices: list[dict], ticker: str, date: Optional[str] = None) -> Optional[PriceBar]:
    for p in prices:
        if p["ticker"] == ticker and (date is None or p["date"] == date):
            return PriceBar(**p)
    return None
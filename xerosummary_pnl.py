import os
import json
import base64
import requests
import argparse
from datetime import datetime

TOKEN_URL = "https://identity.xero.com/connect/token"
ACCOUNTING_BASE = "https://api.xero.com/api.xro/2.0"
TOKENS_FILE = "xero_tokens.json"
Data_file = "pnl.json"

VALID_TIMEFRAMES = {"MONTH", "QUARTER", "YEAR"}
def valid_date(s: str) -> str:
    datetime.strptime(s, "%Y-%m-%d")
    return s


def load_tokens():
    with open(TOKENS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_tokens(tokens):
    with open(TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)


def refresh_access_token(refresh_token: str) -> dict:
    client_id = os.environ["XERO_CLIENT_ID"]
    client_secret = os.environ["XERO_CLIENT_SECRET"]

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}"}
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


#-----------------same for every case till now------------------------
def fetch_bank_summary_json(
    access_token: str,
    tenant_id: str,
    from_date: str,
    to_date: str,
    periods: int | None = None,
    timeframe: str | None = None,
) -> dict:
    if periods is not None and not (1 <= periods <= 11):
        raise ValueError("periods must be an integer between 1 and 11")
    if timeframe and timeframe not in VALID_TIMEFRAMES:
        raise ValueError(f"timeframe must be one of {sorted(VALID_TIMEFRAMES)}")
    url = f"{ACCOUNTING_BASE}/Reports/ProfitAndLoss"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }
    params = {"fromDate": from_date, "toDate": to_date}
    if periods is not None:
        params["periods"] = periods
    if timeframe:
        params["timeframe"] = timeframe
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def get_bank_summary_json(
    from_date: str,
    to_date: str,
    periods: int | None = None,
    timeframe: str | None = None,
) -> dict:
    tokens = load_tokens()
    refreshed = refresh_access_token(tokens["refresh_token"])
    access_token = refreshed["access_token"]
    tokens["refresh_token"] = refreshed.get("refresh_token", tokens["refresh_token"])
    save_tokens(tokens)
    return fetch_bank_summary_json(
        access_token=access_token,
        tenant_id= os.environ.get("tenant_id") or tokens.get("tenant_id"),
        from_date=from_date,
        to_date=to_date,
        periods=periods,
        timeframe=timeframe,
    )

def build_parser():
    p = argparse.ArgumentParser()
    p.add_argument("from_date", type=valid_date)
    p.add_argument("to_date", type=valid_date)
    p.add_argument("--periods", type=int, default=None)
    p.add_argument("--timeframe", choices=["MONTH", "QUARTER", "YEAR"], default=None)
    return p

def main():
    args = build_parser().parse_args()
    payload = get_bank_summary_json(
        args.from_date,
        args.to_date,
        periods=args.periods,
        timeframe=args.timeframe,
    )

if __name__ == "__main__":
    main()

#Acceptable commands->python xerosummary_pnl.py 2025-01-01 2025-01-31 --periods 6 --timeframe MONTH




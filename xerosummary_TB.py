import os
import json
import base64
import requests
import argparse
from datetime import datetime

TOKEN_URL = "https://identity.xero.com/connect/token"
ACCOUNTING_BASE = "https://api.xero.com/api.xro/2.0"
TOKENS_FILE = "xero_tokens.json"

# This code takes info from xerobootstrap and authentication keys from environment variables/docker 
# and fetches info from get request. gets triggered in process_TB.py


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

def fetch_bank_summary_json(access_token: str, tenant_id: str, to_date: str) -> dict:
    url = f"{ACCOUNTING_BASE}/Reports/TrialBalance"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    params = {"date": to_date}

    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def get_bank_summary_json(to_date: str) -> dict:
    tokens = load_tokens()

    refreshed = refresh_access_token(tokens["refresh_token"])
    access_token = refreshed["access_token"]

    # refresh token can rotate — keep the newest one
    tokens["refresh_token"] = refreshed.get("refresh_token", tokens["refresh_token"])
    save_tokens(tokens)
    tenant_id= os.environ.get("tenant_id") or tokens.get("tenant_id")
    payload = fetch_bank_summary_json(access_token, tenant_id, to_date)
    return payload


#Accepted argument python xerosummary_TB.py 2025-01-31 but gets triggered in process_TB file...

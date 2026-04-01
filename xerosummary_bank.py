import os
import json
import base64
import requests
from typing import Optional, Dict, Any, List
import argparse

TOKEN_URL = "https://identity.xero.com/connect/token"
ACCOUNTING_BASE = "https://api.xero.com/api.xro/2.0"
TOKENS_FILE = "xero_tokens.json"
Data_file = "banktrans.json"


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
def to_xero_datetime(date_str: str) -> str:
    y, m, d = date_str.split("-")
    return f"DateTime({int(y)}, {int(m)}, {int(d)})"


def build_where(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    contact_id: Optional[str] = None
) -> Optional[str]:
    clauses: List[str] = []
    if start_date and end_date:
        if start_date == end_date:
            clauses.append(f'Date={to_xero_datetime(start_date)}')
        else:
            clauses.append(f'Date>={to_xero_datetime(start_date)} and Date<={to_xero_datetime(end_date)}')
    elif start_date and not end_date:
        clauses.append(f'Date>={to_xero_datetime(start_date)}')
    elif end_date and not start_date:
        clauses.append(f'Date<={to_xero_datetime(end_date)}')
    if contact_id:
        clauses.append(f'Contact.ContactID=Guid("{contact_id}")')
    if not clauses:
        return None
    return "and".join([c if " and " in c else c for c in clauses]).replace("andContact", " and Contact")


def fetch_bank_summary_json(
    access_token: str,
    tenant_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    contact_id: Optional[str] = None
) -> dict:
    url: str = f"{ACCOUNTING_BASE}/BankTransactions"
    where: Optional[str] = build_where(start_date=start_date, end_date=end_date, contact_id=contact_id)
    headers: Dict[str, str] = {
        "Authorization": f"Bearer {access_token}",
        "xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }
    params: Dict[str, Any] = {}
    if where:
        params["where"] = where
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def get_bank_summary_json(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    contact_id: Optional[str] = None
) -> dict:
    tokens: Dict[str, Any] = load_tokens()
    refreshed: Dict[str, Any] = refresh_access_token(tokens["refresh_token"])
    access_token: str = refreshed["access_token"]
    tokens["refresh_token"] = refreshed.get("refresh_token", tokens.get("refresh_token"))
    save_tokens(tokens)
    payload: dict = fetch_bank_summary_json(
        access_token=access_token,
        tenant_id= os.environ.get("tenant_id") or tokens.get("tenant_id"),
        start_date=start_date,
        end_date=end_date,
        contact_id=contact_id,
    )
    return payload

#contact_id = "96988e67-ecf9-466d-bfbf-0afa1725a649"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="YYYY-MM-DD")
    parser.add_argument("end_date", help="YYYY-MM-DD")
    #parser.add_argument("contact_id")
    #contact_id = "96988e67-ecf9-466d-bfbf-0afa1725a649"

    args = parser.parse_args()

    databt = get_bank_summary_json(args.start_date, args.end_date)

    with open(Data_file, "w", encoding="utf-8") as f:
        json.dump(databt, f, indent=2)

    print(f"Fetched data for {args.start_date} → {args.end_date}")

if __name__ == "__main__":
    main()



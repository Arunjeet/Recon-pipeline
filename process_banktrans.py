#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import csv
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

# Config model and defaults
@dataclass
class Config:
    input_path: str = "banktrans.json"
    output_path: str = "banktrans.csv"


CONFIG = Config(
    input_path="banktrans.json",
    output_path="banktrans.csv",
)


def parse_xero_date(d: Optional[str]) -> Optional[str]:

    if not d or not isinstance(d, str):
        return None
    m = re.search(r"/Date\((\d+)", d)
    if not m:
        return None
    ts_ms = int(m.group(1))
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def derive_direction(ttype: Optional[str]) -> str:
    if not ttype:
        return "OTHER"
    ttype_u = ttype.upper()
    if "RECEIVE" in ttype_u:
        return "RECEIVE"
    if "SPEND" in ttype_u:
        return "SPEND"
    return "OTHER"


def is_transfer(ttype: Optional[str]) -> bool:
    return bool(ttype and "TRANSFER" in ttype.upper())


def as_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if v in (None, ""):
        return None
    s = str(v).strip().lower()
    if s in {"true", "yes", "y", "1"}:
        return True
    if s in {"false", "no", "n", "0"}:
        return False
    return None


def flatten_bank_transactions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    txs = data.get("BankTransactions", []) or []
    out: List[Dict[str, Any]] = []

    for t in txs:
        # Top-level primitives
        tid = t.get("BankTransactionID")
        ttype = t.get("Type")
        reference = t.get("Reference")
        reconciled = as_bool(t.get("IsReconciled"))
        currency_rate = t.get("CurrencyRate")
        has_attachments = as_bool(t.get("HasAttachments"))
        status = t.get("Status")
        line_amount_types = t.get("LineAmountTypes")
        subtotal = t.get("SubTotal")
        total_tax = t.get("TotalTax")
        total = t.get("Total")
        currency_code = t.get("CurrencyCode")
        date_str = t.get("DateString")
        date_clean = parse_xero_date(t.get("Date"))
        updated_utc_raw = t.get("UpdatedDateUTC")
        updated_date_clean = parse_xero_date(updated_utc_raw)

        # BankAccount
        ba = t.get("BankAccount") or {}
        ba_id = ba.get("AccountID")
        ba_code = ba.get("Code")
        ba_name = ba.get("Name")

        # Contact
        c = t.get("Contact") or {}
        cid = c.get("ContactID")
        cname = c.get("Name")

        # Derived
        direction = derive_direction(ttype)
        transfer_flag = is_transfer(ttype)

        out.append({
            # Identity / status
            "BankTransactionID": tid,
            "Type": ttype,
            "Direction": direction,
            "IsTransfer": transfer_flag,
            "Reference": reference,
            "Status": status,
            "IsReconciled": reconciled,
            "HasAttachments": has_attachments,

            # Amounts / currency
            "LineAmountTypes": line_amount_types,
            "SubTotal": subtotal,
            "TotalTax": total_tax,
            "Total": total,
            "CurrencyCode": currency_code,
            "CurrencyRate": currency_rate,

            # Dates
            "Date": date_clean,
            "DateString": date_str,
            "UpdatedDate": updated_date_clean,
            "UpdatedDateUTC_raw": updated_utc_raw,

            # Bank account
            "BankAccountID": ba_id,
            "BankAccountCode": ba_code,
            "BankAccountName": ba_name,

            # Contact
            "ContactID": cid,
            "ContactName": cname,
        })

    return out


def export_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        print("No rows to export.")
        return

    # Stable header order – includes everything in the provided JSON + derived
    cols = [
        # Identity / categorization
        "BankTransactionID", "Type", "Direction", "IsTransfer", "Reference",
        "Status", "IsReconciled", "HasAttachments",

        # Amounts / currency
        "LineAmountTypes", "SubTotal", "TotalTax", "Total",
        "CurrencyCode", "CurrencyRate",

        # Dates
        "Date", "DateString", "UpdatedDate", "UpdatedDateUTC_raw",

        # Bank account
        "BankAccountID", "BankAccountCode", "BankAccountName",

        # Contact
        "ContactID", "ContactName",
    ]

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def run(config: Config = CONFIG) -> int:

    data = load_json(config.input_path)
    rows = flatten_bank_transactions(data)
    export_csv(rows, config.output_path)
    print(f"Exported {len(rows)} rows → {config.output_path}")
    return len(rows)


if __name__ == "__main__":
    run(CONFIG)
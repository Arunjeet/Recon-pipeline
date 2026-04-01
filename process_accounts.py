#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import csv
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from xerosummary_accounts import get_bank_summary_json
import pandas as pd

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------


def load_accounts() -> Dict[str, Any]:
    return get_bank_summary_json()



def parse_xero_date(d: Optional[str]) -> Optional[str]:
    """
    Convert Xero /Date(1769528067125+0000)/ into YYYY-MM-DD.
    """
    if not d or not isinstance(d, str):
        return None
    m = re.search(r"/Date\((\d+)", d)
    if not m:
        return None
    ts_ms = int(m.group(1))
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def flatten_accounts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    accounts = data.get("Accounts", []) or []
    out: List[Dict[str, Any]] = []

    for acc in accounts:
        row: Dict[str, Any] = {
            "AccountID": acc.get("AccountID"),
            "Name": acc.get("Name"),
            "Status": acc.get("Status"),
            "Type": acc.get("Type"),
            "TaxType": acc.get("TaxType"),
            "Class": acc.get("Class"),
            "EnablePaymentsToAccount": acc.get("EnablePaymentsToAccount"),
            "ShowInExpenseClaims": acc.get("ShowInExpenseClaims"),
            "BankAccountNumber": acc.get("BankAccountNumber"),
            "BankAccountType": acc.get("BankAccountType"),
            "CurrencyCode": acc.get("CurrencyCode"),
            "ReportingCode": acc.get("ReportingCode"),
            "ReportingCodeName": acc.get("ReportingCodeName"),
            "HasAttachments": acc.get("HasAttachments"),
            "AddToWatchlist": acc.get("AddToWatchlist"),
            "UpdatedDateUTC_raw": acc.get("UpdatedDateUTC"),
            "UpdatedDateUTC": parse_xero_date(acc.get("UpdatedDateUTC")),
        }

        # Add any additional unmapped fields
        for k, v in acc.items():
            if k not in row:
                row[k] = v

        out.append(row)

    return out

def accounts_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        print("No account rows to export.")
        return pd.DataFrame()  # empty df

    # Fixed stable header columns
    cols = [
        "AccountID", "Name", "Status", "Type", "TaxType", "Class",
        "EnablePaymentsToAccount", "ShowInExpenseClaims",
        "BankAccountNumber", "BankAccountType",
        "CurrencyCode", "ReportingCode", "ReportingCodeName",
        "HasAttachments", "AddToWatchlist",
        "UpdatedDateUTC", "UpdatedDateUTC_raw",
    ]

    # Add any new dynamic columns, good practice even though we use schema. Flexibility can be adopted...
    for r in rows:
        for k in r.keys():
            if k not in cols:
                cols.append(k)

    # Build DataFrame with consistent columns
    df = pd.DataFrame(rows, columns=cols)

    return df


def trigger_accounts() -> pd.DataFrame:

    report = load_accounts()
    rows = flatten_accounts(report)
    df = accounts_df(rows)
    df.drop_duplicates(subset=["AccountID"],inplace=True)

    return df
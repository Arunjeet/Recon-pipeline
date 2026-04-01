#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import csv
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from xerosummary_manualjournals import get_bank_summary_json
import pandas as pd

def load_journal() -> Dict[str, Any]:
    return get_bank_summary_json()

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def parse_xero_date(d: Optional[str]) -> Optional[str]:
    """
    Convert Xero /Date(1769528067125+0000)/ into YYYY-MM-DD.
    Supports both /Date(1234567890)/ and /Date(1234567890+0000)/.
    """
    if not d or not isinstance(d, str):
        return None
    m = re.search(r"/Date\((\d+)", d)
    if not m:
        return None
    ts_ms = int(m.group(1))
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

# ------------------------------------------------------------
# Core Flattening Logic
# ------------------------------------------------------------
def flatten_manual_journals(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flattens the 'ManualJournals' array into rows for CSV.
    Each manual journal becomes one row (journal lines are summarized as count + JSON).
    """
    mjs = data.get("ManualJournals", []) or []
    out: List[Dict[str, Any]] = []

    for mj in mjs:
        lines = mj.get("JournalLines") or []
        row: Dict[str, Any] = {
            "ManualJournalID": mj.get("ManualJournalID"),
            "Status": mj.get("Status"),
            "Narration": mj.get("Narration"),
            "LineAmountTypes": mj.get("LineAmountTypes"),
            "ShowOnCashBasisReports": mj.get("ShowOnCashBasisReports"),
            "HasAttachments": mj.get("HasAttachments"),

            "Date_raw": mj.get("Date"),
            "Date": parse_xero_date(mj.get("Date")),
            "UpdatedDateUTC_raw": mj.get("UpdatedDateUTC"),
            "UpdatedDateUTC": parse_xero_date(mj.get("UpdatedDateUTC")),

            # Journal lines summary
            "JournalLinesCount": len(lines),
            "JournalLinesJSON": json.dumps(lines, ensure_ascii=False),
        }

        # Auto-include any additional unmapped fields for future-proofing
        for k, v in mj.items():
            if k not in row:
                row[k] = v

        out.append(row)

    return out

# ------------------------------------------------------------
# DF write
# ------------------------------------------------------------
def manual_journals_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        print("No manual journal rows to export.")
        return pd.DataFrame()

    # Stable CSV columns first (common fields)
    cols = [
        "ManualJournalID",
        "Status",
        "Narration",
        "LineAmountTypes",
        "ShowOnCashBasisReports",
        "HasAttachments",
        "Date",
        "Date_raw",
        "UpdatedDateUTC",
        "UpdatedDateUTC_raw",
        "JournalLinesCount",
        "JournalLinesJSON",
    ]

    # Auto-append any extra keys found in data
    for r in rows:
        for k in r.keys():
            if k not in cols:
                cols.append(k)

    # Build DataFrame with consistent column order
    df = pd.DataFrame(rows, columns=cols)

    return df

# ------------------------------------------------------------
# Trigger point
# ------------------------------------------------------------

def trigger_manualjournal() -> pd.DataFrame:

    report = load_journal()
    rows = flatten_manual_journals(report)
    df = manual_journals_df(rows)
    df.drop_duplicates(subset=["ManualJournalID"],inplace=True)
    #print(df)

    return df
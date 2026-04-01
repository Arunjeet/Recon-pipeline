#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import csv
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from xerosummary_journals import runjournal
import pandas as pd


def load_journal() -> Dict[str, Any]:
    return runjournal()


def parse_xero_date(d: Optional[str]) -> Optional[str]:
    if not d or not isinstance(d, str):
        return None
    m = re.search(r"/Date\((\d+)", d)
    if not m:
        return None
    ts_ms = int(m.group(1))
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

def parse_xero_datetime_iso(d: Optional[str]) -> Optional[str]:
    if not d or not isinstance(d, str):
        return None
    m = re.search(r"/Date\((\d+)", d)
    if not m:
        return None
    ts_ms = int(m.group(1))
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_all_keys(rows: List[Dict[str, Any]]) -> List[str]:
    cols = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                cols.append(k)
    return cols

def export_csv(rows: List[Dict[str, Any]], csv_path: str, cols: List[str]) -> None:
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in cols})


def flatten_journals(payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for j in payload:  # payload is a list of Journals
        base_ctx: Dict[str, Any] = {}

        base_ctx["JournalID"] = j.get("JournalID")
        base_ctx["JournalNumber"] = j.get("JournalNumber")
        base_ctx["JournalDate_raw"] = j.get("JournalDate")
        base_ctx["JournalDate"] = parse_xero_date(j.get("JournalDate"))
        base_ctx["CreatedDateUTC_raw"] = j.get("CreatedDateUTC")
        base_ctx["CreatedDateUTC"] = parse_xero_datetime_iso(j.get("CreatedDateUTC"))
        base_ctx["Reference"] = j.get("Reference")
        base_ctx["SourceID"] = j.get("SourceID")
        base_ctx["SourceType"] = j.get("SourceType")

        lines = j.get("JournalLines", []) or []

        for line in lines:
            row: Dict[str, Any] = {}
            row.update(base_ctx)

            row["JournalLineID"] = line.get("JournalLineID")
            row["AccountID"] = line.get("AccountID")
            row["AccountCode"] = line.get("AccountCode")
            row["AccountType"] = line.get("AccountType")
            row["AccountName"] = line.get("AccountName")
            row["Description"] = line.get("Description")
            row["NetAmount"] = line.get("NetAmount")
            row["GrossAmount"] = line.get("GrossAmount")
            row["TaxAmount"] = line.get("TaxAmount")
            row["TaxType"] = line.get("TaxType")
            row["TaxName"] = line.get("TaxName")

            # Debit/Credit logic
            try:
                amt = float(line.get("NetAmount") or 0.0)
            except Exception:
                amt = 0.0
            row["Debit"] = amt if amt > 0 else 0.0
            row["Credit"] = -amt if amt < 0 else 0.0

            # === Tracking Categories Expanded ===
            tc_list = line.get("TrackingCategories") or []
            row["TrackingCategoriesCount"] = len(tc_list)

            # Dynamically expand TC1, TC2, ...
            for i, tc in enumerate(tc_list, start=1):
                prefix = f"TrackingCategory{i}"

                row[f"{prefix}_Name"] = tc.get("Name")
                row[f"{prefix}_Option"] = tc.get("Option")
                row[f"{prefix}_TrackingCategoryID"] = tc.get("TrackingCategoryID")
                row[f"{prefix}_TrackingOptionID"] = tc.get("TrackingOptionID")

            # Capture any extra fields not mapped above
            for k, v in line.items():
                if k not in row:
                    row[k] = v

            out.append(row)

    return out


def journalsdf (rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    # Columns you explicitly want — no extras
    base_cols = [
        "JournalID","JournalNumber","JournalDate","CreatedDateUTC","Reference",
        "SourceID","SourceType","JournalLineID","AccountID","AccountCode",
        "AccountType","AccountName","Description","NetAmount","GrossAmount",
        "TaxAmount","TaxType","TaxName","Debit","Credit","TrackingCategoriesCount",
        "TrackingCategory1_Name","TrackingCategory1_Option",
        "TrackingCategory1_TrackingCategoryID",
        "TrackingCategory1_TrackingOptionID",
        "TrackingCategory2_Name","TrackingCategory2_Option",
        "TrackingCategory2_TrackingCategoryID",
        "TrackingCategory2_TrackingOptionID"
    ]

    all_cols = ensure_all_keys(rows)
    ordered_cols = [c for c in base_cols if c in all_cols]
    df = pd.DataFrame(rows)[ordered_cols]
    return df

def trigger_journal() -> pd.DataFrame:

    report = load_journal()
    rows = flatten_journals(report)
    df = journalsdf(rows)
    df.drop_duplicates(inplace=True)

    return df
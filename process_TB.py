#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import html
from typing import Any, Dict, List, Optional, Union
from xerosummary_TB import get_bank_summary_json
import pandas as pd
from datetime import datetime
import argparse
#----------------------------------------------------------------
def valid_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be YYYY-MM-DD")



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("date", type=valid_date)
    p.add_argument("--stage-json", dest="stage_json", default=None)
    return p


def load_tb_for_date(ondate: datetime) -> Dict[str, Any]:
    return get_bank_summary_json(ondate)


#-------------------------------------------------------------------------------------------MAIN PROCESSING STARTS HERE...

def _currency_to_float(v: Optional[str]) -> Optional[float]:

    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None

    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]

    s = s.replace(",", "")
    for sym in ("$", "£", "€"):
        s = s.replace(sym, "")

    try:
        num = float(s)
        return -num if neg else num
    except ValueError:
        return None


def _base_report(report: Dict[str, Any]) -> Dict[str, Any]:

    return report.get("Reports", [report])[-1] if "Reports" in report else report


def flatten_trial_balance(report: Dict[str, Any]) -> List[Dict[str, Any]]:

    base = _base_report(report)
    out: List[Dict[str, Any]] = []

    def walk(rows: List[Dict[str, Any]], section: Optional[str]):
        for r in rows or []:
            rtype = r.get("RowType")

            if rtype == "Section":
                sec_title = r.get("Title")
                new_section = html.unescape(sec_title) if sec_title is not None else section
                walk(r.get("Rows", []), new_section)

            elif rtype == "Row":
                cells = r.get("Cells", [])

                label = html.unescape(str(cells[0].get("Value", "")).strip()) if len(cells) >= 1 else ""

                # AccountId from attributes on first cell (if present)
                account_id: Optional[str] = None
                if len(cells) >= 1:
                    attrs = cells[0].get("Attributes") or []
                    for a in attrs:
                        if a.get("Id") == "account":
                            account_id = a.get("Value")
                            break

                debit = _currency_to_float(cells[1].get("Value") if len(cells) >= 2 else None)
                credit = _currency_to_float(cells[2].get("Value") if len(cells) >= 3 else None)
                ytd_debit = _currency_to_float(cells[3].get("Value") if len(cells) >= 4 else None)
                ytd_credit = _currency_to_float(cells[4].get("Value") if len(cells) >= 5 else None)

                out.append({
                    "Section": section or "",
                    "Label": label,
                    "Debit": debit,
                    "Credit": credit,
                    "YTD_Debit": ytd_debit,
                    "YTD_Credit": ytd_credit,
                    **({"AccountId": account_id} if account_id else {}),
                })

            # Skip "Header"; drill into nested if present
            else:
                if r.get("Rows"):
                    walk(r["Rows"], section)

    walk(base.get("Rows", []), section=None)
    return out


def to_dataframe_tb(rows: List[Dict[str, Any]], ondate:datetime) -> pd.DataFrame:
    base_cols = ["Section", "Label", "Debit", "Credit", "YTD_Debit", "YTD_Credit"]
    has_account_id = any("AccountId" in r for r in rows)
    cols = ["Date"] + base_cols + (["AccountId"] if has_account_id else [])

    if isinstance(ondate, datetime):
        date_str = ondate.strftime("%Y-%m-%d")
    else:
        date_str = str(ondate)

    if not rows:
        return pd.DataFrame(columns=cols)

    enriched = []
    for r in rows:
        e = dict(r)
        e["Date"] = date_str
        enriched.append({k: e.get(k) for k in cols})

    df = pd.DataFrame(enriched, columns=cols)
    return df

#-----------------------------------------------------------------
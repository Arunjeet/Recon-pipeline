#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import html
from typing import Any, Dict, List, Optional
import argparse
import pandas as pd
from xerosummary_pnl import get_bank_summary_json
from datetime import datetime


def load_pl_json(ondate: datetime, todate: datetime, period: int | None = None, timeframe: str | None = None) -> Dict[str, Any]:
    if period and timeframe:
        return get_bank_summary_json(ondate, todate, period, timeframe)
    elif period:
        return get_bank_summary_json(ondate, todate, period)
    elif timeframe:
        return get_bank_summary_json(ondate, todate, timeframe)
    return get_bank_summary_json(ondate, todate)

#------------------------------------------------------------------------------------------Processing PnL--------

def _currency_to_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
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


def flatten_pl(report: Dict[str, Any], include_summaries: bool = True) -> List[Dict[str, Any]]:
    """
    Returns a list of dicts with keys:
      - Section (str)
      - Label (str)
      - Period (str)   # from Header row, e.g. "31 Jan 25"
      - Amount (float or None)
      - IsSummary (bool)
      - AccountId (optional str)
    """
    base = report.get("Reports", [report])[-1] if "Reports" in report else report
    out: List[Dict[str, Any]] = []
    periods: List[str] = []

    def walk(rows: List[Dict[str, Any]], section: Optional[str]):
        nonlocal periods
        for r in rows or []:
            rtype = r.get("RowType")

            if rtype == "Header":
                cells = r.get("Cells", [])
                # Period columns start from index 1
                periods = [
                    html.unescape(str(c.get("Value", "")).strip())
                    for c in cells[1:]
                ]
                # keep walking; headers can appear multiple times per subtree
                continue

            if rtype == "Section":
                sec_title = r.get("Title")
                new_section = html.unescape(sec_title) if sec_title is not None else section
                walk(r.get("Rows", []), new_section)
                continue

            if rtype == "Row":
                cells = r.get("Cells", [])
                if len(cells) >= 1:
                    label_raw = cells[0].get("Value", "")
                    label = html.unescape(str(label_raw).strip())

                    account_id: Optional[str] = None
                    attrs = cells[0].get("Attributes") or []
                    for a in attrs:
                        if a.get("Id") == "account":
                            account_id = a.get("Value")
                            break

                    # amounts from index 1 onward
                    for i, cell in enumerate(cells[1:], start=1):
                        period = periods[i - 1] if i - 1 < len(periods) else f"col_{i}"
                        amount = _currency_to_float(cell.get("Value"))
                        row = {
                            "Section": section or "",
                            "Label": label,
                            "Period": period,
                            "Amount": amount,
                            "IsSummary": False,
                        }
                        if account_id:
                            row["AccountId"] = account_id
                        out.append(row)
                continue

            if rtype == "SummaryRow":
                if not include_summaries:
                    continue
                cells = r.get("Cells", [])
                if len(cells) >= 1:
                    label = html.unescape(str(cells[0].get("Value", "")).strip())
                    for i, cell in enumerate(cells[1:], start=1):
                        period = periods[i - 1] if i - 1 < len(periods) else f"col_{i}"
                        amount = _currency_to_float(cell.get("Value"))
                        out.append({
                            "Section": section or "",
                            "Label": label,
                            "Period": period,
                            "Amount": amount,
                            "IsSummary": True,
                        })
                continue

            # Unknown / other types: drill down if nested
            if r.get("Rows"):
                walk(r["Rows"], section)

    walk(base.get("Rows", []), section=None)
    return out




def rows_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    cols = ["Section", "Label", "Period", "Amount", "IsSummary", "AccountId"]
    cols = [c for c in cols if any(c in r for r in rows)]

    df = pd.DataFrame(rows)

    # Keep only the selected columns (in stable order)
    df = df[cols]


    return df
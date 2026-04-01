#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from bootstrap import ensure_schema
from insertions import load_TB, load_PNL, load_JOURNALS, load_MANUALJOURNALS, master_load, load_ACCOUNTS
from pnl_comp import run_pandasql_transform
from gsheet import sheetdump


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run TB or PnL or journal or manualjournal or account ETL")
    sub = p.add_subparsers(dest="report", required=True)

    # Journal command: requires no argument. Historic sink...
    journal=sub.add_parser("journal", help="Journal run")

    manualjournal=sub.add_parser("manualjournal", help="manualJournal run")

    account=sub.add_parser("account", help="account run")

    # TB subcommand: requires a single date
    tb = sub.add_parser("tb", help="Trial Balance run")
    tb.add_argument("date", help="As-of date (YYYY-MM-DD)")

    # PnL subcommand: requires from_date and to_date + optional periods/timeframe
    pnl = sub.add_parser("pnl", help="Profit & Loss run")
    pnl.add_argument("from_date", help="Start date (YYYY-MM-DD)")
    pnl.add_argument("to_date", help="End date (YYYY-MM-DD)")
    pnl.add_argument("--periods", type=int, default=None,
                     help="Last N periods ending at to_date (e.g., 2)")
    pnl.add_argument("--timeframe", choices=["MONTH", "QUARTER", "YEAR"], default=None,
                     help="Unit for --periods (e.g., MONTH). If omitted, your loader can default.")

    return p


def main():
    ensure_schema()
    args = build_parser().parse_args()

    master_inserted, master_df = master_load()
    print(master_df)

    if args.report == "tb":
        inserted, df = load_TB(args.date)
        print(f"Inserted rows TB: {inserted}")
        try:
            sheetdump(df,"tb")
        except Exception:
            pass

    elif args.report == "pnl":
        inserted, df = load_PNL(
            args.from_date,
            args.to_date,
            period=args.periods,     # your load_PNL uses (period=unit, timeframe=count)
            timeframe=args.timeframe
        )
        print(f"Inserted rows PnL: {inserted}")

        final=run_pandasql_transform(df,args.to_date)
        sheetdump(final,"pnl")

    elif args.report == "journal":
        inserted, df = load_JOURNALS()
        print(f"Inserted rows journals: {inserted}")

        #final=run_pandasql_transform(df,args.to_date)
        #sheetdump(final,"pnl")
        print(df)


    elif args.report == "manualjournal":
        inserted, df = load_MANUALJOURNALS()
        print(f"Inserted rows manualjournals: {inserted}")

        #final=run_pandasql_transform(df,args.to_date)
        #sheetdump(final,"pnl")
        print(df)

    elif args.report == "account":
        inserted, df = load_ACCOUNTS()
        print(f"Inserted rows accounts: {inserted}")

        #final=run_pandasql_transform(df,args.to_date)
        #sheetdump(final,"pnl")
        print(df)


if __name__ == "__main__":
    main()

#python main.py pnl 2025-01-01 2025-01-31 --periods 6 --timeframe MONTH
#python main.py tb 2025-12-31
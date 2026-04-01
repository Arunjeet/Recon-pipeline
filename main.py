#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from bootstrap import ensure_schema
from transformations import load_bank_raw
from transformations import load_client_raw
import argparse

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run client recon or bank recon...")
    sub = p.add_subparsers(dest="report", required=True)

    # client info for bank recon command: requires no argument. Historic sink...
    client=sub.add_parser("client", help="client bank data info run")
    client.add_argument("--markers", type=str, default=None, help="datamarkers")

    # bank data recon command: requires no argument. Historic sink...
    bank=sub.add_parser("bank", help="account run")
    return p

def main():
    ensure_schema()
    args = build_parser().parse_args()

    if args.report == "bank":
        inserted = load_bank_raw(path=r"bank_docs.xlsx")
        print(f"Inserted rows bank raw: {inserted}")

    #-------------------------------------------------------------

    if args.report == "client":
        l=[]
        if args.markers:
            for i in args.markers:
                l.append(i)
            inserted = load_client_raw(path=r"test.xlsx")
        else:
            inserted = load_client_raw(path=r"test.xlsx")
        
        print(f"Inserted rows client raw: {inserted}")

if __name__ == "__main__":
    main()


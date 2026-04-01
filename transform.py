import argparse
from datetime import datetime
import pandas as pd
from process_TB import load_tb_for_date, flatten_trial_balance, to_dataframe_tb
from process_PNL import load_pl_json, flatten_pl, rows_to_dataframe
from process_journals import trigger_journal
from process_manualjournals import trigger_manualjournal
from process_accounts import trigger_accounts
import hashlib
import json
import os



TOKENS_FILE = "xero_tokens.json"

def load_tokens():
    with open(TOKENS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

tokens = load_tokens()

tenant_id=os.environ.get("tenant_id") or tokens.get("tenant_id")


def master_data():
    # Create a single-row DataFrame with the EXACT column name used later:
    df = pd.DataFrame([{"tenant_id": tenant_id}])  # <-- note: 'tenant_id'
    return df



def normalize_date_col(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """Coerce df[col] to ISO 'YYYY-MM-DD' strings from values like '30 Nov 24', ISO, etc."""
    df = df.copy()
    s = df[col].astype(str).str.strip()

    # 1) Try strict format for '30 Nov 24'
    dt = pd.to_datetime(s, format="%d %b %y", errors="coerce")

    # 2) Fallback to general parser for any other shapes (ISO, 30/11/2024, etc.)
    missing = dt.isna()
    if missing.any():
        dt.loc[missing] = pd.to_datetime(s.loc[missing], errors="coerce", dayfirst=True)

    # Final: as ISO strings; keep blanks for unparsable
    df[col] = dt.dt.strftime("%Y-%m-%d")
    df[col] = df[col].fillna("")  # important when using pandasql substr/date comparisons
    return df


#-------------------------FOR TB------------------------------------------------------------

def transform_tb(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True))

    # extract account code from trailing "(...)" and clean label
    if "label" in df.columns:
        df["accountcode"] = df["label"].astype(str).str.extract(r"\((.*?)\)$")
        df["label"] = df["label"].astype(str).str.replace(r"\(.*?\)$", "", regex=True).str.strip()

    # 3) drop debit, credit if present (we’ll use YTD columns instead)
    df = df.drop(columns=[c for c in ("debit", "credit") if c in df.columns])

    # 4) rename YTD_* to debit/credit if present
    rename_map = {}
    if "ytd_debit" in df.columns:
        rename_map["ytd_debit"] = "debit"
    if "ytd_credit" in df.columns:
        rename_map["ytd_credit"] = "credit"
    if rename_map:
        df = df.rename(columns=rename_map)
    df['tenant_id']=tenant_id

    for col in ("tenant_id","date", "section", "label", "accountid", "accountcode"):
        if col not in df.columns:
            df[col] = ""

    normalized_key = (
        df["tenant_id"].astype(str).str.strip().str.lower()       + "|" +
        df["date"].astype(str).str.strip().str.lower()       + "|" +
        df["section"].astype(str).str.strip().str.lower()     + "|" +
        df["label"].astype(str).str.strip().str.lower()       + "|" +
        df["accountid"].astype(str).str.strip().str.lower()   + "|" +
        df["accountcode"].astype(str).str.strip().str.lower()
    )

    df["row_hash"] = normalized_key.apply(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())   
    s = df.pop("row_hash")
    df.insert(0, "row_hash", s)
    
    s = df.pop("tenant_id")
    df.insert(0, "tenant_id", tenant_id)
    return df



def trigger_TB(date_str: str) -> pd.DataFrame:
    ondate = datetime.strptime(date_str, "%Y-%m-%d")
    report = load_tb_for_date(ondate)
    rows = flatten_trial_balance(report)
    df = to_dataframe_tb(rows, ondate)
    df = transform_tb(df)
    print(df)
    return df



#------------------------------------FOR PnL---------------------------------------



def transform_pnl(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True))

    df.rename(columns={"period": "date"}, inplace=True)

    df = normalize_date_col(df, "date")

    s = df.pop("date")
    df.insert(0, "date", s)

    df = df.sort_values(by="date", ascending=True)

    df['tenant_id']=tenant_id

    # ensure required columns exist (empty if missing)
    for col in ["tenant_id","date", "section", "label", "amount", "accountid"]:
        if col not in df.columns:
            df[col] = ""

    normalized_key = (
        df["tenant_id"].astype(str).str.strip().str.lower()   + "|" +
        df["date"].astype(str).str.strip().str.lower()   + "|" +
        df["section"].astype(str).str.strip().str.lower()    + "|" +
        df["label"].astype(str).str.strip().str.lower()      + "|" +
        df["amount"].astype(str).str.strip().str.lower()  + "|" +
        df["accountid"].astype(str).str.strip().str.lower()
    )

    df["row_hash"] = normalized_key.apply(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())
    s = df.pop("row_hash")
    df.insert(0, "row_hash", s)

    s = df.pop("tenant_id")
    df.insert(0, "tenant_id", tenant_id)

    return df

def trigger_pnl(fromdate: str, todate: str, period: int | None = None, timeframe: str | None = None) -> pd.DataFrame:

    ondate = datetime.strptime(fromdate, "%Y-%m-%d")
    tilldate = datetime.strptime(todate, "%Y-%m-%d")

    if period and timeframe:
        report =  load_pl_json(ondate, tilldate, period, timeframe)
    elif period:
        report =  load_pl_json(ondate,tilldate,period)
    elif timeframe:
        report =  load_pl_json(ondate,tilldate,timeframe)
    else:
        report = load_pl_json(ondate,tilldate)

    
    rows = flatten_pl(report)
    df = rows_to_dataframe(rows)
    df = transform_pnl(df)

    #print(df)
    return df



#------------------------------------FOR Jornals---------------------------------------

def transform_journal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True))

    df = df.drop(columns=[c for c in ("journalid") if c in df.columns])

    df = normalize_date_col(df, "journaldate")

    df['tenant_id']=tenant_id

    rename_map = {}
    if "journalnumber" in df.columns:
        rename_map["journalnumber"] = "referencenumber"
    if "sourceid" in df.columns:
        rename_map["sourceid"] = "journalid"
    if rename_map:
        df = df.rename(columns=rename_map)

    if max(list(df['trackingcategoriescount']))>1:

        df = df[["tenant_id","journallineid","referencenumber","journalid","journaldate",
        "accountid","accountcode","accounttype","accountname","description","sourcetype","reference",
        "netamount","grossamount","taxamount","taxtype","taxname","debit","credit","createddateutc",
        "trackingcategoriescount",
        "trackingcategory1_name","trackingcategory1_option",
        "trackingcategory1_trackingcategoryid",
        "trackingcategory1_trackingoptionid",
        "trackingcategory2_name","trackingcategory2_option",
        "trackingcategory2_trackingcategoryid",
        "trackingcategory2_trackingoptionid"
        ]]

        return df
    if max(list(df['trackingcategoriescount']))==1:

        df = df[["tenant_id","journallineid","referencenumber","journalid","journaldate",
        "accountid","accountcode","accounttype","accountname","description","sourcetype","reference",
        "netamount","grossamount","taxamount","taxtype","taxname","debit","credit","createddateutc",
        "trackingcategoriescount",
        "trackingcategory1_name","trackingcategory1_option",
        "trackingcategory1_trackingcategoryid",
        "trackingcategory1_trackingoptionid"
        ]]

        return df

    else:

        df = df[["tenant_id","journallineid","referencenumber","journalid","journaldate",
        "accountid","accountcode","accounttype","accountname","description","sourcetype","reference",
        "netamount","grossamount","taxamount","taxtype","taxname","debit","credit","createddateutc",
        "trackingcategoriescount"
        ]]

        return df



def trigger_journals() -> pd.DataFrame:
    df = trigger_journal()
    df = transform_journal(df)
    #print(df)
    return df


#----------------------------------------------------For Manual Journal------------------------------------------


def transform_manualjournal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True))

    df = df.drop(columns=[c for c in ("date_raw","updateddateutc_raw","journallinescount","journallinesjson","journallines") if c in df.columns])

    df = normalize_date_col(df, "date")
    df = normalize_date_col(df, "updateddateutc")

    df['tenant_id']=tenant_id
    rename_map = {}
    if "narration" in df.columns:
        rename_map["narration"] = "description"
    
    if rename_map:
        df = df.rename(columns=rename_map)

    df = df[[
    "tenant_id",
    "manualjournalid",
    "status",
    "description",
    "date",
    "updateddateutc",
    "lineamounttypes",
    "showoncashbasisreports",
    "hasattachments"]]

    return df


def trigger_manualjournals() -> pd.DataFrame:
    df = trigger_manualjournal()
    df = transform_manualjournal(df)

    return df


#---------------------------------------FOR ACCOUNTS---------------------------------------------------



def transform_accounts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True))

    df = df.drop(columns=[c for c in ("updateddateutc_raw") if c in df.columns])

    df = normalize_date_col(df, "updateddateutc")

    df['tenant_id']=tenant_id

    df = df[[
    "tenant_id",
    "accountid",
    "code",
    "name",
    "status",
    "type",
    "taxtype",
    "class",
    "enablepaymentstoaccount",
    "showinexpenseclaims",
    "bankaccountnumber",
    "bankaccounttype",
    "currencycode",
    "reportingcode",
    "reportingcodename",
    "hasattachments",
    "addtowatchlist",
    "updateddateutc",
    "description",
    "reportingname",
    "systemaccount"]]

    return df


def trigger_account() -> pd.DataFrame:
    df = trigger_accounts()
    df = transform_accounts(df)

    return df


#------------------------------------------------------------------------------------------------------------------------------------------------------------------

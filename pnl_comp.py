import pandas as pd
from pandasql import sqldf

def dump(df: pd.DataFrame, from_date: str) -> pd.DataFrame:

    month_prefix = from_date[:7]  # 'YYYY-MM'
    return df[df["date"].astype(str).str[:7] == month_prefix]



def run_pandasql_transform(df: pd.DataFrame, from_date: str) -> pd.DataFrame:


    q_dedup = """
        SELECT date, label, section, amount, issummary, accountid FROM df
        GROUP BY date, label, section, amount, issummary, accountid
    """
    df_dedup = sqldf(q_dedup, {"df": df})

    # 2) Filter out rows in the same YYYY-MM as from_date
    month_prefix = from_date[:7]  # 'YYYY-MM'
    q_df1 = f"""
        SELECT *
        FROM df
        WHERE substr(date, 1, 7) != '{month_prefix}'
    """
    df1 = sqldf(q_df1, {"df": df})
    #---------------------------------------------------------------------------

    # 3) Pivot df1: dates -> columns (no aggregation)

    df1_pivot = (
        df1.assign(date=lambda d: d["date"].astype(str))
            .pivot_table(
               index=["label", "section", "issummary"],
               columns="date",
               values="amount",
               aggfunc="sum"
            ).reset_index())

        # Optional: order date columns
    fixed = ["label", "section", "issummary"]
    date_cols = sorted([c for c in df1_pivot.columns if c not in fixed])
    df1_pivot = df1_pivot[date_cols+fixed]

    # 4) Merge side-by-side on label & section (as requested)
    #    If either side is empty, this still returns a sensible frame.
    if df1_pivot.empty:
        final = df_dedup.copy()
    elif df_dedup.empty:
        final = df1_pivot.copy()
    else:
        final = pd.merge(
            df_dedup,
            df1_pivot,
            on=["label", "section", "issummary"],
            how="outer"
        )
    print(final)

    return dump(final, from_date)

    #python main.py pnl 2025-01-01 2025-01-31 --periods 6 --timeframe MONTH
import pandas as pd
import re

# Ensure proper column names...
def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    alias_map = {
        "date": ["date", "txn_date", "value_date", "transaction_date", "timestamp"],
        "description": ["description", "particular", "particulars", "transaction", "details", "narrative", "reference"],
        "amount": ["amount", "value", "amt", "debit_credit", "total"],
        "counterpart_coding": ["counterpart_coding", "counterpart", "counterparty", "coding"],
        "talos_name": ["talos", "talos_name", "client_name", "customer"],
    }

    out = df.copy()

    for canon, aliases in alias_map.items():
        present = [a for a in aliases if a in out.columns]
        if present:
            out[canon] = out[present].bfill(axis=1).iloc[:, 0]

    # Ensure required columns exist
    must = ["date", "description", "amount"]
    optional = ["counterpart_coding", "talos_name"]
    for c in must + optional:
        if c not in out.columns:
            out[c] = None

    out = out[out["date"].notna() & out["description"].notna() & out["amount"].notna()]
    out = out[out["description"].astype(str).str.strip() != ""]
    out = out[out["date"].astype(str).str.strip() != ""]


    return out[must + optional]



#Bank files...


def bankfunc(path, sheet=None, markers=["Totals"], lower_cols=True):
    def colname(name: str, lower=True) -> str:
        s = re.sub(r'\W+', '_', str(name))
        s = re.sub(r'_+', '_', s).strip('_')
        return s.lower() if lower else s

    def norm_cell(x: object) -> str:
        return colname(str(x), True)

    def find_first_alias_anchor(df_raw: pd.DataFrame, aliases):
        alias_set = {colname(a, True) for a in aliases}
        for i in range(df_raw.shape[0]):
            for j in range(df_raw.shape[1]):
                if norm_cell(df_raw.iat[i, j]) in alias_set:
                    return i, j
        return None

    def find_first_marker(df_raw: pd.DataFrame, marker: str):
        m = colname(marker, True)
        for i in range(df_raw.shape[0]):
            for j in range(df_raw.shape[1]):
                if m in norm_cell(df_raw.iat[i, j]):
                    return i, j
        return None

    def build_from_anchor(df_block: pd.DataFrame, anchor_rc):
        r, c = anchor_rc
        sub = df_block.iloc[r:, c:].reset_index(drop=True)
        header = sub.iloc[0].astype(str).tolist()
        body = sub.iloc[1:].reset_index(drop=True)
        body.columns = [colname(h, lower_cols) for h in header]
        return body

    def process_sheet(raw: pd.DataFrame, markers):
        date_aliases = ["date", "txn_date", "value_date", "transaction_date", "timestamp", "Timestamp"]

        if markers is None or (isinstance(markers, (list, tuple)) and len(markers) == 0):
            anchor = find_first_alias_anchor(raw, date_aliases)
            if anchor is not None:
                return build_from_anchor(raw, anchor)
            sub = raw.reset_index(drop=True)
            header = sub.iloc[0].astype(str).tolist()
            body = sub.iloc[1:].reset_index(drop=True)
            body.columns = [colname(h, lower_cols) for h in header]
            return body

        if isinstance(markers, (list, tuple)) and len(markers) == 1 or isinstance(markers, str):
            mk = markers[0] if isinstance(markers, (list, tuple)) else markers
            pos = find_first_marker(raw, mk)
            block = raw.iloc[pos[0] + 1:].reset_index(drop=True) if pos is not None else raw
            anchor = find_first_alias_anchor(block, date_aliases)
            if anchor is not None:
                return build_from_anchor(block, anchor)
            header = block.iloc[0].astype(str).tolist()
            body = block.iloc[1:].reset_index(drop=True)
            body.columns = [colname(h, lower_cols) for h in header]
            return body

        if isinstance(markers, (list, tuple)) and len(markers) >= 2:
            m1, m2 = markers[0], markers[1]
            pos1 = find_first_marker(raw, m1)
            pos2 = find_first_marker(raw, m2)
            if pos1 is not None and pos2 is not None and pos2[0] > pos1[0]:
                block = raw.iloc[pos1[0] + 1:pos2[0]].reset_index(drop=True)
            elif pos1 is not None:
                block = raw.iloc[pos1[0] + 1:].reset_index(drop=True)
            else:
                block = raw
            anchor = find_first_alias_anchor(block, date_aliases)
            if anchor is not None:
                return build_from_anchor(block, anchor)
            header = block.iloc[0].astype(str).tolist()
            body = block.iloc[1:].reset_index(drop=True)
            body.columns = [colname(h, lower_cols) for h in header]
            return body

        anchor = find_first_alias_anchor(raw, date_aliases)
        if anchor is not None:
            return build_from_anchor(raw, anchor)
        sub = raw.reset_index(drop=True)
        header = sub.iloc[0].astype(str).tolist()
        body = sub.iloc[1:].reset_index(drop=True)
        body.columns = [colname(h, lower_cols) for h in header]
        return body

    xls = pd.ExcelFile(path)

    if sheet is not None:
        if markers is None or (isinstance(markers, (list, tuple)) and len(markers) == 0):
            raise ValueError("A marker must be provided when a specific sheet name is supplied.")
        raw = pd.read_excel(path, sheet_name=sheet, header=None)
        df_out = process_sheet(raw, markers)
        df_out = canonicalize_columns(df_out)
        df_out["bankname"] = sheet
        df_out.drop_duplicates(subset=["ref_num"])
        return df_out

    frames = []
    for sh in xls.sheet_names:
        raw = pd.read_excel(path, sheet_name=sh, header=None)
        df_out = process_sheet(raw, markers)
        df_out = canonicalize_columns(df_out)
        df_out["bankname"] = sh
        frames.append(df_out)

    if not frames:
        return pd.DataFrame()

    final=pd.concat(frames, ignore_index=True)
    return final.drop_duplicates(subset=["ref_num"])

#------------------------------------------------------------------------------
#Client file...

def clientfunc(path, sheet=None, markers=None, lower_cols=True):
    def colname(name: str, lower=True) -> str:
        s = re.sub(r'\W+', '_', str(name))
        s = re.sub(r'_+', '_', s).strip('_')
        return s.lower() if lower else s

    def norm_cell(x: object) -> str:
        return colname(str(x), True)

    def find_first_alias_anchor(df_raw: pd.DataFrame, aliases):
        alias_set = {colname(a, True) for a in aliases}
        for i in range(df_raw.shape[0]):
            for j in range(df_raw.shape[1]):
                if norm_cell(df_raw.iat[i, j]) in alias_set:
                    return i, j
        return None

    def find_first_marker(df_raw: pd.DataFrame, marker: str):
        m = colname(marker, True)
        for i in range(df_raw.shape[0]):
            for j in range(df_raw.shape[1]):
                if m in norm_cell(df_raw.iat[i, j]):
                    return i, j
        return None

    def build_from_anchor(df_block: pd.DataFrame, anchor_rc):
        r, c = anchor_rc
        sub = df_block.iloc[r:,c:].reset_index(drop=True)
        header = sub.iloc[0].astype(str).tolist()
        body = sub.iloc[1:].reset_index(drop=True)
        body.columns = [colname(h, lower_cols) for h in header]
        return body

    def process_sheet(raw: pd.DataFrame, markers):
        date_aliases = ["date", "txn_date", "value_date", "transaction_date", "timestamp", "Timestamp"]

        if markers is None or (isinstance(markers, (list, tuple)) and len(markers) == 0):
            anchor = find_first_alias_anchor(raw, date_aliases)
            if anchor is not None:
                return build_from_anchor(raw, anchor)
            sub = raw.reset_index(drop=True)
            header = sub.iloc[0].astype(str).tolist()
            body = sub.iloc[1:].reset_index(drop=True)
            body.columns = [colname(h, lower_cols) for h in header]
            return body

        if isinstance(markers, (list, tuple)) and len(markers) == 1 or isinstance(markers, str):
            mk = markers[0] if isinstance(markers, (list, tuple)) else markers
            pos = find_first_marker(raw, mk)
            block = raw.iloc[pos[0] + 1:].reset_index(drop=True) if pos is not None else raw
            anchor = find_first_alias_anchor(block, date_aliases)
            if anchor is not None:
                return build_from_anchor(block, anchor)
            header = block.iloc[0].astype(str).tolist()
            body = block.iloc[1:].reset_index(drop=True)
            body.columns = [colname(h, lower_cols) for h in header]
            return body

        if isinstance(markers, (list, tuple)) and len(markers) >= 2:
            m1, m2 = markers[0], markers[1]
            pos1 = find_first_marker(raw, m1)
            pos2 = find_first_marker(raw, m2)
            if pos1 is not None and pos2 is not None and pos2[0] > pos1[0]:
                block = raw.iloc[pos1[0] + 1:pos2[0]].reset_index(drop=True)
            elif pos1 is not None:
                block = raw.iloc[pos1[0] + 1:].reset_index(drop=True)
            else:
                block = raw
            anchor = find_first_alias_anchor(block, date_aliases)
            if anchor is not None:
                return build_from_anchor(block, anchor)
            header = block.iloc[0].astype(str).tolist()
            body = block.iloc[1:].reset_index(drop=True)
            body.columns = [colname(h, lower_cols) for h in header]
            return body

        anchor = find_first_alias_anchor(raw, date_aliases)
        if anchor is not None:
            return build_from_anchor(raw, anchor)
        sub = raw.reset_index(drop=True)
        header = sub.iloc[0].astype(str).tolist()
        body = sub.iloc[1:].reset_index(drop=True)
        body.columns = [colname(h, lower_cols) for h in header]
        return body

    xls = pd.ExcelFile(path)

    if sheet is not None:
        if markers is None or (isinstance(markers, (list, tuple)) and len(markers) == 0):
            raise ValueError("A marker must be provided when a specific sheet name is supplied.")
        raw = pd.read_excel(path, sheet_name=sheet, header=None)
        df_out = process_sheet(raw, markers)
        df_out = canonicalize_columns(df_out)
        df_out["bankname"] = sheet
        return df_out

    frames = []
    for sh in xls.sheet_names:
        raw = pd.read_excel(path, sheet_name=sh, header=None)
        df_out = process_sheet(raw, markers)
        df_out = canonicalize_columns(df_out)
        df_out["bankname"] = sh
        frames.append(df_out)

    if not frames:
        return pd.DataFrame()

    final=pd.concat(frames, ignore_index=True)
    return final.drop_duplicates(subset=["bankname","date","description","amount"])




import pandas as pd
import re


#Bank files...
def bankfunc(path, sheet, cols):

    data_raw = pd.read_excel(path, sheet_name=sheet)

    # Find positions of markers
    col = data_raw.columns[0]
    mask = data_raw[col].isin(["Plus Unreconciled Statement Lines", "Total Unreconciled Statement Lines"])
    marker_positions = mask[mask].index.to_series().apply(data_raw.index.get_loc).tolist()

    if len(marker_positions) < 2:
        raise ValueError("Both markers not found")

    start, end = marker_positions[0] + 1, marker_positions[1]
    data_raw = data_raw.iloc[start:end].reset_index(drop=True)


    if len(cols) != data_raw.shape[1]:
        raise ValueError("Column count mismatch")
    data_raw.columns = cols
    data_raw.drop(['unnamed1'],axis=1,inplace=True)
    data_raw['transaction']=data_raw['transaction'].str.strip()

    return data_raw

#Client file...

def clientfunc(path, sheet, cols):
    data_raw = pd.read_excel(path, sheet_name=sheet)


    def colname(name: str, lower=True) -> str:
        s = re.sub(r'\W+', '_', name)     # non [A-Za-z0-9_] → _
        s = re.sub(r'_+', '_', s)         # collapse multiple _
        s = s.strip('_')                  # trim leading/trailing _
        return s.lower() if lower else s

    # Find positions of markers
    data_raw.columns=[colname(i) for i in data_raw.columns]
    #mask = data_raw[col].isin(["Plus Unreconciled Statement Lines", "Total Unreconciled Statement Lines"])
    #marker_positions = mask[mask].index.to_series().apply(data_raw.index.get_loc).tolist()
    """
    if len(marker_positions) < 2:
        raise ValueError("Both markers not found")

    start, end = marker_positions[0] + 1, marker_positions[1]
    data_raw = data_raw.iloc[start:end].reset_index(drop=True)"""

    return data_raw



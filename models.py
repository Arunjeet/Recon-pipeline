from typing import Optional
from datetime import datetime
import pandas as pd
from pydantic import BaseModel, Field, AliasChoices, field_validator

# For data cleaning and basic preprocessing....[List of acceptable columns]
# Manually editable....
collist={"date": ["date", "txn_date", "value_date", "transaction_date","timestamp"],
    "description": ["description","particular", "transaction", "particulars","details", "narrative","reference"],
    "amount":      ["amount", "value", "amt", "debit_credit","total"],
    # optional fields
    "counterpart_coding": ["counterpart_coding","counterpart", "counterparty", "coding"],
    "talos_name":         ["talos", "talos_name", "client_name","customer"]
}

# Do not edit..................
def _to_date_str(v) -> str:

    ts = pd.to_datetime(v, errors='coerce')

    if pd.isna(ts):
        # This mimics the original outcome of strftime on NaT -> 'NaT'
        return "NaT"
    return ts.strftime("%Y-%m-%d")


def _to_float(v) -> float:
    
    if v is None:
        return float("nan")
    
    # Handle commas...
    if isinstance(v, str):
        v_str = v.strip().replace(",", "")
        try:
            return float(v_str)
        except ValueError:
            return float("nan")
    try:
        return float(v)
    except Exception:
        return float("nan")


def _strip_or_none(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def _strip_must_str(v) -> str:
    return str(v).strip()



def _to_bool(v) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    if isinstance(v, bool):
        return v
    return None



# Pydantic models called in validation functions..........................


class BankRawModel(BaseModel):
    date: str = Field(validation_alias=AliasChoices(*collist["date"]))
    description: Optional[str] = Field(default=None, validation_alias=AliasChoices(*collist["description"]))
    amount: float = Field(validation_alias=AliasChoices(*collist["amount"]))
    banktransactionid: str #Autoincremental
    direction: Optional[str] = None
    istransfer: Optional[bool] = None
    isreconciled: Optional[bool] = None
    bankaccountid: Optional[str] = None
    bankname: Optional[str] = None
    contactname: Optional[str] = None
    hasattachment: Optional[bool] = None

    # --- Validators ---

    @field_validator("date", mode="before")
    @classmethod
    def _v_date(cls, v):
        return _to_date_str(v)

    @field_validator("description", mode="before")
    @classmethod
    def _v_reference(cls, v):
        return _strip_or_none(v)

    @field_validator("amount", mode="before")
    @classmethod
    def _v_total(cls, v):
        return _to_float(v)

    @field_validator("banktransactionid", mode="before")
    @classmethod
    def _v_banktransactionid(cls, v):
        return _strip_must_str(v)

    @field_validator("direction", mode="before")
    @classmethod
    def _v_direction(cls, v):
        s = _strip_or_none(v)
        return s.lower() if isinstance(s, str) else s

    @field_validator("istransfer", "isreconciled", "hasattachment", mode="before")
    @classmethod
    def _v_bools(cls, v):
        return _to_bool(v)

    @field_validator("bankaccountid", "bankname", "contactname", mode="before")
    @classmethod
    def _v_strings_nullable(cls, v):
        return _strip_or_none(v)







#--------------------------------------------------------------------------------------------------------------


class ClientRawModel(BaseModel):
    date: str = Field(validation_alias=AliasChoices(*collist["date"]))
    bankname: str
    description: str = Field(validation_alias=AliasChoices(*collist["description"]))
    amount: float = Field(validation_alias=AliasChoices(*collist["amount"]))
    counterpart_coding: Optional[str] = Field(default=None, validation_alias=AliasChoices(*collist["counterpart_coding"]))
    talos_name: Optional[str] = Field(default=None, validation_alias=AliasChoices(*collist["talos_name"]))

    @field_validator("date", mode="before")
    @classmethod
    def normalize_date(cls, v):
        return _to_date_str(v)

    @field_validator("bankname", mode="before")
    @classmethod
    def clean_bankname(cls, v):
        return _strip_must_str(v)

    @field_validator("description", mode="before")
    @classmethod
    def clean_description(cls, v):
        return _strip_must_str(v)

    @field_validator("amount", mode="before")
    @classmethod
    def convert_amount(cls, v):
        return _to_float(v)

    @field_validator("counterpart_coding", "talos_name", mode="before")
    @classmethod
    def clean_optional(cls, v):
        return _strip_or_none(v)


# Validation function called in transformations.py.................

#---------------------------------------BANK------------------------------------------
def validate_bank_df(df) -> list[dict]:

    validated_rows = []
    for row in df.to_dict(orient="records"):
        model = BankRawModel.model_validate(row)
        validated_rows.append(model.model_dump())
    return validated_rows

#---------------------------------------CLIENT-------------------------------------------
def validate_client_df(df) -> list[dict]:

    validated_rows = []
    for row in df.to_dict(orient="records"):
        model = ClientRawModel.model_validate(row)
        validated_rows.append(model.model_dump())
    return validated_rows
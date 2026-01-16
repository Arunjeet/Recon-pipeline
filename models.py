from typing import Optional
from datetime import datetime
import pandas as pd
from pydantic import BaseModel, Field, AliasChoices, field_validator

# For data cleaning and basic preprocessing....

def _to_date_str(v) -> str:
    """
    Convert anything to a '%Y-%m-%d' string, mimicking the original pandas logic:
    pd.to_datetime(..., errors='coerce').dt.strftime('%Y-%m-%d')
    Note: NaT -> 'NaT' (same as original behavior when using .dt.strftime)
    """
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


# Pydantic models called in validation functions..........................

class BankRawModel(BaseModel):
    date: str
    description: str = Field(validation_alias=AliasChoices("description", "transaction"))
    amount: float

    @field_validator("date", mode="before")
    @classmethod
    def normalize_date(cls, v):
        return _to_date_str(v)

    @field_validator("description", mode="before")
    @classmethod
    def clean_description(cls, v):
        return _strip_must_str(v)

    @field_validator("amount", mode="before")
    @classmethod
    def convert_amount(cls, v):
        return _to_float(v)


class ClientRawModel(BaseModel):
    date: str
    description: str
    amount: float
    counterpart_coding: Optional[str] = None
    talos_name: Optional[str] = None

    @field_validator("date", mode="before")
    @classmethod
    def normalize_date(cls, v):
        return _to_date_str(v)

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

def validate_bank_df(df) -> list[dict]:

    validated_rows = []
    for row in df.to_dict(orient="records"):
        model = BankRawModel.model_validate(row)
        validated_rows.append(model.model_dump())
    return validated_rows


def validate_client_df(df) -> list[dict]:

    validated_rows = []
    for row in df.to_dict(orient="records"):
        model = ClientRawModel.model_validate(row)
        validated_rows.append(model.model_dump())
    return validated_rows
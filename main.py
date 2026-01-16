# main.py
from bootstrap import ensure_schema
from transformations import load_bank_raw
from transformations import load_client_raw

def main():
    ensure_schema()
    inserted = load_bank_raw(
        path=r"C:\ProgramData\Reconciliation_design\path.xlsx",
        sheet="bankname Reconcil...",
        cols=['date','unnamed1','transaction','amount']
    )
    print(f"Inserted rows bank raw: {inserted}")

    inserted = load_client_raw(
        path=r"C:\ProgramData\Reconciliation_design\client_doc.xlsx",
        sheet="Sheet1",
        cols=['date','description','amount','counterpart_coding','talos_name']
    )
    print(f"Inserted rows client raw: {inserted}")

if __name__ == "__main__":
    main()


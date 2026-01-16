from db_config import engine
from sqlalchemy import select,func,text
from sqlalchemy.exc import SQLAlchemyError
from db_config import SessionLocal
from extraction import bankfunc
from extraction import clientfunc
import pandas as pd


#----------------------------- 


#FOR BANK DATA

# Parameterized INSERT (avoid including id; SQLite will autogenerate)
INSERT_BANK = text("""
INSERT INTO bankraw (date, description, amount)
VALUES (:date, :description, :amount)
""")

# Optional: Upsert to prevent duplicates by (date, description, amount)
# Requires UNIQUE(date, description, amount) in BANK_DDL.

# UPSERT_BANK = text("""
#INSERT INTO bankraw (date, description, amount)
#VALUES (:date, :description, :amount)
#ON CONFLICT(date, description, amount) DO NOTHING;
#""")


#Performs checks for complex operations for alignment and additional checks... CAUSE XERO DATA CONSISTENT...
def normalize_bank_df(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure correct column names
    if 'transaction' in df.columns:
        df = df.rename(columns={'transaction': 'description'})
    # Normalize date to ISO-like string (adjust format to your preference)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    # Clean strings
    if 'description' in df.columns:
        df['description'] = df['description'].astype(str).str.strip()
    # Amount numeric
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    # Keep only required columns in correct order
    return df[['date', 'description', 'amount']]




# main transformation section, can use pandas sql...
def load_bank_raw(path: str, sheet: str, cols: list[str]) -> int:
    """
    1) Calls extraction.loading(path, sheet, cols) to get the raw cleaned df.
    2) Normalizes types/columns.
    3) Loads into SQLite using parameterized SQL in a single transaction.
    """

    df = bankfunc(path, sheet, cols)
    df = normalize_bank_df(df)

    # Convert to list of dicts for executemany
    rows = df.to_dict(orient='records')

    #GETS UPDATED ONCE STARTS INSERTING...
    inserted = 0

    # Does use transaction...
    with SessionLocal.begin() as session:
        try:      
            #cnt = session.execute(select(func.count()).select_from(text("bankraw"))).scalar_one()
            #stmt = UPSERT_BANK if cnt > 0 else INSERT_BANK
            # executemany in one statement
            stmt=INSERT_BANK
            session.execute(stmt, rows)
            inserted = len(rows)

            #session.execute("""       
            #WITH ranked AS (SELECT id,
            #ROW_NUMBER() OVER (PARTITION BY date, description, amount ORDER BY id DESC) AS ranking FROM bankraw) DELETE FROM bankraw b
            #USING ranked r
            #WHERE b.id = r.id
            #AND r.ranking > 1;
            #""" )

            session.execute(text("""DELETE FROM BANKRAW WHERE EXISTS 
            (SELECT 1 FROM BANKRAW B WHERE BANKRAW.DATE=B.DATE AND BANKRAW.DESCRIPTION=B.DESCRIPTION AND BANKRAW.AMOUNT=B.AMOUNT AND BANKRAW.ID<B.ID)   
            """ ))
            #Use staging table to maintain ids...
            session.execute(text("""INSERT INTO BANKRAWSTG(date, description, amount) SELECT date, description, amount FROM BANKRAW;""" ))

            session.execute(text("""DELETE FROM BANKRAW""" ))

            #Final insert...
            session.execute(text("""INSERT INTO BANKRAW SELECT * FROM BANKRAWSTG""" ))

            session.execute(text("""DELETE FROM BANKRAWSTG""" ))


            session.commit()
            
        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted

#------------------------------------------------------------------------------------------------------------------------------------------

#FOR CLIENT DATA...



INSERT_CLIENT = text("""
INSERT INTO clientraw (date, description, amount, counterpart_coding, talos_name)
VALUES (:date, :description, :amount, :counterpart_coding, :talos_name)
""")

# Optional: Upsert to prevent duplicates by (date, description, amount)
# Requires UNIQUE(date, description, amount) in BANK_DDL.
#UPSERT_CLIENT = text("""
#INSERT INTO clientraw (date, description, amount, counterpart_coding, talos_name)
#VALUES (:date, :description, :amount, :counterpart_coding, :talos_name)
#ON CONFLICT(date, description, amount) DO NOTHING;
#""")



def load_client_raw(path: str, sheet: str, cols: list[str]) -> int:
    """
    1) Calls extraction.loading(path, sheet, cols) to get the raw cleaned df.
    2) Normalizes types/columns.
    3) Loads into SQLite using parameterized SQL in a single transaction.
    """
    df = clientfunc(path, sheet, cols)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    # Clean strings
    if 'description' in df.columns:
        df['description'] = df['description'].astype(str).str.strip()
    # Amount numeric
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Convert to list of dicts for executemany
    rows = df.to_dict(orient='records')


    inserted = 0


    # Does use transaction...
    with SessionLocal.begin() as session:
        try:
            #cnt = session.execute(select(func.count()).select_from(text("clientraw"))).scalar_one()
            #stmt = UPSERT_CLIENT if cnt > 0 else INSERT_CLIENT
            # executemany in one statement
            stmt=INSERT_CLIENT
            session.execute(stmt, rows)
            inserted = len(rows)
            #Keeps the latest update with fast execution...
            session.execute(text(""" DELETE FROM CLIENTRAW WHERE EXISTS 
            (SELECT 1 FROM CLIENTRAW B WHERE CLIENTRAW.DATE=B.DATE AND CLIENTRAW.DESCRIPTION=B.DESCRIPTION AND CLIENTRAW.AMOUNT=B.AMOUNT AND CLIENTRAW.ID<B.ID)   
            """ ))

            #Use staging table to maintain ids...
            session.execute(text("""INSERT INTO CLIENTRAWSTG(date, description, amount, counterpart_coding, talos_name) 
            SELECT date, description, amount, counterpart_coding, talos_name FROM CLIENTRAW;""" ))

            session.execute(text("""DELETE FROM CLIENTRAW""" ))

            #Final insert...
            session.execute(text("""INSERT INTO CLIENTRAW SELECT * FROM CLIENTRAWSTG""" ))
            session.execute(text("""DELETE FROM CLIENTRAWSTG""" ))

            session.commit()

        except SQLAlchemyError:
            session.rollback()
            raise


    return inserted

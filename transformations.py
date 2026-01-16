from db_config import engine
from sqlalchemy import select,func,text
from sqlalchemy.exc import SQLAlchemyError
from db_config import SessionLocal
from extraction import bankfunc
from extraction import clientfunc
import pandas as pd
from models import validate_bank_df, validate_client_df


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




# main transformation section, can use pandas sql...
def load_bank_raw(path: str, sheet: str, cols: list[str]) -> int:
    """
    1) Calls extraction.loading(path, sheet, cols) to get the raw cleaned df.
    2) Normalizes types/columns.
    3) Loads into SQLite using parameterized SQL in a single transaction.
    """

    df = bankfunc(path, sheet, cols)
    rows = validate_bank_df(df)

    # Convert to list of dicts for executemany
    #rows = df.to_dict(orient='records')

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
    rows=validate_client_df(df)


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

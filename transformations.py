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
INSERT INTO bankrawstg (date, description, amount, banktransactionid, direction, istransfer, isreconciled, bankaccountid, bankname, contactname, hasattachment)
VALUES (:date, :description, :amount, :banktransactionid, :direction, :istransfer, :isreconciled, :bankaccountid, :bankname, :contactname, :hasattachment)
""")

# Optional: Upsert to prevent duplicates by (date, description, amount)
# Requires UNIQUE(date, description, amount) in BANK_DDL.

# UPSERT_BANK = text("""
#INSERT INTO bankraw (date, description, amount)
#VALUES (:date, :description, :amount)
#ON CONFLICT(date, description, amount) DO NOTHING;
#""")

def load_bank_raw(path: str) -> int:
    """
    1) Calls extraction.loading(path, sheet, cols) to get the raw cleaned df.
    2) Normalizes types/columns.
    3) Loads into SQLite using parameterized SQL in a single transaction.
    """

    df = bankfunc(path)
    rows = validate_bank_df(df)

    #GETS UPDATED ONCE STARTS INSERTING...
    inserted = 0

    # Does use transaction...
    with SessionLocal.begin() as session:
        try:      
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

            session.execute(text("""
            INSERT INTO BANKRAW(date, description, amount, banktransactionid, direction, istransfer, isreconciled, bankaccountid, bankname, contactname, hasattachment)
            SELECT date, description, amount, banktransactionid, direction, istransfer, isreconciled, bankaccountid, bankname, contactname, hasattachment FROM BANKRAWSTG 
            WHERE NOT EXISTS(
            SELECT 1 FROM BANKRAW A 
            WHERE A.DATE=BANKRAWSTG.DATE 
            AND A.BANKNAME=BANKRAWSTG.BANKNAME 
            AND A.DESCRIPTION=BANKRAWSTG.DESCRIPTION 
            AND A.AMOUNT=BANKRAWSTG.AMOUNT
            AND A.banktransactionid=BANKRAWSTG.banktransactionid);""" ))

            #Use staging table to maintain ids...
            session.execute(text("""
            UPDATE BANKRAW AS c
            SET
            direction = s.direction, istransfer = s.istransfer, isreconciled = s.isreconciled, 
            bankaccountid = s.bankaccountid, contactname = s.contactname, hasattachment = s.hasattachment
            FROM BANKRAWSTG AS s
            WHERE s.bankname = c.bankname 
            AND s.date = c.date
            AND s.description = c.description
            AND s.amount = c.amount
            AND s.banktransactionid=c.banktransactionid;""" ))


            # Delete from staging table...
            session.execute(text("""DELETE FROM BANKRAWSTG""" ))
            session.commit()
            
        except SQLAlchemyError:
            session.rollback()
            raise

    return inserted

#------------------------------------------------------------------------------------------------------------------------------------------

#FOR CLIENT DATA...



INSERT_CLIENT = text("""
INSERT INTO CLIENTRAWSTG (date, bankname, description, amount, counterpart_coding, talos_name)
VALUES (:date, :bankname, :description, :amount, :counterpart_coding, :talos_name)
""")

# Optional: Upsert to prevent duplicates by (date, description, amount)
# Requires UNIQUE(date, description, amount) in BANK_DDL.
#UPSERT_CLIENT = text("""
#INSERT INTO clientraw (date, description, amount, counterpart_coding, talos_name)
#VALUES (:date, :description, :amount, :counterpart_coding, :talos_name)
#ON CONFLICT(date, description, amount) DO NOTHING;
#""")



def load_client_raw(path, sheet=None, markers=None, lower_cols=True) -> int:
    """
    1) Calls extraction.loading(path, sheet, cols) to get the raw cleaned df.
    2) Normalizes types/columns.
    3) Loads into SQLite using parameterized SQL in a single transaction.
    """
    df = clientfunc(path, sheet, markers)
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
            #session.execute(text(""" DELETE FROM CLIENTRAW WHERE EXISTS 
            #(SELECT 1 FROM CLIENTRAW B WHERE CLIENTRAW.DATE=B.DATE AND CLIENTRAW.DESCRIPTION=B.DESCRIPTION AND CLIENTRAW.AMOUNT=B.AMOUNT AND CLIENTRAW.ID<B.ID)   
            #""" ))
            session.execute(text("""INSERT INTO CLIENTRAW (date, bankname, description, amount, counterpart_coding, talos_name)
            SELECT date, bankname, description, amount, counterpart_coding, talos_name FROM CLIENTRAWSTG 
            WHERE NOT EXISTS(
            SELECT 1 FROM CLIENTRAW A 
            WHERE A.DATE=CLIENTRAWSTG.DATE 
            AND A.BANKNAME=CLIENTRAWSTG.BANKNAME 
            AND A.DESCRIPTION=CLIENTRAWSTG.DESCRIPTION 
            AND A.AMOUNT=CLIENTRAWSTG.AMOUNT)"""))

            #UPDATING EXISTING COLUMNS...

            session.execute(text("""
            UPDATE clientraw AS c
            SET
            bankname=s.bankname,
            counterpart_coding = s.counterpart_coding,
            talos_name = s.talos_name
            FROM clientrawstg AS s
            WHERE s.bankname = c.bankname 
            AND s.date = c.date
            AND s.description = c.description
            AND s.amount = c.amount
            """))

            #Final insert...
            session.execute(text("""DELETE FROM CLIENTRAWSTG""" ))

            session.commit()

        except SQLAlchemyError:
            session.rollback()
            raise


    return inserted


from db_config import engine
from sqlalchemy import select,func,text
from sqlalchemy.exc import SQLAlchemyError
from db_config import SessionLocal
import pandas as pd

with SessionLocal() as session:

    data=session.execute(text("""DELETE FROM bankprocessedstg"""))

    data=session.execute(text("""
    INSERT INTO bankprocessedstg (id,bankname,date,description,amount,banktransactionid, istransfer, contactname, isreconciled)
    SELECT id,bankname,date,description, case when direction='SPEND' then -1*amount else amount end as amount,
    banktransactionid, istransfer, contactname, isreconciled from bankraw
    """))

    print("Excecution done for staging...")

    #ADDS NEW RECONS...

    data=session.execute(text("""INSERT INTO bankunrec (id,bankname,date,description,amount,banktransactionid, istransfer, contactname)
    SELECT d.id, d.bankname, d.date, d.description, d.amount, d.banktransactionid, d.istransfer, d.contactname from (
    SELECT id,bankname,date,description,amount,banktransactionid, istransfer, contactname,isreconciled FROM bankprocessedstg
    WHERE NOT EXISTS (SELECT 1 FROM bankunrec a
    WHERE bankprocessedstg.bankname=a.bankname
    AND bankprocessedstg.date=a.date
    AND bankprocessedstg.description=a.description
    AND bankprocessedstg.amount=a.amount
    AND bankprocessedstg.banktransactionid=a.banktransactionid) ) AS d WHERE d.isreconciled=0"""))

    print("Script 2 execution done for writing...")

    data=session.execute(text("""
    DELETE FROM bankunrec AS a
    WHERE EXISTS (
    SELECT 1
    FROM bankprocessedstg AS s
    WHERE s.isreconciled = 1
    AND a.bankname = s.bankname
    AND a.date = s.date
    AND a.description = s.description
    AND a.amount = s.amount
    AND a.banktransactionid = s.banktransactionid);"""))

    print("Script 2 execution done for updates...")

    session.commit()
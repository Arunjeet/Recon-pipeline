
from db_config import engine
from sqlalchemy import select,func,text
from sqlalchemy.exc import SQLAlchemyError
from db_config import SessionLocal
import pandas as pd

with SessionLocal() as session:

    data=session.execute(text("""DELETE FROM clientprocessedstg"""))

    data=session.execute(text("""
    INSERT INTO clientprocessedstg (id,bankname,date,description,amount,coding, entity, name, keyword)
    SELECT id,bankname,date,description,amount,
    CASE WHEN lower(counterpart_coding) LIKE '%cp id:%' THEN ltrim(substr(counterpart_coding,instr(lower(counterpart_coding),'cp id:')+length('cp id:')))
        WHEN counterpart_coding GLOB '[0-9]*' THEN counterpart_coding
        ELSE NULL END AS coding,
    LOWER(CASE WHEN upper(talos_name) LIKE '%BVI%' THEN 'BVI'
            WHEN upper(talos_name) LIKE '%DE%' THEN 'DE'
            WHEN upper(talos_name) LIKE '%LP%' THEN 'LP'
            ELSE NULL END) AS entity,
    CASE WHEN upper(talos_name) IN ('LP','DE','BVI') THEN NULL
        ELSE TRIM(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(talos_name,'__BVI',''),'_BVI',''),'__DE',''),'_DE',''),'__LP',''),'_LP',''),'_')) END AS name,
    CASE WHEN instr(description,'2025')>0 THEN substr(description,1,instr(description,'2025')-1) ELSE description END AS keyword
    FROM clientraw;
    """))

    print("Excecution done for staging...")

    data=session.execute(text("""INSERT INTO clientprocessed (id,bankname,date,description,amount,coding, entity, name, keyword, encoding)
    SELECT data.*, CASE WHEN trim(lower(entity))='bvi' then 'bvi-1113'
    WHEN trim(lower(entity))='de' then 'usa-1112'
    WHEN trim(lower(entity))='lp' then '1115/1116' else NULL END as encoding from(
    SELECT id,bankname,date,description,amount,coding, entity, name, keyword FROM clientprocessedstg 
    WHERE NOT EXISTS(
    SELECT 1 FROM clientprocessed A 
    WHERE A.DATE=clientprocessedstg.DATE 
    AND A.BANKNAME=clientprocessedstg.BANKNAME 
    AND A.DESCRIPTION=clientprocessedstg.DESCRIPTION 
    AND A.AMOUNT=clientprocessedstg.AMOUNT)) as data"""))

    print("Script 1 execution done for writing...")

    data=session.execute(text("""
    UPDATE clientprocessed AS c
    SET
    coding = s.coding,
    entity = s.entity,
    name = s.name,
    keyword = s.keyword
    FROM clientprocessedstg AS s
    WHERE s.bankname = c.bankname 
    AND s.date = c.date
    AND s.description = c.description
    AND s.amount = c.amount
    """))

    print("Script 1 execution done for updates...")

    session.commit()
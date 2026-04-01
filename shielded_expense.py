from db_config import engine
from sqlalchemy import select,func,text,bindparam
from sqlalchemy.exc import SQLAlchemyError
from db_config import SessionLocal
import pandas as pd
from schemas import JOURNAL_PROCESS
from gsheet import sheetdump

def ensure_schema():
    with engine.begin() as conn:
        #conn.execute(text("""DROP TABLE IF EXISTS journal_processed"""))
        conn.execute(text(JOURNAL_PROCESS))

ensure_schema()

#--------------CREATE TEMP--------------------------------------------------------

SQL_CREATE_TEMP = text("""
CREATE TEMP TABLE journal_temp AS
SELECT
    j1.tenant_id, 
    j1.journallineid, 
    j1.sourcetype, 
    j1.journaldate, 
    j1.referencenumber, 
    j1.accountid,
    j1.accountcode, 
    j1.accountname, 
    j1.accounttype, 
    j1.description AS journal_description,
    j2.description AS description_manualjournal,
    j3.description AS description_account,
    j2.status       AS status_journal,
    j3.status       AS status_account,
    j1.debit,
    j1.credit,
    j1.grossamount,
    j1.netamount,
    j2.updateddateutc AS updateddateutc_journal, 
    j3.updateddateutc AS updateddateutc_account,
    j2.showoncashbasisreports, 
    j2.hasattachments,
    j3.class,
    j3.reportingcodename
FROM journalsraw j1 
LEFT JOIN manualjournalsraw j2 
    ON j1.journalid = j2.manualjournalid
LEFT JOIN accountsraw j3 
    ON j1.accountid = j3.accountid
WHERE CAST(j1.accountcode AS INTEGER) IN (
    820,
    400000, 400002, 400005, 400010, 400020, 400200,
    401000, 402000, 410000,
    420000, 420001, 420010, 420100,
    430000,
    500000, 500001, 500010, 500011, 500020, 500021, 500030, 500040, 500050,
    500100, 500101, 500150, 500151, 500200, 500250,
    500500, 500550, 500600, 500650, 500700, 500750, 500800, 500900, 500950,
    600000, 604000, 604050, 605000, 606000, 607000, 608000, 609000,
    610209, 610300, 610301,
    620010, 620011, 620016, 620026, 620031, 620033, 620055, 620064,
    620502, 620504, 620505,
    630005,
    640001, 640002, 640003, 640004, 640005, 640006,
    700000, 702000, 703000, 706000, 706500, 707000, 708000, 709000,
    800000, 800006, 800100, 800305,
    900000, 910000
)
  AND substr(j1.journaldate, 1, 4) = '2025'
  AND j1.tenant_id = 'd7418ac2-e3ec-488b-b942-5bfef34ff7b7'
  AND (j2.status LIKE '%POSTED%' OR j2.status IS NULL)
  AND j3.status LIKE '%active%'
ORDER BY CAST(j1.referencenumber AS INTEGER) ASC;
""")


#------------------------------------------------------------------------------------------
SQL_INSERT_NEW = text("""
INSERT INTO journal_processed (
  tenant_id,
  journallineid,
  sourcetype,
  journaldate,
  referencenumber,
  accountid,
  accountcode,
  accountname,
  accounttype,
  journal_description,
  description_manualjournal,
  description_account,
  status_journal,
  status_account,
  debit,
  credit,
  grossamount,
  netamount,
  updateddateutc_journal,
  updateddateutc_account,
  showoncashbasisreports,
  hasattachments,
  class,
  reportingcodename
)
SELECT
  t.tenant_id,
  t.journallineid,
  t.sourcetype,
  t.journaldate,
  t.referencenumber,
  t.accountid,
  t.accountcode,
  t.accountname,
  t.accounttype,
  t.journal_description,
  t.description_manualjournal,
  t.description_account,
  t.status_journal,
  t.status_account,
  t.debit,
  t.credit,
  t.grossamount,
  t.netamount,
  t.updateddateutc_journal,
  t.updateddateutc_account,
  t.showoncashbasisreports,
  t.hasattachments,
  t.class,
  t.reportingcodename
FROM journal_temp t
WHERE NOT EXISTS (
  SELECT 1
  FROM journal_processed p
  WHERE p.journallineid = t.journallineid
);
""")

SQL_UPDATE_EXISTING = text("""
UPDATE journal_processed AS p
SET
  tenant_id                 = t.tenant_id,
  sourcetype                = t.sourcetype,
  journaldate               = t.journaldate,
  referencenumber           = t.referencenumber,
  accountid                 = t.accountid,
  accountcode               = t.accountcode,
  accountname               = t.accountname,
  accounttype               = t.accounttype,
  journal_description       = t.journal_description,
  description_manualjournal = t.description_manualjournal,
  description_account       = t.description_account,
  status_journal            = t.status_journal,
  status_account            = t.status_account,
  debit                     = t.debit,
  credit                    = t.credit,
  grossamount               = t.grossamount,
  netamount                 = t.netamount,
  updateddateutc_journal    = t.updateddateutc_journal,
  updateddateutc_account    = t.updateddateutc_account,
  showoncashbasisreports    = t.showoncashbasisreports,
  hasattachments            = t.hasattachments,
  class                     = t.class,
  reportingcodename         = t.reportingcodename
FROM journal_temp AS t
WHERE t.journallineid = p.journallineid;
""")

SQL_DROP_TEMP = text("""DROP TABLE IF EXISTS journal_temp;""")


#--------------------------------------------------------------------
def main():

  with SessionLocal.begin() as session:
      try:
          session.execute(SQL_CREATE_TEMP)
          session.execute(SQL_INSERT_NEW)
          session.execute(SQL_UPDATE_EXISTING) 
          session.execute(SQL_DROP_TEMP)
          
          session.commit()
      except SQLAlchemyError:
              session.rollback()
              raise
      session.close()
  #-------------------------------------------------------------------

  with SessionLocal() as session:
      data=session.execute(text("""SELECT a.*, max(a.referencenumber) from journal_processed a
      group by a.journaldate, a.accountcode, a.accountid, a.accountname, a.accounttype, abs(a.grossamount)
      order by CAST(a.referencenumber AS INTEGER) ASC;"""))

      rows = data.fetchall()
      session.commit()


  df = pd.DataFrame(rows)
  print(df)

  sheetdump(df,"accounttrans")

if __name__=="__main__":
  main()
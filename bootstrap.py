#ENSURES DATA CHECKS BEFORE SCHEMA EXECUTION...
# app/bootstrap.py
from sqlalchemy import text
from db_config import engine

from schemas import TB_DDL
from schemas import TB_STG

from schemas import PNL_DDL
from schemas import PNL_STG

from schemas import JOURNALS_DDL
from schemas import JOURNALS_STG

from schemas import MANUALJOURNALS_DDL
from schemas import MANUALJOURNALS_STG


from schemas import ACCOUNTS_DDL
from schemas import ACCOUNTS_STG


from schemas import MASTER_DDL


def ensure_schema():
    # engine.begin() gives you a transactional connection that auto-commits/rolls back
    with engine.begin() as conn:
        

        #conn.execute(text("""DROP TABLE IF EXISTS master"""))
        conn.execute(text(MASTER_DDL))
        #----------------------------------------------------------------------------
        #conn.execute(text("""DROP TABLE IF EXISTS tb_client"""))
        conn.execute(text(TB_DDL))
        

        conn.execute(text("""DROP TABLE IF EXISTS tb_client_stg"""))
        conn.execute(text(TB_STG))
        #-----------------------------------------------------------------------------

        #conn.execute(text("""DROP TABLE IF EXISTS pnl_client"""))
        conn.execute(text(PNL_DDL))
        

        conn.execute(text("""DROP TABLE IF EXISTS pnl_client_stg"""))
        conn.execute(text(PNL_STG))


        #-----------------------------------------------------------------------------


        #conn.execute(text("""DROP TABLE IF EXISTS journalsraw"""))
        conn.execute(text(JOURNALS_DDL))
        

        conn.execute(text("""DROP TABLE IF EXISTS journalsrawstg"""))
        conn.execute(text(JOURNALS_STG))




        #-----------------------------------------------------------------------------

        #conn.execute(text("""DROP TABLE IF EXISTS manualjournalsraw"""))
        conn.execute(text(MANUALJOURNALS_DDL))
        

        conn.execute(text("""DROP TABLE IF EXISTS manualjournalsstg"""))
        conn.execute(text(MANUALJOURNALS_STG))
        #------------------------------------------------------------------------------



        #conn.execute(text("""DROP TABLE IF EXISTS accountsraw"""))
        conn.execute(text(ACCOUNTS_DDL))
        

        conn.execute(text("""DROP TABLE IF EXISTS accountsstg"""))
        conn.execute(text(ACCOUNTS_STG))
        #------------------------------------------------------------------------------
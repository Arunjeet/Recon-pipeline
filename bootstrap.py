#ENSURES DATA CHECKS BEFORE SCHEMA EXECUTION... STAGING TABLES ARE ALWAYS DELETED...

# app/bootstrap.py
from sqlalchemy import text
from db_config import engine

from schemas import BANK_DDL
from schemas import BANK_DDL_STG
from schemas import BANK_DDL_PROCESSED_STG
from schemas import BANK_UNREC

from schemas import CLIENT_DDL
from schemas import CLIENT_DDL_STG

from schemas import CLIENT_DDL_PROCESSED
from schemas import CLIENT_DDL_PROCESSED_STG

def ensure_schema():
    # engine.begin() gives you a transactional connection that auto-commits/rolls back
    with engine.begin() as conn:
        conn.execute(text("""DROP TABLE IF EXISTS bankraw"""))
        conn.execute(text(BANK_DDL))
        #conn.execute(text(UNIQUE_COLS_BANK))

        conn.execute(text("""DROP TABLE IF EXISTS bankrawstg"""))
        conn.execute(text(BANK_DDL_STG))

        conn.execute(text("""DROP TABLE IF EXISTS bankprocessedstg"""))
        conn.execute(text(BANK_DDL_PROCESSED_STG))
        #conn.execute(text(UNIQUE_COLS_CLIENT))

        conn.execute(text("""DROP TABLE IF EXISTS bankunrec"""))
        conn.execute(text(BANK_UNREC))



        #-----------------------------------------------------------------------------

        conn.execute(text("""DROP TABLE IF EXISTS clientraw"""))
        conn.execute(text(CLIENT_DDL))

        conn.execute(text("""DROP TABLE IF EXISTS clientrawstg"""))
        conn.execute(text(CLIENT_DDL_STG))

        conn.execute(text("""DROP TABLE IF EXISTS clientprocessed"""))
        conn.execute(text(CLIENT_DDL_PROCESSED))

        conn.execute(text("""DROP TABLE IF EXISTS clientprocessedstg"""))
        conn.execute(text(CLIENT_DDL_PROCESSED_STG))

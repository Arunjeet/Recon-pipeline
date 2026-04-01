#Performs data validation...
BANK_DDL = """ 
CREATE TABLE IF NOT EXISTS bankraw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL CHECK (length(date) >= 10),
  description text,
  amount NUMERIC,
  banktransactionid text NOT NULL,
  direction text,
  istransfer boolean,
  isreconciled boolean,
  bankaccountid text,
  bankname text,
  contactname text,
  hasattachment boolean,
  UNIQUE(banktransactionid)
);
"""

BANK_DDL_STG = """
CREATE TABLE IF NOT EXISTS bankrawstg (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL CHECK (length(date) >= 10),
  description text,
  amount NUMERIC,
  banktransactionid text NOT NULL,
  direction text,
  istransfer boolean,
  isreconciled boolean,
  bankaccountid text,
  bankname text,
  contactname text,
  hasattachment boolean,
  UNIQUE(banktransactionid)
);
"""


#----------------------------------------------------------------------


BANK_UNREC = """
CREATE TABLE IF NOT EXISTS bankunrec (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bankname TEXT,
    date TEXT NOT NULL CHECK (length(date) >= 10),
    description TEXT,
    amount NUMERIC,
    banktransactionid TEXT NOT NULL,
    istransfer BOOLEAN,
    contactname TEXT,
    UNIQUE(banktransactionid)
    );
"""



BANK_DDL_PROCESSED_STG = """
CREATE TABLE IF NOT EXISTS bankprocessedstg (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bankname TEXT,
  date TEXT NOT NULL CHECK (length(date) >= 10),
  description TEXT,
  amount NUMERIC,
  banktransactionid TEXT NOT NULL,
  istransfer BOOLEAN,
  contactname TEXT,
  isreconciled BOOLEAN,
  UNIQUE(banktransactionid)
);
"""

#-----------------------------------------------------------------------------------------------
CLIENT_DDL = """
CREATE TABLE IF NOT EXISTS clientraw (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  bankname    TEXT NOT NULL,
  date        TEXT NOT NULL CHECK (length(date) >= 10),
  description TEXT NOT NULL,
  amount      NUMERIC,
  counterpart_coding TEXT,
  talos_name TEXT,
  UNIQUE (bankname, date, description, amount)
);

"""

CLIENT_DDL_STG = """
CREATE TABLE IF NOT EXISTS clientrawstg (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  bankname    TEXT NOT NULL,
  date        TEXT NOT NULL CHECK (length(date) >= 10),
  description TEXT NOT NULL,
  amount      NUMERIC,
  counterpart_coding TEXT,
  talos_name TEXT,
  UNIQUE (bankname, date, description, amount)
);

"""

#-------------------------------------------------------
CLIENT_DDL_PROCESSED = """
CREATE TABLE IF NOT EXISTS clientprocessed (
  id          INTEGER,
  bankname    TEXT NOT NULL,
  date        TEXT NOT NULL CHECK (length(date)>= 10),
  description TEXT,
  amount      NUMERIC,
  coding      TEXT,
  entity      VARCHAR(5),
  name        TEXT,
  keyword     TEXT,
  encoding    TEXT
);
"""

CLIENT_DDL_PROCESSED_STG = """
CREATE TABLE IF NOT EXISTS clientprocessedstg (
  id          INTEGER ,
  bankname    TEXT NOT NULL,
  date        TEXT NOT NULL CHECK (length(date)>= 10),
  description TEXT,
  amount      NUMERIC,
  coding      TEXT,
  entity      VARCHAR(5),
  name        TEXT,
  keyword     TEXT
);
"""

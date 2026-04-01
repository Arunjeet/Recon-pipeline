#Performs data validation...


MASTER_DDL = """
CREATE TABLE IF NOT EXISTS master (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant_id  TEXT
);

"""


TB_DDL = """ 
CREATE TABLE IF NOT EXISTS tb_client (
  tenant_id     TEXT    NOT NULL, 
  row_hash     TEXT    PRIMARY KEY,
  date         TEXT    NOT NULL,
  section      TEXT    NOT NULL,
  label        TEXT    NOT NULL,
  debit        REAL    NULL,
  credit       REAL    NULL,
  accountid    TEXT    NULL,
  accountcode  TEXT    NULL
);

"""


TB_STG = """
CREATE TABLE IF NOT EXISTS tb_client_stg (
  tenant_id     TEXT    NOT NULL, 
  row_hash     TEXT    PRIMARY KEY,
  date         TEXT    NOT NULL,
  section      TEXT    NOT NULL,
  label        TEXT    NOT NULL,
  debit        REAL    NULL,
  credit       REAL    NULL,
  accountid    TEXT    NULL,
  accountcode  TEXT    NULL
);
"""


#----------------------------------------------------------------------



PNL_DDL = """ 
CREATE TABLE IF NOT EXISTS pnl_client (
  tenant_id     TEXT    NOT NULL, 
  row_hash   TEXT    PRIMARY KEY,
  date       TEXT    NOT NULL,
  section    TEXT    NOT NULL,
  label      TEXT    NOT NULL,
  amount     REAL    NULL,
  issummary  TEXT    NULL,
  accountid  TEXT    NULL
);
"""


PNL_STG = """
CREATE TABLE IF NOT EXISTS pnl_client_stg (
  tenant_id     TEXT    NOT NULL, 
  row_hash   TEXT    PRIMARY KEY,
  date       TEXT    NOT NULL,
  section    TEXT    NOT NULL,
  label      TEXT    NOT NULL,
  amount     REAL    NULL,
  issummary  TEXT    NULL,
  accountid  TEXT    NULL
);
"""

#-----------------------------------------------------------------------------------------------

JOURNALS_DDL = """
CREATE TABLE IF NOT EXISTS journalsraw (
  tenant_id     TEXT    NOT NULL, 
  journallineid                TEXT PRIMARY KEY,
  referencenumber              TEXT,
  journalid                    TEXT,
  journaldate                  DATE,
  accountid                    TEXT,
  accountcode                  TEXT,
  accounttype                  TEXT,
  accountname                  TEXT,
  description                  TEXT,
  sourcetype                   TEXT,
  reference                    TEXT,
  netamount                    DECIMAL(18, 2),
  grossamount                  DECIMAL(18, 2),
  taxamount                    DECIMAL(18, 2),
  taxtype                      TEXT,
  taxname                      TEXT,
  debit                        DECIMAL(18, 2),
  credit                       DECIMAL(18, 2),
  createddateutc               TEXT,
  trackingcategoriescount      INT,

  trackingcategory1_name                   TEXT NULL,
  trackingcategory1_option                 TEXT NULL,
  trackingcategory1_trackingcategoryid     TEXT NULL,
  trackingcategory1_trackingoptionid       TEXT NULL,

  trackingcategory2_name                   TEXT NULL,
  trackingcategory2_option                 TEXT NULL,
  trackingcategory2_trackingcategoryid     TEXT NULL,
  trackingcategory2_trackingoptionid       TEXT NULL
);

"""

JOURNALS_STG = """
CREATE TABLE IF NOT EXISTS journalsrawstg (
  tenant_id     TEXT    NOT NULL, 
  journallineid                TEXT PRIMARY KEY,
  referencenumber              TEXT,
  journalid                    TEXT,
  journaldate                  DATE,
  accountid                    TEXT,
  accountcode                  TEXT,
  accounttype                  TEXT,
  accountname                  TEXT,
  description                  TEXT,
  sourcetype                   TEXT,
  reference                    TEXT,
  netamount                    DECIMAL(18, 2),
  grossamount                  DECIMAL(18, 2),
  taxamount                    DECIMAL(18, 2),
  taxtype                      TEXT,
  taxname                      TEXT,
  debit                        DECIMAL(18, 2),
  credit                       DECIMAL(18, 2),
  createddateutc               TEXT,
  trackingcategoriescount      INT,

  trackingcategory1_name                   TEXT NULL,
  trackingcategory1_option                 TEXT NULL,
  trackingcategory1_trackingcategoryid     TEXT NULL,
  trackingcategory1_trackingoptionid       TEXT NULL,

  trackingcategory2_name                   TEXT NULL,
  trackingcategory2_option                 TEXT NULL,
  trackingcategory2_trackingcategoryid     TEXT NULL,
  trackingcategory2_trackingoptionid       TEXT NULL
);

"""

#---------------------------------------------------------------------------------------------------

MANUALJOURNALS_DDL = """
CREATE TABLE IF NOT EXISTS manualjournalsraw (
  tenant_id     TEXT    NOT NULL, 
  manualjournalid           TEXT PRIMARY KEY,
  status                    TEXT,
  description               TEXT,
  date                      DATE,
  updateddateutc            DATE,
  lineamounttypes           TEXT,
  showoncashbasisreports    TEXT,
  hasattachments            TEXT
);

"""

MANUALJOURNALS_STG = """
CREATE TABLE IF NOT EXISTS manualjournalsstg (
  tenant_id     TEXT    NOT NULL, 
  manualjournalid           TEXT PRIMARY KEY,
  status                    TEXT,
  description               TEXT,
  date                      DATE,
  updateddateutc            DATE,
  lineamounttypes           TEXT,
  showoncashbasisreports    TEXT,
  hasattachments            TEXT
);

"""

#-------------------------------------------------------------------------------------------
ACCOUNTS_DDL = """
CREATE TABLE IF NOT EXISTS accountsraw (
  tenant_id                TEXT    NOT NULL,
  accountid                TEXT    PRIMARY KEY,
  code                     TEXT,
  name                     TEXT,
  status                   TEXT,
  type                     TEXT,
  taxtype                  TEXT,
  class                    TEXT,
  enablepaymentstoaccount  TEXT,
  showinexpenseclaims      TEXT,
  bankaccountnumber        TEXT,
  bankaccounttype          TEXT,
  currencycode             TEXT,
  reportingcode            TEXT,
  reportingcodename        TEXT,
  hasattachments           TEXT,
  addtowatchlist           TEXT,
  updateddateutc           DATE,
  description              TEXT,
  reportingname            TEXT,
  systemaccount            TEXT
);
"""

ACCOUNTS_STG = """
CREATE TABLE IF NOT EXISTS accountsstg (
  tenant_id                TEXT    NOT NULL,
  accountid                TEXT    PRIMARY KEY,
  code                     TEXT,
  name                     TEXT,
  status                   TEXT,
  type                     TEXT,
  taxtype                  TEXT,
  class                    TEXT,
  enablepaymentstoaccount  TEXT,
  showinexpenseclaims      TEXT,
  bankaccountnumber        TEXT,
  bankaccounttype          TEXT,
  currencycode             TEXT,
  reportingcode            TEXT,
  reportingcodename        TEXT,
  hasattachments           TEXT,
  addtowatchlist           TEXT,
  updateddateutc           DATE,
  description              TEXT,
  reportingname            TEXT,
  systemaccount            TEXT
);
"""

#----------------------------------------------------------------------------------------------------------

JOURNAL_PROCESS = """
CREATE TABLE IF NOT EXISTS journal_processed (
  tenant_id                 TEXT    NOT NULL,
  journallineid             TEXT,
  sourcetype                TEXT,
  journaldate               DATE,
  referencenumber           TEXT,
  accountid                 TEXT,
  accountcode               TEXT,
  accountname               TEXT,
  accounttype               TEXT,
  journal_description       TEXT,
  description_manualjournal TEXT,
  description_account       TEXT,
  status_journal            TEXT,
  status_account            TEXT,
  debit                     REAL,
  credit                    REAL,
  grossamount               REAL,
  netamount                 REAL,
  updateddateutc_journal    DATE,
  updateddateutc_account    DATE,
  showoncashbasisreports    TEXT,
  hasattachments            TEXT,
  class                     TEXT,
  reportingcodename         TEXT,
  PRIMARY KEY (journallineid)
);
"""

#--------------------------------------------------------------------------------
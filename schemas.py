#Performs data validation...
BANK_DDL = """
CREATE TABLE IF NOT EXISTS bankraw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL CHECK (length(date) >= 10),
    description TEXT NOT NULL,
    amount NUMERIC
);
"""

BANK_DDL_STG = """
CREATE TABLE IF NOT EXISTS bankrawstg (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL CHECK (length(date) >= 10),
    description TEXT NOT NULL,
    amount NUMERIC
);
"""

UNIQUE_COLS_BANK="""
CREATE UNIQUE INDEX IF NOT EXISTS bankraw_unique_triplet
ON bankraw (date, description, amount);

"""

#------------------------------------------------------
CLIENT_DDL = """
CREATE TABLE IF NOT EXISTS clientraw (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  date        TEXT NOT NULL CHECK (length(date)>= 10),
  description TEXT NOT NULL,
  amount      NUMERIC,
  counterpart_coding text,
  talos_name text
);
"""

CLIENT_DDL_STG = """
CREATE TABLE IF NOT EXISTS clientrawstg (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  date        TEXT NOT NULL CHECK (length(date)>= 10),
  description TEXT NOT NULL,
  amount      NUMERIC,
  counterpart_coding text,
  talos_name text
);
"""

UNIQUE_COLS_CLIENT="""
CREATE UNIQUE INDEX IF NOT EXISTS clientraw_unique_triplet
ON clientraw (date, description, amount);
"""

#-------------------------------------------------------
CLIENT_DDL_PROCESSED = """
CREATE TABLE IF NOT EXISTS clientprocessed (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  date        TEXT NOT NULL CHECK (length(date)>= 10),
  description TEXT,
  amount      NUMERIC,
  coding      TEXT,
  entity      VARCHAR(5),
  name        TEXT
);
"""

CLIENT_DDL_PROCESSED_STG = """
CREATE TABLE IF NOT EXISTS clientprocessedstg (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  date        TEXT NOT NULL CHECK (length(date)>= 10),
  description TEXT,
  amount      NUMERIC,
  coding      TEXT,
  entity      VARCHAR(5),
  name        TEXT
);
"""

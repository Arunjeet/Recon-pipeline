STEP1: Create virtual environment inside project folder
A> get the miniconda file path. Open anaconda terminal and run find conda. Get the conda executable file path
B> Create a folder inside editor, open terminal and run & "C:\ProgramData\miniconda3\Scripts\conda.exe" create --prefix ./venv python=3.10
C> Initiate the conda inside project folder & "C:\ProgramData\miniconda3\Scripts\conda.exe" init powershell
D> Activate the venv conda activate ./venv

STEP2: Create a extraction.py for loading, cleaning. Use pandas/spark or whatever to make it easy.
Create a requirements.txt file and execute using pip install -r requirements.txt (can use docker for whole process if required)
db_config, schema, bootstrap, extraction (from different sources), executes extraction and performs manipulation before inserting into schema

STEP3: Use db.config for db initiation and engine and db url can be migrated to postgres later.

STEP4: extraction and transformation.py s these can be used for data loading and transformation for reading from different sources, transforming and loading with normalized checks.

STEP5: Finally executed using main.py() and using sql.py() to see output and DDL or DML

conn.executescript("""
BEGIN;




COMMIT;
""")

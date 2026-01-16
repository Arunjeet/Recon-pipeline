
from db_config import engine
from sqlalchemy import select,func,text
from sqlalchemy.exc import SQLAlchemyError
from db_config import SessionLocal
import pandas as pd

with SessionLocal() as session:
    session.execute(text("""INSERT INTO CLIENTRAWSTG SELECT * FROM CLIENTRAW"""))
    result=session.execute(text("""SELECT * FROM CLIENTRAWSTG"""))

    rows = result.fetchall()

    columns = result.keys()
    session.commit()


df = pd.DataFrame(rows, columns=columns)


print(df.columns)
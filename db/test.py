import pandas as pd
from sqlalchemy import select, insert

from db.engine import SessionLocal
from db.models.iee import IeeIeeDataToday, IeeIeeDataHistorical


session = SessionLocal()
query = select(*IeeIeeDataHistorical.__table__.columns).where(
    IeeIeeDataHistorical.tradeDateShamsi == "1404/09/12")
rows = session.execute(query).mappings().fetchall()
df = pd.DataFrame(rows)

session.execute(
    insert(IeeIeeDataToday),
    df.astype(object).where(pd.notnull(df), None).to_dict(orient='records')
)
session.commit()


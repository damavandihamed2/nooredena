import time
import requests as rq
from pyodbc import Connection
from urllib.parse import quote
from utils.database import make_connection



base_url = "https://cdn.ime.co.ir/realTimeServer"

def get_connection_token(session: rq.Session) -> str:
    r_ = session.get(url=f"{base_url}/negotiate")
    r_json = r_.json()
    negotiate_token = r_json["ConnectionToken"]
    return negotiate_token


def stream_sse_data(
        db_conn: Connection,
        session: rq.Session,
        token: str
) -> None:
    sse_url = (f'{base_url}/connect?transport=serverSentEvents&clientProtocol=2.1&'
               f'connectionToken={quote(token)}&connectionData=[{{"name":"marketshub"}}]')
    with session.get(sse_url, stream=True) as r:
        r.raise_for_status()
        print("start fetching data stream")
        lines = r.iter_lines()
        next(lines)
        for line in lines:
            if line:
                line = line.decode("utf-8", errors="ignore").strip()
                crsr = db_conn.cursor()
                crsr.execute(
                    f"INSERT INTO [nooredenadb].[ime].[tablo] (record, time) VALUES ('{line}', {time.time_ns()})"
                )


def fetch_market_data(db_conn: Connection) -> None:
    session = rq.Session()
    token = get_connection_token(session)
    stream_sse_data(db_conn=db_conn, session=session, token=token)


if __name__ == "__main__":
    db_ = make_connection()
    fetch_market_data(db_)
    db_.close()


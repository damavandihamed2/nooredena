import time, json
import pandas as pd
import requests as rq
from urllib.parse import quote
from utils.database import make_connection, insert_to_database


db_conn = make_connection()


def get_connection_token(session: rq.Session) -> str:
    """Negotiate a SignalR connection and return the ConnectionToken."""
    r = session.get(
        url=f"https://cdn.ime.co.ir/realTimeServer/negotiate",
        params={
            "clientProtocol": "2.1",
            "connectionData": '[{"name":"marketshub"}]',
            "_": str(int(time.time() * 1000)),
        },
    )
    r.raise_for_status()
    return r.json()["ConnectionToken"]


def stream_sse_data(
    session: rq.Session,
    token: str,
    iterations: int = 30,
) -> list[dict]:

    # session = rq.Session()
    # token = get_connection_token(session)
    # token = quote(token)

    """Open an SSE stream and collect all JSON messages into a list."""
    sse_url = (
        "https://cdn.ime.co.ir/realTimeServer/connect?"
        "transport=serverSentEvents&"
        "clientProtocol=2.1&"
        f"connectionToken={token}&"
        'connectionData=[{"name":"marketshub"}]'
    )

    data_list = []
    with session.get(sse_url, stream=True) as r:
        r.raise_for_status()
        # i = 0
        for line in r.iter_lines():
            if line:
                line = line.decode("utf-8", errors="ignore").strip()
                crsr = db_conn.cursor()
                crsr.execute(f"INSERT INTO [nooredenadb].[ime].[test] (record) VALUES ('{line}')")
                # data_list.append(line)
                # content = line[5:].strip()
                # if content not in ("[DONE]", "null", ""):
                #     try:
                #         data_list.append(json.loads(content))
                #     except json.JSONDecodeError as je:
                #         print(je)
            # i += 1
            # print(i)
            # if i > iterations:
            #     break
    return data_list



def fetch_market_data(session: rq.Session) -> list[dict]:
    """End-to-end: negotiate, stream SSE, and return a DataFrame."""
    session = rq.Session()
    token = get_connection_token(session)
    token = quote(token)
    print(f"Token: {token}")
    records = stream_sse_data(session, token, 300)
    return records

records = []

# for i in range(len(records)):
#     try:
#         records[60]["M"]
#     except KeyError:
#         print(i)
records_new = []
for i in range(len(records)):
    _m = records[i].get("M")
    if _m:
        records_new += _m

m_set = set()
for r in records_new:
    if len(r["A"]) > 1:
        print(r)
    if r["H"] != "marketsHub":
        print(r)
    m_set.add(r["M"])

records_by_m ={}

for m in m_set:
    tmp = []
    for r in records_new:
        m_ = r.get("M")
        if m_ and m_ == m:
            tmp += r.get("A")
    records_by_m[m] = tmp




# ── entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":

    df = fetch_market_data()


    print(df)







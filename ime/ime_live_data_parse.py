import pandas as pd
import json, warnings

from utils.database import make_connection

warnings.filterwarnings("ignore")


db_conn = make_connection()
test = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[test] ORDER BY time ASC", db_conn)
db_conn.close()
test = test[["record"]].iloc[1:].reset_index(drop=True, inplace=False)
test["record"] = test["record"].str[6:]

test_list = test["record"].values.tolist()
records = []
for r in range(len(test_list)):
    record = json.loads(test_list[r])
    m = record.get("M")
    if m:
        records += m

m_set = set()
for r in range(len(records)):
    m = records[r].get("M")
    m_set.add(m)


records_by_m = {}
for m in m_set:
    tmp = []
    for r in range(len(records)):
        if records[r].get("M") == m:
            tmp.append(records[r])
    records_by_m[m] = tmp

keys_of_m_set = {}
for m in records_by_m.keys():
    tmp = set()
    if m == 'updateFutureDateTime':
        tmp.add(records_by_m[m][-1]["A"][0])
    elif "Info" in m:
        for r in records_by_m[m]:
            for a in r["A"][0]:
                for k in a.keys():
                    tmp.add(k)
    elif "Option" in m:
        for r in records_by_m[m]:
            for k in r["A"][0].keys():
                if k[:1] != "_":
                    tmp.add(k)
    else:
        for r in records_by_m[m]:
            for k in r["A"][0].keys():
                tmp.add(k)
    keys_of_m_set[m] = tmp



"""

"updateGavahiSingle"
"updateSalafSingle"
"updateSandoqSingle"

"updateCDCSingle"
"updateCDCMarketsInfo"

"updateOptionSingle"

"""

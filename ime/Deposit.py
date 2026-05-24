import json
import requests
from time_utils import handle_time


#################################################################


class Deposit:
    def __init__(self, deposit_id: int):
        self.deposit_id = deposit_id
        self.info = None
    def update(self, info: list[dict]):
        self.info = info


##################################################################


# get ids associated with each deposit info
def get_dep_ids() -> list[int]:
    API_URL_Contracts = 'https://dataapi.ime.co.ir/api/CDC/CDCContracts'
    r = requests.get(API_URL_Contracts) 
    output = r.json()
    id_code = {r['ID']:r['Code'] for r in output}
    return id_code.keys()


# get data of each chunk of time range
def get_data(s_date: str, e_date: str, dep_id: int) -> list:
    data = []
    page_ind = 1
    API_URL_Trades = 'https://dataapi.ime.co.ir/api/CDC/CDCTrades' 
    pageSize_max = 100
    while True:
        payload = {"fromDate":s_date,
                   "toDate":e_date,
                   "pageNumber":page_ind,
                   "pageSize":pageSize_max,
                   "customFilter":str(dep_id),
                   "sortOrder":"asc"}
        r = requests.post(API_URL_Trades, json=payload) 
        rr = json.loads(r.text)
        data = data + rr['Data'] 
        page_num = rr['TotalPages']
        if (page_ind == page_num) or (page_num==0):
            break
        page_ind += 1
    return data


# Get data related to each ID
def get_id_data(s_date: str, e_date: str, dep_id: int, time_range: list[dict]) -> list:
    data = []      
    for t in time_range:
        data = data + get_data(t['s_date'], t['e_date'], dep_id)  
    return data
    

# Main Function
def get_deposit_data(s_date: str, e_date: str, dep_ids: list[int]) -> list[Deposit]:
    time_range = handle_time(s_date, e_date)
    deposits = {}
    for id_ in dep_ids:
        c = Deposit(id_)
        c.update(get_id_data(s_date, e_date, id_,time_range))
        deposits[id_] = c
    return deposits

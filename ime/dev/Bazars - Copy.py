import json
import requests
from ime.dev.time_utils import handle_time

####################### Constants ##########################

BASE_STATS_URL = 'https://www.ime.co.ir/'
PAGE_LIMIT = 100

################### Network Requests #######################

def GET_req(api_url: str, s_date: str, e_date: str, group_id: int = None, cat_id: int = None, 
            subCat_id: int = None, producer_id: int = None, offset: int = None, limit: int = None)-> requests.models.Response:
    
    params = {'f': s_date,
              't': e_date,
              'm': group_id, 
              'c': cat_id, 
              's': subCat_id, 
              'p': producer_id,
              'ot': subCat_id,
              'lang': 8,
              'order': 'asc',
              'offset': offset,
              "limit": limit}
    try:
        r = requests.get(BASE_STATS_URL+api_url, params=params) 
        r.raise_for_status() 
        return r
    except requests.exceptions.RequestException as e:
        print(f"Network error occurred: {e}")
        raise   

        
def POST_req(api_url: str, s_date: str, e_date: str, fari: bool|None=None, group_id=0, cat_id=0, subCat_id=0, producer_id=0, premium: bool|None=None)\
            -> requests.models.Response:
    
    if premium:
        api_url = f"{api_url}?f={s_date}&t={e_date}"
        payload = None
    else:
        payload = {
            "Language": 8,
            "fari": fari,
            "GregorianFromDate": s_date,
            "GregorianToDate": e_date,
            "MainCat": group_id,
            "Cat": cat_id,
            "SubCat": subCat_id,
            "Producer": producer_id}
    try:
        r = requests.post(BASE_STATS_URL+api_url, json=payload) 
        r.raise_for_status() 
        return r
    except requests.exceptions.RequestException as e:
        print(f"Network error occurred: {e}")
        raise   

############################# Classes ##############################

class Contract:
    
    def __init__(self, contract_id: str|tuple[str]) -> None:
        
        self.contract_id = contract_id
        self.info = {}
        
    def update(self, info: dict) -> None:
        
        self.info = info

        
    
class Market:
    
    def __init__(self, market_id: int, name: str, url: str, req_type: str, id_name: str, fari: bool|None=None, premium: bool|None=None, 
                 cat_id: int|None=None, subCat_id: int|None=None, offset: int|None=None, limit: int|None=None) -> None:
        
        self.market_id = market_id
        self.name = name
        self.api_url = url
        self.req_type = req_type
        self.id_name = id_name
        self.fari = fari
        self.premium = premium
        self.cat_id = cat_id
        self.subCat_id = subCat_id
        self.offset = offset
        self.limit = limit
        self.contracts = {}
        
    def send_req(self, s_date: str, e_date: str) -> requests.models.Response:
        
        if self.req_type == 'post':
            resp = POST_req(self.api_url, s_date, e_date, getattr(self, "fari", None), premium=getattr(self, "premium", None))
        elif self.req_type == 'get':
            resp = GET_req(self.api_url, s_date, e_date, cat_id = getattr(self, "cat_id", None), 
                           subCat_id = getattr(self, "subCat_id", None), offset = getattr(self, "offset", None))
        return resp
        
    def update_contract(self, ud: dict) -> None:

        id_parts = self.id_name.split('/')
        try:
            if len(id_parts) == 2:
                contract_id = (ud.get(id_parts[0]), ud.get(id_parts[1]))
            else:
                contract_id = ud.get(id_parts[0])
        except KeyError as e:
            print('Warning: No contract_id')
            return
        
        if contract_id not in self.contracts:
            self.contracts[contract_id] = Contract(contract_id)
        self.contracts[contract_id].update(ud) 

############################ Functions ##############################

def init_markets() -> list[Market]:
    
    markets = {}
    markets[11] = Market(11, 'فیزیکی', 'subsystems/ime/services/home/imedata.asmx/GetAmareMoamelatList', 'post', 'Symbol' , fari = False)
    markets[5] = Market(5, 'فرعی', 'subsystems/ime/services/home/imedata.asmx/GetAmareMoamelatList', 'post', 'Symbol', fari = True)
    markets[12] = Market(12, 'پریمیوم', 'SubSystems/IME/Fiziki/AmarePermium.ashx', 'post', 'Symbol', premium = True)
    markets[13] = Market(13, 'صادراتی', 'subsystems/ime/fiziki/export.ashx', 'get', 'Symbol')
    markets[2] = Market(2, 'مناقصه', 'SubSystems/IME/Fiziki/AmareMonaghesat.ashx', 'post', 'Symbol')
    markets[31] = Market(31, 'آتی', 'subsystems/ime/futurereports/FutureAmareMoamelatHnadler.ashx',
                         'get', 'ContractCode/ContractDay', cat_id = 0) # cat_id=-1: active contracts
    markets[32] = Market(32, 'اختیار معامله', 'subsystems/ime/option/optionboarddata.ashx', 'get',
                         'ContractCode', cat_id = 0, subCat_id = 0, offset = 0, limit = PAGE_LIMIT) # cat_id=-1: active contracts
    markets[4] = Market(4, 'مالی', 'subsystems/ime/bazaremali/bazaremalidata.ashx', 'get', 'Namad')
    return markets


    # Extract the list of updates from the data
def normalize_data(data: dict):
    
    if isinstance(data, dict):
        rows = data.get('rows')
        d = data.get('d')
        if rows:
            if type(rows) != list:
                data = json.loads(rows)
            else:
                data = rows
        elif d:
            data = json.loads(d)
        else:
            print('Unknown Response:')
    if type(data) != list:
        data = [data]
    
    return data



def get_market_data(market_id: int, s_date: str, e_date: str) -> list[dict]:

    s, e = handle_time(s_date, e_date)
    # if there is a problem with input dates, return 
    if not(s and e):
        return []
    b = markets.get(market_id)  
    update_list = []
    try:
        while True:           
            resp = b.send_req(s_date, e_date)
            resp.raise_for_status()
            data = resp.json()
            list1 = normalize_data(data)
            update_list.extend(list1)
            resp_num = len(list1)
            if market_id == 32: # In Market 32, we have to set a limit, so we create a loop to get all the data
                if (resp_num < PAGE_LIMIT): 
                    break
                b.offset = b.offset + PAGE_LIMIT
            else:
                break
        for ud in update_list:
            markets[market_id].update_contract(ud)
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        print(f"Error: Failed to process response for {market_id}: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        
    return markets
    

##################### TEST ###########################


markets = init_markets()
market_id = 11
s_date = '1400/06/01'
e_date = '1400/06/31'
output = get_market_data(market_id, s_date, e_date)

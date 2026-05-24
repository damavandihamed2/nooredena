import json
import requests

##########################################

def session_req(
        session: requests.Session,
        base_url: str,
        req_name: str,
        *,
        transport: str|None = None,
        token: str|None = None,
        connection_data: str|None = None,
        verify: bool|None = None,
        stream: bool|None = None,
        tid: int|None = None
) -> tuple[requests.models.Response, str|None, str|None]:

    connect_url = f"{base_url}/{req_name}"
    connect_params = {
        'transport': transport,
        'clientProtocol': '2.1',
        'connectionToken': token,
        'connectionData': connection_data,
        'tid': tid} 
    resp = session.get(connect_url, params=connect_params, stream=stream, verify=verify)
    if req_name=='negotiate':
        neg_data = resp.json()
        connection_data = json.dumps([{"name": "marketshub"}])
        token = neg_data['ConnectionToken']
    return resp, token, connection_data

###################### TABLOS ###########################

class Contract:
    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id
        self.info = {}
        self.history = []
    def update(self, ud: dict):
        self.info.update(ud)
    def set_history(self, history: list):
        self.history = history
        
class Market:
    def __init__(self, name: str) -> None:
        self.name = name
        self.contracts = {}
        self.info = {}
    def update_contract(self, contract_id: int, ud: dict):
        if contract_id not in self.contracts:
            self.contracts[contract_id] = Contract(contract_id)
        self.contracts[contract_id].update(ud)  
    def update_market(self, info: dict):
        self.info.update(info)

#########################################

def connect() -> requests.models.Response:
    session = requests.Session()
    base_url = "https://cdn.ime.co.ir//realTimeServer"
    transport = 'serverSentEvents'    
    # 1. Negotiate:
    neg_resp, token, connection_data = session_req(session, base_url, 'negotiate') 
    print(f'Negotiate Phase: {'Successful' if neg_resp.status_code==200 else 'Not Successful'}')
    # 2. Connect:
    con_resp, _, _ = session_req(session, base_url, 'connect', transport=transport, token=token,                                
                                 connection_data=connection_data, verify=False, stream=True, tid=3) 
    print(f'Connection Phase: {'Successful' if con_resp.status_code==200 else 'Not Successful'}')
    # 3. Start:
    strt_resp, _, _ = session_req(session, base_url, 'start', transport=transport, token=token, connection_data=connection_data, verify=False) 
    print(f'Start Phase: {'Successful' if strt_resp.status_code==200 else 'Not Successful'}')
    return con_resp

def update_tablo(market_name: Market, update_list: list[dict]|dict, markets: list[Market]) -> list[Market]:
    # if it's the first time, construct the market instance
    if market_name not in markets:
        markets[market_name] = Market(market_name)
    this_market = markets[market_name]
    # if it only wants to update one entry, make it a list (iterable)
    if isinstance(update_list, dict):
        update_list = [update_list] 
    for ud in update_list:
        contract_id = ud.get('ID') or ud.get('ContractID') or ud.get('id') or\
                      ud.get('CallContractCode') or ud.get('PutContractCode')
        this_market.update_contract(contract_id, ud) # update the tablo with specified ID
    return markets
    
def get_current_data(markets: list[Market], data: str) -> tuple[list[Market], str]:
    future_DateTime = []
    method_list = []
    data1 = json.loads(data[6:]) # Separate the dict part of data
    data1 = data1.get('M',[])  # Access the list of methods in this line
    for d in data1:  
        method_name = d.get('M')
        method_list.append(method_name)
        update_list = d['A'][0]
        if method_name.endswith('Time'):
            future_DateTime = d['A'][0]
            continue
        elif method_name.endswith('Info'):
            market_name = method_name[6:-11]
            if market_name == '':
                market_name = 'Option'
        elif method_name.endswith('Single'):
            market_name = method_name[6:-6]
        elif method_name.endswith('Data'):
            market_name = 'Future'       
        markets = update_tablo(market_name, update_list, markets)
    return markets, future_DateTime, method_list

def get_data(lines: list[str]) -> list[Market]:
    methods_set = set()
    markets = {} # Save market instances
    future_Date_list = []
    for data in lines[2:]:
        updated_markets, future_DateTime, method_list = get_current_data(markets, data)
        future_Date_list.append(future_DateTime)
        methods_set.update(method_list)
    return updated_markets

def read_lines(resp: requests.models.Response, lines_limit: int = 100) -> list[str]:
    i = 1
    lines = []
    for line in resp.iter_lines():
        print(f"Iteration: {i}")
        if line:
            lines.append(line.decode("utf-8", errors="ignore"))
        i += 1
        if i > lines_limit:
            break
    return lines

def init_fiziki_markets(markets: list[Market]) -> tuple[list[Market], list[int]]:
    # Get Available Market IDs
    info_url = 'https://dataapi.ime.co.ir/api/spotmarketdata/GetMarketsInfo'
    r = requests.get(info_url)
    market_infos = r.json()
    market_ids = []    
    for info in market_infos:
        market_id = info.get('MarketId')
        market_ids.append(market_id)
        markets[market_id] = Market(info.get('MarketId'))
        markets[market_id].update_market(info)
    return markets, market_ids

# [
#     {'MarketId': 6, 'MarketName': 'صادراتی کیش', 'StartTime': '11:30:00', 'FinishTime': '11:39:00', 'Duration': 0, 'Counter': 0, 'Color': '', 'StepDescription': '', 'Activate': None, 'Count': None, 'Status': 3},
#     {'MarketId': 20, 'MarketName': 'سیمان', 'StartTime': '12:30:00', 'FinishTime': '15:54:00', 'Duration': 0, 'Counter': 0, 'Color': '', 'StepDescription': '', 'Activate': None, 'Count': None, 'Status': 3},
#     {'MarketId': 23, 'MarketName': 'حراج همزمان', 'StartTime': '11:30:00', 'FinishTime': '13:34:00', 'Duration': 0, 'Counter': 0, 'Color': '', 'StepDescription': '', 'Activate': None, 'Count': None, 'Status': 3}
# ]


def get_fiziki_data(markets: list[Market], market_ids: list[int]) -> list[Market]:
    for market_id in market_ids:
        print(market_id)
        # Get Trades infos of eash market
        market_URL = f"https://dataapi.ime.co.ir/api/spotmarketdata/GetTradesWithId/{market_id}"
        r = requests.get(market_URL)
        print(r.status_code)
        update_list = r.json()
        for ud in update_list:
            contract_id = ud['InstrumentSymbol']
            markets[market_id].update_contract(contract_id, ud)
            offers_URL = 'https://dataapi.ime.co.ir/api/spot/GetHistoryOfOffersById'
            params = {'id': ud['InstrumentId']}
            r = requests.get(offers_URL, params=params) 
            if r.status_code == 200:
                hist = r.json()
                markets[market_id].contracts[contract_id].set_history(hist)
    return markets
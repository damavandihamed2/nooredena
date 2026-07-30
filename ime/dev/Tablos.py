import json
import requests

####################### Constants ##########################

BASE_BOARD_URL = "https://cdn.ime.co.ir//realTimeServer"
MARKETS_INFO_URL = "https://dataapi.ime.co.ir/api/spotmarketdata/GetMarketsInfo"
BASE_FIZIKI_URL = "https://dataapi.ime.co.ir/api/spotmarketdata/GetTradesWithId"
FIZIKI_OFFERS_URL = "https://dataapi.ime.co.ir/api/spot/GetHistoryOfOffersById"

################### Network Requests #######################

# Handle session requests 
def session_req(session: requests.Session, req_name: str,* , transport: str|None = None, 
                token: str|None = None, connection_data: str|None = None, verify: bool|None = None, 
                stream: bool|None = None, tid: int|None = None) ->\
                tuple[requests.models.Response, str|None, str|None]:
    connect_url = f"{BASE_BOARD_URL}/{req_name}"
    connect_params = {
        'transport': transport,
        'clientProtocol': '2.1',
        'connectionToken': token,
        'connectionData': connection_data,
        'tid': tid}
    try:
        resp = session.get(connect_url, params=connect_params, stream=stream, verify=verify)
        if req_name == 'negotiate':
            neg_data = resp.json()
            connection_data = json.dumps([{"name": "marketshub"}])
            token = neg_data.get('ConnectionToken')
        return resp, token, connection_data
    except (json.JSONDecodeError, KeyError, requests.exceptions.RequestException):
        raise  

######################## BOARDS ############################

class Contract:
    
    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id
        self.info = {}
        self.history = []
    
    def update(self, ud: dict) -> None:
        self.info.update(ud)
    
    def set_history(self, history: list) -> None:
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


def connect_to_server() -> requests.models.Response:
    
    session = requests.Session()
    transport = 'serverSentEvents'    
    # 1. Negotiate:
    try:
        neg_resp, token, connection_data = session_req(session, 'negotiate') 
        print('Negotiate Phase: Successful') 
    except Exception as e:
        print(f"Error in Negotatiation Phase: {e}")
        return None
    # 2. Connect:
    try:
        con_resp, _, _ = session_req(session, 'connect', transport=transport, token=token,                                
                                 connection_data=connection_data, verify=False, stream=True, tid=3) 
        print('Connection Phase: Successful')
    except Exception as e:
        print(f"Error in Connection Phase: {e}")
        return None
    # 3. Start:
    try:
        strt_resp, _, _ = session_req(session, 'start', transport=transport, token=token, connection_data=connection_data, verify=False) 
        print('Start Phase: Successful')
    except Exception as e:
        print(f"Error in Start Phase: {e}")
        return None    
    
    return con_resp


    # update the contracts of the market specified by market_name
def update_board(market_name: str, update_list: list[dict], markets: list[Market]) -> None:
    
    # if it's the first time, construct the market instance
    if market_name not in markets:
        markets[market_name] = Market(market_name)
    this_market = markets[market_name]
    for ud in update_list:
        contract_id = ud.get('ID') or ud.get('CommodityID') or ud.get('id') or\
                      ud.get('CallContractCode') or ud.get('PutContractCode')
        this_market.update_contract(contract_id, ud) # update the board with specified ID


    # decode data to extract market name and the list of updates for that market
def decode_data(data: dict) -> tuple[list[Market], list[dict]]:

    try:
        method_name = data.get('M')
        update_list = data.get('A')[0]
    except KeyError as e:
        print(f"Error in Accessing method_name or update_list: {e}")
        return [], []  
    # if there is only one update, convert to list, make it iterable
    if isinstance(update_list, dict):
        update_list = [update_list]
    if method_name.endswith('Time'):
        market_name = 'Time'
    elif method_name.endswith('Info'):
        market_name = method_name[6:-11]
        if market_name == '':
            market_name = 'Option'
    elif method_name.endswith('Single'):
        market_name = method_name[6:-6]
    elif method_name.endswith('Data'):
        market_name = 'Future'       
    else:
        print(f'Error: Unknown method name: {method_name}')
        return [], []
    
    return market_name, update_list


    # Read lines of the data coming from the server
def read_lines(resp: requests.models.Response, lines_limit: int = 100) -> list[str]:
    
    i = 1
    lines = []
    for line in resp.iter_lines():
        if line:
            lines.append(line.decode("utf-8", errors="ignore"))
        i += 1
        if i > lines_limit:
            break
    
    return lines

    
    # Iterate over the saved lines, decode each and update the board accordingly
def get_data(markets: list[Market], resp: requests.models.Response) -> list[str]:

    lines_limit = 200
    lines = read_lines(resp, lines_limit) 
    future_Date_list = []
    for line in lines[2:]:
        try:
            data1 = json.loads(line[6:]) # Separate the dict part of the data
            data1 = data1.get('M',[])  # Access the list of methods in this line
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error in Reading the line: {e}")
            return [], []
        for d in data1: 
            market_name, update_list = decode_data(d) 
            if market_name == 'Time':
                future_Date_list.append(update_list) # Save the future update time
            else:
                update_board(market_name, update_list, markets) # Update the market instance info
    
    return future_Date_list

####################### Fiziki Market Board ########################
#
#     # Initialize available fiziki markets, fill in their info section
# def init_fiziki_markets() -> list[Market]:
#
#     markets = {}
#     # Get Available Market IDs
#     try:
#         r = requests.get(MARKETS_INFO_URL)
#         market_infos = r.json()
#     except (json.JSONDecodeError, KeyError, requests.exceptions.RequestException) as e:
#         print(f"Error in getting available market ids: {e}")
#         return []
#     for info in market_infos:
#         try:
#             market_id = info.get('MarketId')
#             markets[market_id] = Market(info.get('MarketId'))
#             markets[market_id].update_market(info)
#         except (json.JSONDecodeError, KeyError, requests.exceptions.RequestException) as e:
#             print(f"Error in getting info of the market {market_id}: {e}")
#             return
#
#     return markets
#
#     # Retrieve the data related to the fiziki markets
# def get_fiziki_data(markets: list[Market]) -> list[Market]:
#
#     for market_id in markets:
#         print(f"Processing market: {market_id}")
#         # Get Trades infos of each market
#         market_URL = f"{BASE_FIZIKI_URL}/{market_id}"
#         try:
#             r = requests.get(market_URL)
#             r.raise_for_status()
#             update_list = r.json()
#         except Exception as e:
#             print(f"Error in Getting trades info of market {market_id} from the server: {e}")
#             continue
#         for ud in update_list:
#             try:
#                 contract_id = ud.get('InstrumentSymbol') # Set the contract symbol as its id
#                 if contract_id:
#                     markets.get(market_id).update_contract(contract_id, ud)
#                     params = {'id': ud.get('InstrumentId')}
#                     r_offers = requests.get(FIZIKI_OFFERS_URL, params=params)
#                     r_offers.raise_for_status()
#                     hist = r_offers.json()
#                     markets.get(market_id).contracts.get(contract_id).set_history(hist)
#             except requests.exceptions.HTTPError as e:
#                 continue
#             except Exception as e:
#                 print(f"Error in udating market {market_id}, contract {contract_id}: {e}")
#                 continue
#
#     return markets

#######################################################################
# Gavahi = Zaferan, ID: ID
# CDC = other Gavahis, ID: CommodityID, [ContractID: CommodityID =  41:2, 42:4, 43:21, 44:14, 45: 26, 46:30, 47:29, 48:28, 49:27]
# Markets = Option = Ekhtiar moamele, ID: CallContractID, PutContractID
# All = Future, ID: id
# Sandogh   ID: ID

############################### TEST ##################################

def test_mali_boards() -> list[Market]:
    markets = {}
    resp = connect_to_server() # Connect to the server and save response
    future_Date_list = get_data(markets, resp) # update markets based on 
    return markets
# def test_fiziki_boards():
#     fiziki_markets = init_fiziki_markets()
#     fiziki_markets = get_fiziki_data(fiziki_markets)
#     return fiziki_markets


if __name__ == '__main__':
    markets = test_mali_boards()
    print(f"Contracts of markets['Gavahi'] = {markets['Gavahi'].contracts}\n")
    print(f"markets['CDC'].contracts[4].info = {markets['CDC'].contracts[4].info}")
    # fiziki_markets = test_fiziki_boards()
    # print(f"fiziki_markets[23].contracts['MPC-PPC30SJ-00'].info = {fiziki_markets[23].contracts['MPC-PPC30SJ-00'].info}")



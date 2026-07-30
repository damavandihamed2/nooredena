# گواهی سپرده سرمایه گذاری

import json
import requests
from time_utils import get_time_range

######################### Constants #############################

API_URL_TRADES = 'https://dataapi.ime.co.ir/api/CDC/CDCTrades' 
API_URL_CONTRACTS = 'https://dataapi.ime.co.ir/api/CDC/CDCContracts'
MAX_PAGE_SIZE = 100

#################################################################

# get data of each chunk of time range for current deposit
def get_data(s_date: str, e_date: str, deposit_id: int) -> list:
    
    whole_data = []
    page_ind = 1
    while True:
        payload = {"fromDate": s_date,
                   "toDate": e_date,
                   "pageNumber": page_ind,
                   "pageSize": MAX_PAGE_SIZE,
                   "customFilter": str(deposit_id),
                   "sortOrder": "asc"}
        try:
            response = requests.post(API_URL_TRADES, json=payload, timeout=10) 
            response.raise_for_status()
            trades_data = response.json()
            data = trades_data.get('Data', [])
            whole_data.extend(data)
            page_num = trades_data.get('TotalPages',0)
            if (page_ind >= page_num) or (page_num == 0):
                break
            page_ind += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching trades for deposit {deposit_id} ({s_date} to {e_date}, page {page_ind}): {e}")
            break 
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON response for deposit {deposit_id} ({s_date} to {e_date}).")
            break
        except KeyError as e:
            print(f"Error: Missing key {e} in trades data for deposit {deposit_id} ({s_date} to {e_date}).")
            break
    
    return whole_data

#################################################################

class Contract:
    
    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id
        self.info = {}
    
    def update(self, ud: dict) -> None:
        self.info.update(ud)
        

# Deposit class denotes the whole deposit data, each including various contracts
class Deposit:
    
    def __init__(self, deposit_id: int) -> None:
        self.deposit_id = deposit_id
        self.contracts = {}
        # self.trade_data: list[dict] = None
    
    def update_contract(self, contract_id: int, ud: dict) -> None:
        if contract_id not in self.contracts:
            self.contracts[contract_id] = Contract(contract_id)
        self.contracts[contract_id].update(ud)  
    
    def get_info(self, time_range: list[dict]) -> list[dict]:
        data = []      
        for t in time_range:
            s_date = t.get('s_date')
            e_date = t.get('e_date')
            data.extend(get_data(s_date, e_date, self.deposit_id))  
        return data

##################################################################

# Get ids associated with each deposit info
def get_deposit_ids() -> dict:
    
    try:
        response = requests.get(API_URL_CONTRACTS, timeout=10) 
        response.raise_for_status()
        dep_data = response.json()
        id_code_map = {r['ID']:r['Code'] for r in dep_data}
        return id_code_map
    except requests.exceptions.RequestException as e:
        print(f"Error fetching deposit contract IDs: {e}")
        return {}
    except json.JSONDecodeError:
        print("Error: Could not decode JSON response from API_URL_CONTRACTS.")
        return {}
    except KeyError as e:
        print(f"Error: Missing key {e} in contract data.")
        return {}


# Main Function
def get_deposit_data(s_date: str, e_date: str, deposit_ids: list[int]) -> list[Deposit]:
  
    deposits = {}
    time_range = get_time_range(s_date, e_date)
    if time_range:
        for id_ in deposit_ids:
            d = Deposit(id_)
            info_list = d.get_info(time_range)
            for info in info_list:
                contract_id = info.get('ContractCode')
                d.update_contract(contract_id, info)
                deposits[id_] = d
    
    return deposits


######################### TEST ###############################
# ids = {2: 'GoldBar', 4: 'GoldCoin', 21: 'SilverBar', 14: 'CopperCthd', 26: 'Bitumen', 30: 'ZincIngot', 29: 'SteelRebar', 28: 'IronOrePlt', 27: 'KMCT9'}

s_date = "2025/5-6"
e_date = "2026-03/03"
id_code_map = get_deposit_ids()
ids = list(id_code_map.keys())
output = get_deposit_data(s_date, e_date, ids)
print(f"output[4].contracts['CD1GOC0001'].info: {output[4].contracts['CD1GOC0001'].info}")
import json
import requests


################################################################


def GET_req(base_url: str, API_URL: str, s_date: str, e_date: str, group_id=None, cat_id=None, subCat_id=None, producer_id=None)\
            -> requests.models.Response:
    params = {'f': s_date,
              't': e_date,
              'm':group_id, 
              'c':cat_id, 
              's':subCat_id, 
              'p':producer_id,
              'ot':subCat_id,
              'lang':8,
              'order':'asc'}
    r = requests.get(base_url+API_URL, params=params) 
    return r

def POST_req(
        base_url: str,
        api_url: str,
        s_date: str,
        e_date: str,
        fari: bool | None = None,
        group_id = 0,
        cat_id = 0,
        subCat_id = 0,
        producer_id = 0,
        premium: bool | None = None
) -> requests.models.Response:
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
    r = requests.post(base_url+api_url, json=payload)
    return r


###########################################################


class Contract:
    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id
        self.cat_id = None
        self.subCat_id = None
        self.info = {}
    def update(self, info: dict) -> None:
        self.info = info


class Market:
    def __init__(self, market_id: int, name: str, url: str, req_type: str, fari: bool|None=None,
                 premium: bool|None=None, cat_id: int|None=None, subCat_id: int|None=None) -> None:
        self.market_id = market_id
        self.name = name
        self.api_url = url
        self.req_type = req_type
        self.fari = fari
        self.premium = premium
        self.contracts = {}
    def send_req(self, base_url: str, s_date: str, e_date: str) -> requests.models.Response:
        if self.req_type == 'post':
            resp = POST_req(base_url, self.api_url, s_date, e_date, getattr(self, "fari", None), premium=getattr(self, "premium", None))
        elif self.req_type == 'get':
            resp = GET_req(base_url, self.api_url, s_date, e_date, cat_id=getattr(self, "cat_id", None), subCat_id=getattr(self, "subCat_id", None))
        return resp

    def update_contract(self,
                        contract_id: str,
                        ud: dict):
        if contract_id not in self.contracts:
            self.contracts[contract_id] = Contract(contract_id)
        self.contracts[contract_id].update(ud)




def init_markets(

) -> list[Market]:
    markets = {}
    markets[11] = Market(11, 'فیزیکی', 'subsystems/ime/services/home/imedata.asmx/GetAmareMoamelatList', 'post', fari = False)
    markets[5] = Market(5, 'فرعی', 'subsystems/ime/services/home/imedata.asmx/GetAmareMoamelatList', 'post', fari = True)
    markets[12] = Market(12, 'پریمیوم', 'SubSystems/IME/Fiziki/AmarePermium.ashx', 'post', premium = True)
    markets[13] = Market(13, 'صادراتی', 'subsystems/ime/fiziki/export.ashx', 'get')
    markets[2] = Market(2, 'مناقصه', 'SubSystems/IME/Fiziki/AmareMonaghesat.ashx', 'post')
    markets[31] = Market(31, 'آتی', 'subsystems/ime/futurereports/FutureAmareMoamelatHnadler.ashx', 'get', cat_id = 0) # cat_id=-1: active contracts
    markets[32] = Market(32, 'اختیار معامله', 'subsystems/ime/option/optionboarddata.ashx', 'get', cat_id = 0, subCat_id = 0) # cat_id=-1: active contracts
    markets[4] = Market(4, 'مالی', 'subsystems/ime/bazaremali/bazaremalidata.ashx', 'get')
    return markets



def get_market_data(markets: list[Market], market_id: int, s_date: str, e_date: str) -> list[dict]:
    base_url = 'https://www.ime.co.ir/'
    b = markets.get(market_id)
    resp = b.send_req(base_url, s_date, e_date)
    if resp.status_code != 200:
        print(resp.text)
    else:
        update_list = resp.json()
        if type(update_list)!=list:
            if 'rows' in update_list.keys():
                update_list = update_list['rows']
                if type(update_list) != list:
                    update_list = json.loads(update_list)
            elif 'd' in update_list.keys():
                update_list = json.loads(update_list['d'])
            else:
                print('Unknown Response:')
                print(resp.json())   

        for ud in update_list:
            contract_id = ud.get('Symbol') or ud.get('Namad')
            markets[market_id].update_contract(contract_id, ud)
    return markets






